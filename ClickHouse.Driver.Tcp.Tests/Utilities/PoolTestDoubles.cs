using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// A clock the test drives by hand, so age and idleness can be aged by minutes without waiting for them. Its
/// timers do nothing: the pool's sweep is invoked directly, keeping every test deterministic rather than
/// racing a background timer.
/// </summary>
internal sealed class ControlledTimeProvider : TimeProvider
{
    private long timestamp = TimeSpan.TicksPerHour; // Not zero, so "before the start" is representable.
    private Action onNextTimestamp;

    /// <summary>Ticks are the unit, so an elapsed span is exactly the span advanced.</summary>
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <summary>
    /// Runs once, on the next timestamp read, and is then cleared. The pool reads the clock at points a test
    /// cannot otherwise reach — inside a checkout, after the connection has left the idle set and before it is
    /// recorded as leased — so this is how a test acts at such a moment rather than around it.
    /// </summary>
    public Action OnNextTimestamp
    {
        get => Volatile.Read(ref onNextTimestamp);
        set => Volatile.Write(ref onNextTimestamp, value);
    }

    public override long GetTimestamp()
    {
        // Cleared before it runs, so a hook that makes the pool read the clock again does not re-enter itself.
        if (Volatile.Read(ref onNextTimestamp) is not null)
        {
            Interlocked.Exchange(ref onNextTimestamp, null)?.Invoke();
        }

        return Interlocked.Read(ref timestamp);
    }

    public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch + TimeSpan.FromTicks(GetTimestamp());

    /// <summary>Moves the clock forward.</summary>
    public void Advance(TimeSpan by) => Interlocked.Add(ref timestamp, by.Ticks);

    public override ITimer CreateTimer(TimerCallback callback, object state, TimeSpan dueTime, TimeSpan period)
        => new InertTimer();

    private sealed class InertTimer : ITimer
    {
        public bool Change(TimeSpan dueTime, TimeSpan period) => true;

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => default;
    }
}

/// <summary>
/// Hands the pool connections that need no server: each is driven through a real handshake over a scripted
/// server reply, so it reaches the Ready state the pool checks for and can be terminated like a real one.
/// </summary>
internal sealed class FakeConnectionFactory : IConnectionFactory
{
    private readonly List<ClickHouseTcpConnection> created = [];
    private readonly object gate = new();

    /// <summary>Set to make the next create fail with this exception instead of returning a connection.</summary>
    public Exception FailNextWith { get; set; }

    /// <summary>Awaited before each create completes, so a test can hold a checkout open. Null for none.</summary>
    public Func<CancellationToken, Task> BeforeCreate { get; set; }

    /// <summary>When set, connections are built over a transport whose disposal throws, so closing one fails.</summary>
    public bool ClosingThrows { get; set; }

    /// <summary>
    /// Runs when the pool closes a connection, so a test can act at that moment. Read when the connection is
    /// created, so set it before the connections are opened.
    /// </summary>
    public Action OnClose { get; set; }

    /// <summary>
    /// Extra bytes the "server" sends after its Hello, so the connection reaches Ready with the reader out of
    /// step. What an operation that did not consume its whole reply leaves behind, which no scripted-clean
    /// connection can reproduce.
    /// </summary>
    public byte[] Trailing { get; set; }

    /// <summary>
    /// When set, the create ignores the token it is given. This is the dial whose connect and handshake had
    /// already finished when the cancellation arrived, so it returns a connection the pool then has to deal with
    /// — the race that a dial which simply observes the token never reaches.
    /// </summary>
    public bool IgnoresCancellation { get; set; }

    /// <summary>Whether the pool disposed this factory during its teardown.</summary>
    public bool Disposed { get; private set; }

    /// <summary>Records the call. This double holds nothing to release; the real factory holds TLS certificates.</summary>
    public void Dispose() => Disposed = true;

    /// <summary>How many connections have been opened.</summary>
    public int CreateCount
    {
        get
        {
            lock (gate)
            {
                return created.Count;
            }
        }
    }

    /// <summary>The connections opened so far, in order.</summary>
    public IReadOnlyList<ClickHouseTcpConnection> Created
    {
        get
        {
            lock (gate)
            {
                return [.. created];
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask<ClickHouseTcpConnection> CreateAsync(CancellationToken cancellationToken)
    {
        if (BeforeCreate is not null)
        {
            await BeforeCreate(cancellationToken).ConfigureAwait(false);
        }

        if (FailNextWith is { } failure)
        {
            FailNextWith = null;
            throw failure;
        }

        ClickHouseTcpConnection connection = await CreateReadyAsync(
            IgnoresCancellation ? CancellationToken.None : cancellationToken,
            trailing: Trailing,
            throwOnClose: ClosingThrows,
            onClose: OnClose).ConfigureAwait(false);
        lock (gate)
        {
            created.Add(connection);
        }

        return connection;
    }

    /// <summary>Builds a handshaken, Ready connection over a scripted server Hello and no socket.</summary>
    /// <param name="cancellationToken">A token to observe during the handshake.</param>
    /// <param name="trailing">Extra bytes the "server" sent after the Hello, to leave the reader out of step.</param>
    /// <param name="throwOnClose">When true, the transport throws when disposed, so terminating the connection fails.</param>
    /// <param name="onClose">Runs when the transport is disposed. Null for none.</param>
    /// <returns>A connection in the Ready state.</returns>
    internal static async ValueTask<ClickHouseTcpConnection> CreateReadyAsync(
        CancellationToken cancellationToken = default,
        byte[] trailing = null,
        bool throwOnClose = false,
        Action onClose = null)
    {
        byte[] hello = await ServerHelloBytesAsync(cancellationToken).ConfigureAwait(false);
        byte[] script = hello;
        if (trailing is { Length: > 0 })
        {
            script = new byte[hello.Length + trailing.Length];
            hello.CopyTo(script, 0);
            trailing.CopyTo(script, hello.Length);
        }

        Stream transport = new ScriptedDuplexStream(script);
        if (throwOnClose)
        {
            transport = new OnDisposeStream(transport, static () => throw new IOException("teardown failed"));
        }
        else if (onClose is not null)
        {
            transport = new OnDisposeStream(transport, onClose);
        }

        var connection = new ClickHouseTcpConnection(transport, socket: null);
        await connection.HandshakeAsync(new ClientHandshakeParameters { Username = "default" }, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <summary>The server's Hello reply, for a test that plays the server side over a real socket.</summary>
    /// <param name="cancellationToken">A token to observe while encoding.</param>
    /// <returns>The encoded Hello packet.</returns>
    internal static async Task<byte[]> ServerHelloBytesAsync(CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(buffer))
        {
            writer.WriteVarUInt((ulong)ServerPacketType.Hello);
            writer.WriteString("ClickHouse");
            writer.WriteVarUInt(25);
            writer.WriteVarUInt(8);
            writer.WriteVarUInt(54476);
            writer.WriteString("UTC");
            writer.WriteString("clickhouse-server");
            writer.WriteVarUInt(0);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        return buffer.ToArray();
    }
}

/// <summary>
/// Wraps a transport and runs an action when it is disposed. Used two ways: with an action that throws, to stand
/// in for a socket teardown that fails — which proves the sweep's exception guard is load-bearing, since such a
/// failure would otherwise reach a thread-pool thread with no one to catch it — and with an ordinary action, to
/// let a test act at the moment the pool closes a connection.
/// </summary>
internal sealed class OnDisposeStream(Stream inner, Action onDispose) : Stream
{
    public override bool CanRead => inner.CanRead;

    public override bool CanSeek => false;

    public override bool CanWrite => inner.CanWrite;

    public override long Length => inner.Length;

    public override long Position
    {
        get => inner.Position;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);

    public override int Read(Span<byte> buffer) => inner.Read(buffer);

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        => inner.ReadAsync(buffer, cancellationToken);

    public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);

    public override void Write(ReadOnlySpan<byte> buffer) => inner.Write(buffer);

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        => inner.WriteAsync(buffer, cancellationToken);

    public override void Flush() => inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    // The inner stream is disposed after the action, so an action that throws leaves it alone — which is what the
    // failing-teardown case needs.
    protected override void Dispose(bool disposing)
    {
        onDispose();
        inner.Dispose();
    }
}
