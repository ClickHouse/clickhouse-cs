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

    /// <summary>Ticks are the unit, so an elapsed span is exactly the span advanced.</summary>
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => Interlocked.Read(ref timestamp);

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

        ClickHouseTcpConnection connection = await CreateReadyAsync(cancellationToken).ConfigureAwait(false);
        lock (gate)
        {
            created.Add(connection);
        }

        return connection;
    }

    /// <summary>Builds a handshaken, Ready connection over a scripted server Hello and no socket.</summary>
    /// <param name="cancellationToken">A token to observe during the handshake.</param>
    /// <param name="trailing">Extra bytes the "server" sent after the Hello, to leave the reader out of step.</param>
    /// <returns>A connection in the Ready state.</returns>
    internal static async ValueTask<ClickHouseTcpConnection> CreateReadyAsync(
        CancellationToken cancellationToken = default,
        byte[] trailing = null)
    {
        byte[] hello = await ServerHelloBytesAsync(cancellationToken).ConfigureAwait(false);
        byte[] script = hello;
        if (trailing is { Length: > 0 })
        {
            script = new byte[hello.Length + trailing.Length];
            hello.CopyTo(script, 0);
            trailing.CopyTo(script, hello.Length);
        }

        var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(script), socket: null);
        await connection.HandshakeAsync(new ClientHandshakeParameters { Username = "default" }, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    private static async Task<byte[]> ServerHelloBytesAsync(CancellationToken cancellationToken)
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
