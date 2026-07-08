using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// A duplex test stream: reads are served from a fixed script of "server" bytes, writes are captured into an
/// in-memory sink. Lets a connection be driven through a scripted server exchange (handshake, ping, …) with no
/// real socket, while the bytes the client sent remain inspectable via <see cref="Written"/>. Optionally caps
/// bytes returned per read to exercise buffer refill loops.
/// </summary>
internal sealed class ScriptedDuplexStream : Stream
{
    private readonly byte[] script;
    private readonly int maxChunk;
    private readonly bool blockWhenExhausted;
    private readonly MemoryStream sink = new();
    private int position;

    public ScriptedDuplexStream(byte[] script, int maxChunk = int.MaxValue, bool blockWhenExhausted = false)
    {
        this.script = script;
        this.maxChunk = maxChunk < 1 ? 1 : maxChunk;
        this.blockWhenExhausted = blockWhenExhausted;
    }

    /// <summary>The bytes the connection has written (the client → server side of the exchange).</summary>
    public byte[] Written => sink.ToArray();

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => script.Length;

    public override long Position
    {
        get => position;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
        => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        int available = Math.Min(Math.Min(buffer.Length, maxChunk), script.Length - position);
        if (available <= 0)
        {
            return 0;
        }

        script.AsSpan(position, available).CopyTo(buffer);
        position += available;
        return available;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Once the script is drained, optionally block as a real socket would when the server hasn't sent the
        // next packet yet — so a test can cancel while the read is genuinely pending. Throws when cancelled.
        if (blockWhenExhausted && position >= script.Length)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }

        return Read(buffer.Span);
    }

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        sink.Write(buffer);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Write(buffer.Span);
        return default;
    }

    public override void Flush()
    {
    }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();
}
