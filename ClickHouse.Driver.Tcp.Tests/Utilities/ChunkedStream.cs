using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// A read-only test stream over a byte array that returns at most <c>maxChunk</c> bytes per read, so tests
/// can exercise partial fills and buffer refill loops the way a real socket would. Optionally observes
/// cancellation.
/// </summary>
internal sealed class ChunkedStream : Stream
{
    private readonly byte[] data;
    private readonly int maxChunk;
    private readonly bool honorCancellation;
    private int position;

    public ChunkedStream(byte[] data, int maxChunk = int.MaxValue, bool honorCancellation = false)
    {
        this.data = data;
        this.maxChunk = maxChunk < 1 ? 1 : maxChunk;
        this.honorCancellation = honorCancellation;
    }

    public int ReadCount { get; private set; }

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => data.Length;

    public override long Position
    {
        get => position;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        int n = Math.Min(Math.Min(maxChunk, count), data.Length - position);
        Array.Copy(data, position, buffer, offset, n);
        position += n;
        ReadCount++;
        return n;
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (honorCancellation)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }

        int n = Math.Min(Math.Min(maxChunk, buffer.Length), data.Length - position);
        data.AsSpan(position, n).CopyTo(buffer.Span);
        position += n;
        ReadCount++;
        return ValueTask.FromResult(n);
    }

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
