using System;
using System.Buffers;
using System.IO;

namespace ClickHouse.Driver.Formats;

/// <summary>
/// A read-only buffering stream — like <see cref="BufferedStream"/> for reads — that rents its backing
/// buffer from <see cref="ArrayPool{T}"/> instead of allocating a fresh array per instance. The reader wraps
/// each HTTP query response in one of these, so this removes the fresh per-query read buffer
/// (<c>ReadBufferSize</c>, 64 KiB by default; larger sizes would otherwise land on the large object heap).
///
/// The buffer is returned to the pool on <see cref="Dispose(bool)"/>, which is idempotent. Callers pass
/// <c>leaveOpen: true</c> when the inner stream is owned elsewhere (e.g. the HTTP response message).
/// </summary>
internal sealed class PooledReadBufferStream : Stream
{
    private readonly Stream inner;
    private readonly bool leaveOpen;
    private byte[] buffer;
    private int position; // Cursor into the filled region of the buffer.
    private int filled;   // Number of valid bytes currently in the buffer.
    private bool disposed;

    /// <param name="inner">The stream buffered reads are pulled from.</param>
    /// <param name="bufferSize">Minimum backing-buffer size; the pool may return a larger one.</param>
    /// <param name="leaveOpen">When <c>false</c> the inner stream is disposed together with this wrapper.</param>
    public PooledReadBufferStream(Stream inner, int bufferSize, bool leaveOpen = false)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "Buffer size must be positive.");
        this.leaveOpen = leaveOpen;
        // The pool may hand back a larger array; the full length is used as capacity.
        buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
    }

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] array, int offset, int count)
    {
        // Validate arguments per the Stream contract (Stream.ValidateBufferArguments is unavailable on net6.0).
        if (array is null)
            throw new ArgumentNullException(nameof(array));
        if (offset < 0)
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Non-negative number required.");
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count), count, "Non-negative number required.");
        if (array.Length - offset < count)
            throw new ArgumentException("The sum of offset and count is larger than the buffer length.");

        ThrowIfDisposed();
        if (count == 0)
            return 0;

        // Serve from the buffer while it still has unread bytes.
        if (position < filled)
            return CopyOut(array, offset, count);

        // Buffer exhausted. A request at least as large as the buffer reads straight into the caller's
        // array, skipping the copy through our buffer (matches BufferedStream).
        if (count >= buffer.Length)
            return inner.Read(array, offset, count);

        if (!Refill())
            return 0;

        return CopyOut(array, offset, count);
    }

    public override int ReadByte()
    {
        ThrowIfDisposed();
        if (position >= filled && !Refill())
            return -1;
        return buffer[position++];
    }

    public override void Flush()
    {
        // Nothing to flush on a read stream.
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] array, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        // Idempotent: the reader disposes this explicitly (the BinaryReader -> PeekableStreamWrapper chain
        // does not propagate Dispose to inner streams), and a `using` may dispose it again. Returning a
        // pooled array twice would corrupt the pool, so guard against re-entry.
        if (disposed)
            return;
        disposed = true;

        if (disposing)
        {
            var toReturn = buffer;
            buffer = null;
            if (toReturn != null)
                ArrayPool<byte>.Shared.Return(toReturn);
            if (!leaveOpen)
                inner.Dispose();
        }

        base.Dispose(disposing);
    }

    // Copies as much of the buffered region as fits into the caller's array, advancing the cursor.
    private int CopyOut(byte[] array, int offset, int count)
    {
        int available = Math.Min(count, filled - position);
        Buffer.BlockCopy(buffer, position, array, offset, available);
        position += available;
        return available;
    }

    // Pulls the next block from the inner stream into the buffer. Returns false at end of stream.
    private bool Refill()
    {
        filled = inner.Read(buffer, 0, buffer.Length);
        position = 0;
        return filled > 0;
    }

    private void ThrowIfDisposed()
    {
        // ObjectDisposedException.ThrowIf is unavailable on net6.0 (a supported target), so throw manually.
#pragma warning disable CA1513
        if (disposed)
            throw new ObjectDisposedException(nameof(PooledReadBufferStream));
#pragma warning restore CA1513
    }
}
