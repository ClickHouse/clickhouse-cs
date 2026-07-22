using System;
using System.Buffers;
using System.IO;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// A write-only buffering stream that coalesces many small writes into large blocks before
/// forwarding them to an inner stream.
///
/// Functionally equivalent to <see cref="BufferedStream"/> for the compressor write path
/// (row serialization into a <see cref="System.IO.Compression.GZipStream"/> /
/// <see cref="System.IO.Compression.BrotliStream"/>), but rents its backing buffer from
/// <see cref="ArrayPool{T}"/> instead of allocating a fresh array per instance. A binary insert
/// constructs one compressing stream per batch, so this removes one large-object-heap allocation
/// (256 KiB by default) — and the Gen2 collection it drives — for every batch sent.
/// </summary>
internal sealed class PooledWriteBufferStream : Stream
{
    private readonly Stream inner;
    private readonly bool leaveOpen;
    private byte[] buffer;
    private int position;
    private bool disposed;

    /// <param name="inner">The stream buffered writes are forwarded to.</param>
    /// <param name="bufferSize">Minimum backing-buffer size; the pool may return a larger one.</param>
    /// <param name="leaveOpen">
    /// When <c>false</c> (the default, matching <see cref="BufferedStream"/>) the inner stream is
    /// disposed together with this wrapper — important for a compression stream, whose dispose
    /// flushes the trailing footer. The compressor passes the caller's <c>leaveOpen</c> to the
    /// compression stream itself, which governs whether the final destination is left open.
    /// </param>
    public PooledWriteBufferStream(Stream inner, int bufferSize, bool leaveOpen = false)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "Buffer size must be positive.");
        this.leaveOpen = leaveOpen;
        // The pool may hand back a larger array; the full length is used as capacity.
        buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
    }

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] array, int offset, int count)
    {
        _ = array ?? throw new ArgumentNullException(nameof(array));
        Write(new ReadOnlySpan<byte>(array, offset, count));
    }

    public override void Write(ReadOnlySpan<byte> source)
    {
        ThrowIfDisposed();

        while (source.Length > 0)
        {
            int space = buffer.Length - position;
            if (space == 0)
            {
                FlushBuffer();
                space = buffer.Length;
            }

            // A write at least as large as the buffer with nothing pending is forwarded
            // directly, avoiding a copy through the buffer (matches BufferedStream).
            if (position == 0 && source.Length >= buffer.Length)
            {
                inner.Write(source);
                return;
            }

            int toCopy = Math.Min(source.Length, space);
            source.Slice(0, toCopy).CopyTo(buffer.AsSpan(position));
            position += toCopy;
            source = source.Slice(toCopy);
        }
    }

    public override void WriteByte(byte value)
    {
        ThrowIfDisposed();
        if (position == buffer.Length)
            FlushBuffer();
        buffer[position++] = value;
    }

    public override void Flush()
    {
        ThrowIfDisposed();
        FlushBuffer();
        inner.Flush();
    }

    public override int Read(byte[] array, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        // Dispose is idempotent: BinaryWriter disposes the underlying stream, and the caller's
        // own `using` then disposes it again. Returning a pooled array twice would be a serious
        // corruption bug, so guard against re-entry.
        if (disposed)
            return;
        disposed = true;

        if (disposing)
        {
            try
            {
                FlushBuffer();
            }
            finally
            {
                var toReturn = buffer;
                buffer = null;
                if (toReturn != null)
                    ArrayPool<byte>.Shared.Return(toReturn);
                if (!leaveOpen)
                    inner.Dispose();
            }
        }

        base.Dispose(disposing);
    }

    private void FlushBuffer()
    {
        if (position > 0)
        {
            inner.Write(buffer, 0, position);
            position = 0;
        }
    }

    private void ThrowIfDisposed()
    {
        // ObjectDisposedException.ThrowIf is unavailable on net6.0 (a supported target), so throw manually.
#pragma warning disable CA1513
        if (disposed)
            throw new ObjectDisposedException(nameof(PooledWriteBufferStream));
#pragma warning restore CA1513
    }
}
