using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Http;

/// <summary>
/// Decodes an HTTP <c>Content-Encoding: deflate</c> body, tolerating both encodings that name is used
/// for in the wild. RFC 9110 defines it as the <b>zlib</b> format (RFC 1950), which is what ClickHouse
/// emits (verified: its bodies start <c>78 5E</c>) and what a bare <see cref="DeflateStream"/> — which
/// expects raw DEFLATE (RFC 1951) — rejects with <see cref="InvalidDataException"/>. Some servers and
/// proxies do send raw DEFLATE, so, like .NET's own <c>DecompressionHandler</c>, this sniffs the first
/// two bytes and picks the matching decoder. Sniffing is deferred to the first read to keep
/// construction non-blocking; the sniffed bytes are replayed into the chosen decoder, so none are lost.
/// </summary>
internal sealed class ZLibOrDeflateStream : Stream
{
    private readonly Stream source;
    private readonly bool leaveOpen;
    private Stream decoder;
    private bool disposed;

    public ZLibOrDeflateStream(Stream source, bool leaveOpen)
    {
        this.source = source ?? throw new ArgumentNullException(nameof(source));
        this.leaveOpen = leaveOpen;
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

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        EnsureDecoder();
        return decoder.Read(buffer, offset, count);
    }

    // Overridden, not inherited: Stream's base implementation of the span overload rents an array from
    // ArrayPool, copies through the byte[] overload and returns it — a rent plus a copy on every
    // synchronous read. PooledReadBufferStream, which sits directly above this stream, reads into the
    // caller's span for any read at least as large as its buffer.
    public override int Read(Span<byte> buffer)
    {
        EnsureDecoder();
        return decoder.Read(buffer);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await EnsureDecoderAsync(cancellationToken).ConfigureAwait(false);
        return await decoder.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    /// <summary>
    /// A zlib stream starts with CMF+FLG where the low nibble of CMF is 8 (the DEFLATE method) and the
    /// big-endian 16-bit value is a multiple of 31. Raw DEFLATE can satisfy both by coincidence — a
    /// non-final stored block also begins with low nibble 8, and one in 31 of those passes the checksum —
    /// so this is a heuristic, not a proof. It is the same one .NET's own <c>DecompressionHandler</c>
    /// uses, and it is only reached for a codec ClickHouse sends as zlib anyway.
    /// </summary>
    private static bool LooksLikeZLibHeader(byte cmf, byte flg)
        => (cmf & 0x0F) == 0x08 && (((cmf << 8) | flg) % 31) == 0;

    /// <summary>
    /// Guards against reading after disposal, which would otherwise sniff and build a second decoder over
    /// a source that may already be closed.
    /// </summary>
    private void ThrowIfDisposed()
    {
        // ObjectDisposedException.ThrowIf is unavailable on net6.0 (a supported target), so throw manually.
#pragma warning disable CA1513
        if (disposed)
            throw new ObjectDisposedException(nameof(ZLibOrDeflateStream));
#pragma warning restore CA1513
    }

    private void EnsureDecoder()
    {
        ThrowIfDisposed();
        if (decoder != null)
            return;

        var header = new byte[2];
        var read = ReadHeader(header);
        decoder = BuildDecoder(header, read);
    }

    private async ValueTask EnsureDecoderAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (decoder != null)
            return;

        var header = new byte[2];
        var read = await ReadHeaderAsync(header, cancellationToken).ConfigureAwait(false);
        decoder = BuildDecoder(header, read);
    }

    private int ReadHeader(byte[] header)
    {
        var read = 0;
        while (read < header.Length)
        {
            var n = source.Read(header, read, header.Length - read);
            if (n == 0)
                break;
            read += n;
        }

        return read;
    }

    private async ValueTask<int> ReadHeaderAsync(byte[] header, CancellationToken cancellationToken)
    {
        var read = 0;
        while (read < header.Length)
        {
            var n = await source.ReadAsync(header.AsMemory(read, header.Length - read), cancellationToken).ConfigureAwait(false);
            if (n == 0)
                break;
            read += n;
        }

        return read;
    }

    private Stream BuildDecoder(byte[] header, int headerLength)
    {
        // Replay whatever we consumed for the sniff ahead of the rest of the body. leaveOpen is honoured
        // by the concatenating stream, which owns neither the prefix nor the source.
        var replayed = new PrefixedStream(header, headerLength, source);

        var isZLib = headerLength == 2 && LooksLikeZLibHeader(header[0], header[1]);
        return isZLib
            ? new ZLibStream(replayed, CompressionMode.Decompress, leaveOpen: true)
            : new DeflateStream(replayed, CompressionMode.Decompress, leaveOpen: true);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !disposed)
        {
            disposed = true;

            // The decoder wraps the replaying stream with leaveOpen, so disposing it releases only the
            // codec's own state; the transport stream is disposed here iff we were told to own it.
            decoder?.Dispose();
            if (!leaveOpen)
                source.Dispose();
        }

        base.Dispose(disposing);
    }

    /// <summary>
    /// Serves a small in-memory prefix, then delegates to the underlying stream. Used to put the sniffed
    /// header bytes back in front of the body. Internal rather than private so its overloads can be
    /// exercised directly — the BCL decoder above it only reaches some of them.
    /// </summary>
    internal sealed class PrefixedStream : Stream
    {
        private readonly byte[] prefix;
        private readonly int prefixLength;
        private readonly Stream inner;
        private int prefixPosition;

        public PrefixedStream(byte[] prefix, int prefixLength, Stream inner)
        {
            this.prefix = prefix;
            this.prefixLength = prefixLength;
            this.inner = inner;
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

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (count == 0)
                return 0;

            var fromPrefix = CopyFromPrefix(buffer.AsSpan(offset, count));
            return fromPrefix > 0 ? fromPrefix : inner.Read(buffer, offset, count);
        }

        // Which overload a BCL decoder reads with is an implementation detail that varies by target
        // framework — on net10.0 both take the byte[] one — so this exists to keep the prefix replay
        // correct and allocation-free either way, not because a measured path depends on it. The base
        // implementation would also be correct, just via an ArrayPool rent and a copy per read.
        public override int Read(Span<byte> buffer)
        {
            if (buffer.Length == 0)
                return 0;

            var fromPrefix = CopyFromPrefix(buffer);
            return fromPrefix > 0 ? fromPrefix : inner.Read(buffer);
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (buffer.Length == 0)
                return 0;

            var fromPrefix = CopyFromPrefix(buffer.Span);
            return fromPrefix > 0
                ? fromPrefix
                : await inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        /// <summary>
        /// Returns the prefix bytes remaining, without touching the inner stream. Deliberately does not
        /// top the buffer up from the inner stream: a short read is legal, and mixing the two would
        /// block on a network read the caller may not want yet.
        /// </summary>
        private int CopyFromPrefix(Span<byte> destination)
        {
            var remaining = prefixLength - prefixPosition;
            if (remaining <= 0)
                return 0;

            var take = Math.Min(remaining, destination.Length);
            prefix.AsSpan(prefixPosition, take).CopyTo(destination);
            prefixPosition += take;
            return take;
        }
    }
}
