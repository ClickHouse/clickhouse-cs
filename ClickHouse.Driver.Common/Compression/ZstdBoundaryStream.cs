using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Vendor.ZstdSharp;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// Wraps a vendored ZSTD stream so that no vendored exception escapes the driver's public surface.
/// <para>
/// The vendored codec reports every codec-level failure as <see cref="ZstdException"/>, which is
/// <c>internal</c> to <c>ClickHouse.Driver.Common</c> — a caller cannot name it, so it can only be
/// caught as bare <see cref="Exception"/>. The block path already translates it at the boundary
/// (<see cref="ZstdCompressor.Encode"/> / <see cref="ZstdCompressor.Decode"/> rethrow as
/// <see cref="InvalidOperationException"/>); this type gives <see cref="ZstdCompressor.Compress"/>
/// and <see cref="ZstdCompressor.Decompress"/> the same boundary.
/// </para>
/// <para>
/// The translated types are chosen per direction rather than uniformly:
/// <list type="bullet">
/// <item>
/// <b>reading</b> (a response body that is corrupt or not a ZSTD frame) becomes
/// <see cref="InvalidDataException"/> — what every sibling read-path codec already throws for the
/// same condition: gzip, deflate and brotli through the BCL streams, and LZ4 through its vendored
/// frame reader. A caller with a <c>catch (InvalidDataException)</c> around response reading
/// therefore behaves identically whichever codec the server answered with;
/// </item>
/// <item>
/// <b>writing</b> becomes <see cref="InvalidOperationException"/>, matching
/// <see cref="ZstdCompressor.Encode"/> — the mirror operation on the same type. Data the caller
/// hands us to compress is never "invalid" (any byte sequence is compressible), so a failure here
/// is a codec-state fault, not a data fault.
/// </item>
/// </list>
/// </para>
/// <para>
/// Every member forwards to the inner stream, including the asynchronous ones: the read path issues
/// large asynchronous reads, and relying on <see cref="Stream"/>'s default async implementations
/// would silently reroute them through the synchronous path.
/// </para>
/// </summary>
internal sealed class ZstdBoundaryStream : Stream
{
    private readonly Stream inner;
    private readonly bool reading;
    private bool disposed;

    /// <param name="inner">The vendored stream to wrap. Owned by this instance and disposed with it.</param>
    /// <param name="reading">
    /// <see langword="true"/> for a decompression (read) stream, <see langword="false"/> for a
    /// compression (write) stream. Selects the exception type failures are translated to.
    /// </param>
    private ZstdBoundaryStream(Stream inner, bool reading)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        this.reading = reading;
    }

    /// <summary>
    /// Builds the vendored stream <em>inside</em> the boundary and wraps it. Constructing one of the
    /// vendored streams already runs codec calls that report failure as <see cref="ZstdException"/>
    /// (sizing its buffers, applying the compression level), so building it as a constructor argument
    /// would leave that first moment outside the very boundary this type exists to provide.
    /// </summary>
    /// <param name="build">Creates the vendored stream to wrap. Owned by the result and disposed with it.</param>
    /// <param name="reading">
    /// <see langword="true"/> for a decompression (read) stream, <see langword="false"/> for a
    /// compression (write) stream. Selects the exception type failures are translated to.
    /// </param>
    public static Stream Create(Func<Stream> build, bool reading)
    {
        _ = build ?? throw new ArgumentNullException(nameof(build));

        try
        {
            return new ZstdBoundaryStream(build(), reading);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex, reading);
        }
    }

    public override bool CanRead => inner.CanRead;

    public override bool CanSeek => inner.CanSeek;

    public override bool CanWrite => inner.CanWrite;

    public override long Length => inner.Length;

    public override long Position
    {
        get => inner.Position;
        set => inner.Position = value;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        try
        {
            return inner.Read(buffer, offset, count);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
    }

    public override int Read(Span<byte> buffer)
    {
        try
        {
            return inner.Read(buffer);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        try
        {
            return await inner.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        // The hot read overload. The vendored stream completes most reads synchronously out of input it
        // has already buffered, so forward a successfully-completed ValueTask untouched rather than
        // boxing a second async state machine on top of the codec's own for every 64 KiB read.
        ValueTask<int> read;
        try
        {
            read = inner.ReadAsync(buffer, cancellationToken);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }

        return read.IsCompletedSuccessfully ? read : Awaited(read);

        async ValueTask<int> Awaited(ValueTask<int> pending)
        {
            try
            {
                return await pending.ConfigureAwait(false);
            }
            catch (ZstdException ex)
            {
                throw Translate(ex);
            }
        }
    }

    public override int ReadByte()
    {
        try
        {
            return inner.ReadByte();
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        try
        {
            inner.Write(buffer, offset, count);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        try
        {
            inner.Write(buffer);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        try
        {
            await inner.WriteAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        // Same shape as ReadAsync: pass a synchronously-completed write straight back.
        ValueTask write;
        try
        {
            write = inner.WriteAsync(buffer, cancellationToken);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }

        return write.IsCompletedSuccessfully ? write : Awaited(write);

        async ValueTask Awaited(ValueTask pending)
        {
            try
            {
                await pending.ConfigureAwait(false);
            }
            catch (ZstdException ex)
            {
                throw Translate(ex);
            }
        }
    }

    public override void WriteByte(byte value)
    {
        try
        {
            inner.WriteByte(value);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
    }

    public override void Flush()
    {
        try
        {
            inner.Flush();
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        try
        {
            await inner.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
    }

    public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

    public override void SetLength(long value) => inner.SetLength(value);

    public override async ValueTask DisposeAsync()
    {
        // Disposing a compression stream flushes its trailing frame footer, so it can fail the same
        // way a Write can — and disposal is idempotent because the interface requires the returned
        // stream to tolerate repeated disposal.
        try
        {
            if (!disposed)
            {
                disposed = true;
                await inner.DisposeAsync().ConfigureAwait(false);
            }
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
        finally
        {
            await base.DisposeAsync().ConfigureAwait(false);
        }
    }

    protected override void Dispose(bool disposing)
    {
        try
        {
            if (disposing && !disposed)
            {
                disposed = true;
                inner.Dispose();
            }
        }
        catch (ZstdException ex)
        {
            throw Translate(ex);
        }
        finally
        {
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// Rethrows a vendored failure as a public type, keeping the original as
    /// <see cref="Exception.InnerException"/> so the codec's own diagnosis is not lost.
    /// </summary>
    private static Exception Translate(ZstdException ex, bool reading) => reading
        ? new InvalidDataException(
            "ZSTD decode failed; the response body may be corrupt or not a valid ZSTD frame. " + ex.Message, ex)
        : new InvalidOperationException($"ZSTD encode failed: {ex.Message}", ex);

    private Exception Translate(ZstdException ex) => Translate(ex, reading);
}
