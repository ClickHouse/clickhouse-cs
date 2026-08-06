using System;
using System.IO;
using System.Threading;
using ClickHouse.Driver.Vendor.ZstdSharp;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// ZSTD compressor backed by a vendored, dependency-free copy of the <c>ZstdSharp</c> library
/// (a pure-managed port of zstd, bundled into the driver — see <c>Vendor/ZstdSharp/README.md</c>),
/// so ZSTD is available without pulling in any third-party or native runtime dependency.
/// <para>
/// ZSTD is the codec query responses are negotiated with, and the codec binary inserts are
/// compressed with, by default. Decoding happens on the calling thread; a caller who wants a
/// different trade-off between payload size and client CPU can name another codec explicitly
/// (<c>AcceptEncoding</c> for responses, <see cref="IClickHouseCompressor"/> for inserts).
/// </para>
/// <para>
/// <b>Recommendation:</b> use <see cref="Default"/> (level <see cref="DefaultLevel"/>, zstd's own
/// default) unless you have measured a higher level to be worth its CPU on your data.
/// </para>
/// </summary>
public sealed class ZstdCompressor : IClickHouseCompressor
{
    /// <summary>
    /// The ClickHouse native-protocol compression method byte for ZSTD.
    /// </summary>
    public const byte ZstdMethodByte = 0x90;

    /// <summary>
    /// zstd's own default compression level, used when none is given.
    /// </summary>
    public const int DefaultLevel = 3;

    /// <summary>
    /// Shared default instance: level <see cref="DefaultLevel"/> with a 256 KiB write buffer.
    /// Safe to use concurrently from any number of threads.
    /// </summary>
    public static readonly ZstdCompressor Default = new();

    /// <summary>
    /// Compression contexts for the native block path. zstd contexts are stateful and, unlike the
    /// stateless LZ4 codec, cannot be shared across concurrent calls — but they are also the
    /// expensive part (each holds a compression workspace), so allocating one per call is exactly
    /// what they exist to avoid. Every context is therefore rented for the duration of one
    /// <see cref="Encode"/> / <see cref="Decode"/> call and owned exclusively by that call, which
    /// makes the shared <see cref="Default"/> instance thread-safe. The pool is per compressor
    /// instance, so a pooled context always carries this instance's <see cref="level"/>.
    /// <para>
    /// The stream paths need no pooling: <see cref="Compress"/> and <see cref="Decompress"/> each
    /// hand their stream a private context that the stream owns and disposes.
    /// </para>
    /// </summary>
    private readonly ContextPool<Compressor> encoders;
    private readonly ContextPool<Decompressor> decoders;

    private readonly int level;
    private readonly int bufferSize;

    /// <param name="level">
    /// Compression level, between <see cref="MinLevel"/> and <see cref="MaxLevel"/>. Defaults to
    /// <see cref="DefaultLevel"/>. Negative levels trade ratio for speed.
    /// </param>
    /// <param name="bufferSize">Size in bytes of the write buffer wrapped around the ZSTD stream. Defaults to 256 KiB.</param>
    public ZstdCompressor(int level = DefaultLevel, int bufferSize = 256 * 1024)
    {
        if (level < MinLevel || level > MaxLevel)
        {
            throw new ArgumentOutOfRangeException(
                nameof(level), level, $"Compression level must be between {MinLevel} and {MaxLevel}.");
        }

        if (bufferSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "Buffer size must be positive.");
        }

        this.level = level;
        this.bufferSize = bufferSize;
        this.encoders = new ContextPool<Compressor>(() => new Compressor(level));
        this.decoders = new ContextPool<Decompressor>(static () => new Decompressor());
    }

    /// <summary>
    /// The lowest level the codec accepts. Negative and version-dependent, so read it rather than
    /// hard-coding it.
    /// </summary>
    public static int MinLevel => Compressor.MinCompressionLevel;

    /// <summary>
    /// The highest level the codec accepts.
    /// </summary>
    public static int MaxLevel => Compressor.MaxCompressionLevel;

    /// <inheritdoc />
    public string ContentEncoding => "zstd";

    /// <inheritdoc />
    public byte MethodByte => ZstdMethodByte;

    /// <inheritdoc />
    public Stream Compress(Stream destination, bool leaveOpen)
        // ZstdBoundaryStream sits directly on the vendored stream so the internal ZstdException the
        // codec reports failures with never reaches the caller, exactly as Encode/Decode translate it
        // on the block path — see ZstdBoundaryStream.
        => new PooledWriteBufferStream(
            ZstdBoundaryStream.Create(
                () => new CompressionStream(destination, this.level, bufferSize: 0, leaveOpen: leaveOpen),
                reading: false),
            this.bufferSize);

    /// <inheritdoc />
    public Stream Decompress(Stream source, bool leaveOpen)
        // The vendored DecompressionStream returns from Read as soon as any output has been decoded
        // rather than filling the caller's buffer first, which is what the read path needs: it asks
        // for a whole 64 KiB buffer at a time, so a query that trickles rows must still surface its
        // first row before that much output exists. (LZ4 needs an explicit `interactive` flag for
        // the same behavior; zstd's is unconditional.) checkEndOfStream surfaces a body truncated
        // mid-frame instead of silently returning short data. The boundary wrapper turns the codec's
        // internal ZstdException into InvalidDataException, the same type gzip/deflate/brotli/LZ4
        // already throw for a corrupt body — see ZstdBoundaryStream.
        => ZstdBoundaryStream.Create(
            () => new DecompressionStream(source, bufferSize: 0, checkEndOfStream: true, leaveOpen: leaveOpen),
            reading: true);

    /// <inheritdoc />
    public int MaxEncodedLength(int sourceLength) => Compressor.GetCompressBound(sourceLength);

    /// <inheritdoc />
    public int Encode(ReadOnlySpan<byte> source, Span<byte> target)
    {
        if (source.IsEmpty)
        {
            return 0;
        }

        var encoder = this.encoders.Rent();
        try
        {
            // Uses the level this compressor was constructed with, exactly like Compress.
            if (!encoder.TryWrap(source, target, out var written) || written <= 0)
            {
                throw new InvalidOperationException(
                    $"ZSTD encode failed; the target buffer ({target.Length} bytes) is likely too small. " +
                    $"Size it to at least MaxEncodedLength({source.Length}).");
            }

            return written;
        }
        catch (ZstdException ex)
        {
            throw new InvalidOperationException($"ZSTD encode failed: {ex.Message}", ex);
        }
        finally
        {
            this.encoders.Return(encoder);
        }
    }

    /// <inheritdoc />
    public int Decode(ReadOnlySpan<byte> source, Span<byte> target)
    {
        if (source.IsEmpty)
        {
            return 0;
        }

        var decoder = this.decoders.Rent();
        try
        {
            if (!decoder.TryUnwrap(source, target, out var written))
            {
                throw new InvalidOperationException(
                    $"ZSTD decode failed; the target buffer ({target.Length} bytes) is smaller than the decoded " +
                    "length. Size it to the block's uncompressed length.");
            }

            return written;
        }
        catch (ZstdException ex)
        {
            throw new InvalidOperationException(
                "ZSTD decode failed; the source block may be corrupt or not a valid ZSTD frame. " + ex.Message, ex);
        }
        finally
        {
            this.decoders.Return(decoder);
        }
    }

    /// <summary>
    /// A fixed-capacity, lock-free pool of zstd contexts. <see cref="Rent"/> takes exclusive
    /// ownership of a context (or builds a transient one when the pool is empty) and
    /// <see cref="Return"/> gives it back, disposing it when the pool is full — so retention is
    /// bounded by the pool's capacity rather than by peak concurrency.
    /// <para>
    /// Not disposable on purpose: the shared <see cref="Default"/> instance has no owner who could
    /// dispose it, and the vendored contexts hold their unmanaged memory in a
    /// <see cref="System.Runtime.InteropServices.SafeHandle"/>, so anything the pool still holds
    /// when it becomes unreachable is released by that handle's critical finalizer.
    /// </para>
    /// </summary>
    private sealed class ContextPool<T>
        where T : class, IDisposable
    {
        private readonly Func<T> factory;
        private readonly T[] slots;

        public ContextPool(Func<T> factory)
        {
            this.factory = factory;
            this.slots = new T[Math.Max(1, Environment.ProcessorCount)];
        }

        public T Rent()
        {
            for (var i = 0; i < this.slots.Length; i++)
            {
                // Exchange, so a context can never be handed to two callers at once.
                var pooled = Interlocked.Exchange(ref this.slots[i], null);
                if (pooled != null)
                {
                    return pooled;
                }
            }

            return this.factory();
        }

        public void Return(T context)
        {
            for (var i = 0; i < this.slots.Length; i++)
            {
                if (Interlocked.CompareExchange(ref this.slots[i], context, null) is null)
                {
                    return;
                }
            }

            context.Dispose();
        }
    }
}
