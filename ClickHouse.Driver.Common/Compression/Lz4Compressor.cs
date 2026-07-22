using System;
using System.IO;
using ClickHouse.Driver.Vendor.K4os.Compression.LZ4;
using ClickHouse.Driver.Vendor.K4os.Compression.LZ4.Streams;
using K4osLevel = ClickHouse.Driver.Vendor.K4os.Compression.LZ4.LZ4Level;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// LZ4 compressor backed by a vendored, dependency-free copy of the <c>K4os.Compression.LZ4</c>
/// library (bundled into the driver — see <c>Vendor/K4os/README.md</c>), so LZ4 is available without
/// pulling in any third-party runtime dependency.
/// <para>
/// LZ4 is faster than GZip/Brotli at a lower compression ratio, which makes it a good fit for
/// throughput-bound inserts where CPU (not bandwidth) is the constraint. It also imposes the lowest
/// <b>server-side</b> load of the available codecs, making it the best choice when minimizing load on
/// the ClickHouse server matters.
/// </para>
/// <para>
/// <b>Recommendation:</b> use <see cref="Default"/> (<see cref="Lz4Level.Fast"/>) for almost all
/// inserts; higher levels show very small gains in compression size at a large CPU cost.
/// </para>
/// </summary>
public sealed class Lz4Compressor : IClickHouseCompressor
{
    /// <summary>
    /// The ClickHouse native-protocol compression method byte for LZ4.
    /// </summary>
    public const byte Lz4MethodByte = 0x82;

    /// <summary>
    /// Shared default instance: <see cref="Lz4Level.Fast"/> with a 256 KiB write buffer — the
    /// recommended setting for almost all inserts.
    /// </summary>
    public static readonly Lz4Compressor Default = new();

    private readonly K4osLevel level;
    private readonly int bufferSize;

    /// <param name="level">Compression level. Defaults to <see cref="Lz4Level.Fast"/>, which is also the recommended level.</param>
    /// <param name="bufferSize">Size in bytes of the write buffer wrapped around the LZ4 stream. Defaults to 256 KiB.</param>
    public Lz4Compressor(Lz4Level level = Lz4Level.Fast, int bufferSize = 256 * 1024)
    {
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "Buffer size must be positive.");

        // Lz4Level's numeric values match the vendored codec's LZ4Level exactly.
        this.level = (K4osLevel)(int)level;
        this.bufferSize = bufferSize;
    }

    /// <inheritdoc />
    public string ContentEncoding => "lz4";

    /// <inheritdoc />
    public byte MethodByte => Lz4MethodByte;

    /// <inheritdoc />
    public Stream Compress(Stream destination, bool leaveOpen)
        => new PooledWriteBufferStream(LZ4Stream.Encode(destination, this.level, extraMemory: 0, leaveOpen: leaveOpen), this.bufferSize);

    /// <inheritdoc />
    public int MaxEncodedLength(int sourceLength) => LZ4Codec.MaximumOutputSize(sourceLength);

    /// <inheritdoc />
    public int Encode(ReadOnlySpan<byte> source, Span<byte> target)
    {
        if (source.IsEmpty)
            return 0;

        // Uses the level this compressor was constructed with, exactly like Compress.
        var written = LZ4Codec.Encode(source, target, this.level);
        if (written <= 0)
            throw new InvalidOperationException(
                $"LZ4 encode failed; the target buffer ({target.Length} bytes) is likely too small. " +
                $"Size it to at least MaxEncodedLength({source.Length}).");

        return written;
    }

    /// <inheritdoc />
    public int Decode(ReadOnlySpan<byte> source, Span<byte> target)
    {
        if (source.IsEmpty)
            return 0;

        var written = LZ4Codec.Decode(source, target);
        if (written < 0)
            throw new InvalidOperationException(
                $"LZ4 decode failed; the target buffer ({target.Length} bytes) may be smaller than the decoded " +
                "length, or the source block may be corrupt or not valid LZ4 data.");

        return written;
    }
}
