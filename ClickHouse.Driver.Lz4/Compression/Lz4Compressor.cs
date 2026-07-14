using System;
using System.IO;
using System.IO.Compression;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// LZ4 compressor backed by the <c>K4os.Compression.LZ4</c> library. Shipped as the opt-in
/// <c>ClickHouse.Driver.Lz4</c> package so the core <c>ClickHouse.Driver</c> package keeps a
/// Microsoft-only dependency set.
/// <para>
/// Implements both paths of <see cref="IClickHouseCompressor"/>:
/// <list type="bullet">
/// <item>the HTTP request-body path (<see cref="ContentEncoding"/> = <c>"lz4"</c> + <see cref="Compress"/>),
/// producing the standard LZ4 frame format that ClickHouse accepts as <c>Content-Encoding: lz4</c> for
/// binary inserts;</item>
/// <item>the native-TCP block path (<see cref="MethodByte"/> = <c>0x82</c> + <see cref="Encode"/>/
/// <see cref="Decode"/>/<see cref="MaxEncodedLength"/>) over raw LZ4 blocks.</item>
/// </list>
/// </para>
/// LZ4 is much faster than GZip/Brotli at a lower compression ratio, which makes it a good fit for
/// throughput-bound inserts where CPU (not bandwidth) is the constraint.
/// </summary>
public sealed class Lz4Compressor : IClickHouseCompressor
{
    /// <summary>
    /// The ClickHouse native-protocol compression method byte for LZ4.
    /// </summary>
    public const byte Lz4MethodByte = 0x82;

    /// <summary>
    /// Shared default instance: <see cref="CompressionLevel.Fastest"/> (LZ4 fast mode) with a 256 KiB
    /// write buffer.
    /// </summary>
    public static readonly Lz4Compressor Default = new();

    private readonly LZ4Level level;
    private readonly int bufferSize;

    /// <param name="level">
    /// Compression level, mapped onto the LZ4 level ladder: <see cref="CompressionLevel.NoCompression"/>
    /// and <see cref="CompressionLevel.Fastest"/> use LZ4 fast mode, <see cref="CompressionLevel.Optimal"/>
    /// uses a high-compression level, and <see cref="CompressionLevel.SmallestSize"/> uses the maximum
    /// level. Defaults to <see cref="CompressionLevel.Fastest"/>.
    /// </param>
    /// <param name="bufferSize">Size in bytes of the write buffer wrapped around the LZ4 stream. Defaults to 256 KiB.</param>
    public Lz4Compressor(CompressionLevel level = CompressionLevel.Fastest, int bufferSize = 256 * 1024)
    {
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "Buffer size must be positive.");

        this.level = MapLevel(level);
        this.bufferSize = bufferSize;
    }

    /// <inheritdoc />
    public string ContentEncoding => "lz4";

    /// <inheritdoc />
    public byte MethodByte => Lz4MethodByte;

    /// <inheritdoc />
    public Stream Compress(Stream destination, bool leaveOpen)
        => new BufferedStream(LZ4Stream.Encode(destination, this.level, extraMemory: 0, leaveOpen: leaveOpen), this.bufferSize);

    /// <inheritdoc />
    public int MaxEncodedLength(int sourceLength) => LZ4Codec.MaximumOutputSize(sourceLength);

    /// <inheritdoc />
    public int Encode(ReadOnlySpan<byte> source, Span<byte> target, CompressionLevel level = CompressionLevel.Fastest)
    {
        if (source.IsEmpty)
            return 0;

        var written = LZ4Codec.Encode(source, target, MapLevel(level));
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
                $"LZ4 decode failed; the target buffer ({target.Length} bytes) is smaller than the decoded length.");

        return written;
    }

    private static LZ4Level MapLevel(CompressionLevel level) => level switch
    {
        CompressionLevel.NoCompression => LZ4Level.L00_FAST,
        CompressionLevel.Fastest => LZ4Level.L00_FAST,
        CompressionLevel.Optimal => LZ4Level.L09_HC,
        CompressionLevel.SmallestSize => LZ4Level.L12_MAX,
        _ => LZ4Level.L00_FAST,
    };
}
