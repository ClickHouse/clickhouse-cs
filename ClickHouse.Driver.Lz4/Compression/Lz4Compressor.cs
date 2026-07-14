using System;
using System.IO;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// LZ4 compressor backed by the <c>K4os.Compression.LZ4</c> library, shipped as the opt-in
/// <c>ClickHouse.Driver.Lz4</c> package so its <c>K4os</c> dependency is only pulled in when you use LZ4.
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
/// throughput-bound inserts where CPU (not bandwidth) is the constraint. It also imposes the lowest
/// <b>server-side</b> load of the available codecs: LZ4 decompression is near-free, so in our benchmarks
/// the server CPU per insert was essentially the same as uncompressed and far below GZip/Brotli — making
/// it the best choice when minimizing load on the ClickHouse server matters.
/// <para>
/// <b>Recommendation:</b> use <see cref="Default"/> (fast mode, level 0) for almost all inserts. In our
/// benchmarks the compression ratio plateaus almost immediately, so higher levels cost markedly more CPU
/// for little-to-no extra ratio on typical insert data — reach for a higher level only if you have
/// measured that your data compresses meaningfully better at it.
/// </para>
/// </summary>
public sealed class Lz4Compressor : IClickHouseCompressor
{
    /// <summary>
    /// The ClickHouse native-protocol compression method byte for LZ4.
    /// </summary>
    public const byte Lz4MethodByte = 0x82;

    /// <summary>
    /// Shared default instance: LZ4 fast mode (level <c>0</c>) with a 256 KiB write buffer — the
    /// recommended setting for almost all inserts.
    /// </summary>
    public static readonly Lz4Compressor Default = new();

    private readonly LZ4Level level;
    private readonly int bufferSize;

    /// <param name="level">
    /// LZ4 level, given as the underlying <c>K4os.Compression.LZ4.LZ4Level</c> value cast to
    /// <see cref="int"/>: <c>0</c> = fast mode (default), <c>3</c>–<c>12</c> = high-compression
    /// (HC/OPT/MAX). Any other value throws <see cref="ArgumentOutOfRangeException"/>.
    /// <para>
    /// On typical (semi-compressible, short-record) insert payloads the ratio plateaus around level
    /// <c>3</c>, so <c>0</c> or <c>3</c> are usually the only levels worth using — higher levels cost
    /// markedly more CPU for little-to-no extra ratio.
    /// </para>
    /// </param>
    /// <param name="bufferSize">Size in bytes of the write buffer wrapped around the LZ4 stream. Defaults to 256 KiB.</param>
    public Lz4Compressor(int level = 0, int bufferSize = 256 * 1024)
    {
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "Buffer size must be positive.");

        this.level = ToLz4Level(level);
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
                $"LZ4 decode failed; the target buffer ({target.Length} bytes) is smaller than the decoded length.");

        return written;
    }

    // Validates an int level as a K4os LZ4Level (0 = fast, 3-12 = HC/OPT/MAX) and casts it.
    private static LZ4Level ToLz4Level(int level)
        => Enum.IsDefined(typeof(LZ4Level), level)
            ? (LZ4Level)level
            : throw new ArgumentOutOfRangeException(nameof(level), level,
                "LZ4 level must be a valid LZ4Level value cast to int: 0 (fast) or 3-12 (high-compression).");
}
