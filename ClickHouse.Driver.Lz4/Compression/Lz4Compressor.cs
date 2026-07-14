using System;
using System.IO;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// LZ4 compressor backed by the <c>K4os.Compression.LZ4</c> library, shipped as the opt-in
/// <c>ClickHouse.Driver.Lz4</c> package so its <c>K4os</c> dependency is only pulled in when you use LZ4.
/// <para>
/// LZ4 is faster than GZip/Brotli at a lower compression ratio, which makes it a good fit for
/// throughput-bound inserts where CPU (not bandwidth) is the constraint. It also imposes the lowest
/// <b>server-side</b> load of the available codecs, making
/// it the best choice when minimizing load on the ClickHouse server matters.
/// </para>
/// <para>
/// <b>Recommendation:</b> use <see cref="Default"/> (fast mode, level 0) for almost all inserts,
/// higher levels show very small gains in compression size.
/// </para>
/// </summary>
public sealed class Lz4Compressor : IClickHouseCompressor
{
    /// <summary>
    /// The ClickHouse native-protocol compression method byte for LZ4.
    /// </summary>
    public const byte Lz4MethodByte = 0x82;

    /// <summary>
    /// Shared default instance: <see cref="LZ4Level.L00_FAST"/> with a 256 KiB write buffer — the
    /// recommended setting for almost all inserts.
    /// </summary>
    public static readonly Lz4Compressor Default = new();

    private readonly LZ4Level level;
    private readonly int bufferSize;

    /// <param name="level">Compression level. Defaults to fastest, which is also the recommended level.</param>
    /// <param name="bufferSize">Size in bytes of the write buffer wrapped around the LZ4 stream. Defaults to 256 KiB.</param>
    public Lz4Compressor(LZ4Level level = LZ4Level.L00_FAST, int bufferSize = 256 * 1024)
    {
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "Buffer size must be positive.");

        this.level = level;
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
}
