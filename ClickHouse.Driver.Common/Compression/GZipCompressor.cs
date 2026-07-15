using System;
using System.IO;
using System.IO.Compression;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// GZip request-body compressor — the default codec for binary inserts. Exposes the level and write
/// buffer size as knobs so callers can trade CPU for payload size.
/// </summary>
public sealed class GZipCompressor : IClickHouseCompressor
{
    /// <summary>
    /// Shared default instance: <see cref="CompressionLevel.Fastest"/> with a 256 KiB write buffer.
    /// </summary>
    public static readonly GZipCompressor Default = new();

    private readonly CompressionLevel level;
    private readonly int bufferSize;

    /// <param name="level">GZip compression level. Defaults to <see cref="CompressionLevel.Fastest"/>.</param>
    /// <param name="bufferSize">Size in bytes of the write buffer wrapped around the GZip stream. Defaults to 256 KiB.</param>
    public GZipCompressor(CompressionLevel level = CompressionLevel.Fastest, int bufferSize = 256 * 1024)
    {
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "Buffer size must be positive.");

        this.level = level;
        this.bufferSize = bufferSize;
    }

    /// <inheritdoc />
    public string ContentEncoding => "gzip";

    /// <inheritdoc />
    public Stream Compress(Stream destination, bool leaveOpen)
        => new PooledBufferStream(new GZipStream(destination, level, leaveOpen), bufferSize);
}
