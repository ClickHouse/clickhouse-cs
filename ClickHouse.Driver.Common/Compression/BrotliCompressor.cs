using System;
using System.IO;
using System.IO.Compression;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// Brotli request-body compressor. Available on every targeted framework (net6+) via the BCL with no
/// third-party dependency; ClickHouse accepts <c>Content-Encoding: br</c> for request bodies. Brotli
/// generally reaches a smaller payload than GZip, trading more CPU for it — so it favours
/// bandwidth-constrained links. Note that <see cref="CompressionLevel.Optimal"/>/<see cref="CompressionLevel.SmallestSize"/>
/// map to high Brotli quality levels that are markedly slower than <see cref="CompressionLevel.Fastest"/>.
/// </summary>
public sealed class BrotliCompressor : IClickHouseCompressor
{
    /// <summary>
    /// Shared default instance: <see cref="CompressionLevel.Fastest"/> with a 256 KiB write buffer.
    /// </summary>
    public static readonly BrotliCompressor Default = new();

    private readonly CompressionLevel level;
    private readonly int bufferSize;

    /// <param name="level">Brotli compression level. Defaults to <see cref="CompressionLevel.Fastest"/>.</param>
    /// <param name="bufferSize">Size in bytes of the write buffer wrapped around the Brotli stream. Defaults to 256 KiB.</param>
    public BrotliCompressor(CompressionLevel level = CompressionLevel.Fastest, int bufferSize = 256 * 1024)
    {
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "Buffer size must be positive.");

        this.level = level;
        this.bufferSize = bufferSize;
    }

    /// <inheritdoc />
    public string ContentEncoding => "br";

    /// <inheritdoc />
    public Stream Compress(Stream destination, bool leaveOpen)
        => new PooledBufferStream(new BrotliStream(destination, level, leaveOpen), bufferSize);
}
