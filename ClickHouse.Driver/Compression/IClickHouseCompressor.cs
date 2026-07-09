using System.IO;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// Compresses an outgoing request body (e.g. a binary insert payload) and declares the HTTP
/// <c>Content-Encoding</c> the server must use to decompress it. The presence of a compressor is
/// itself the on/off switch: pass <see langword="null"/> where a compressor is accepted to send the
/// body uncompressed.
/// </summary>
public interface IClickHouseCompressor
{
    /// <summary>
    /// The HTTP <c>Content-Encoding</c> token for this codec (e.g. <c>"gzip"</c>). Sent as the
    /// request's <c>Content-Encoding</c> header so ClickHouse knows how to decompress the body.
    /// Must match a codec the server understands for the request body.
    /// </summary>
    string ContentEncoding { get; }

    /// <summary>
    /// Wraps <paramref name="destination"/> in a compressing write stream. Bytes written to the
    /// returned stream are compressed and forwarded to <paramref name="destination"/>. Disposing the
    /// returned stream flushes all remaining compressed bytes; when <paramref name="leaveOpen"/> is
    /// <see langword="true"/>, <paramref name="destination"/> is left open afterwards so the caller
    /// can continue using it (e.g. seek and read the compressed result).
    /// </summary>
    Stream Compress(Stream destination, bool leaveOpen);
}
