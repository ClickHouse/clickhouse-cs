using System;
using System.IO;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// Compresses ClickHouse payloads. Two independent paths are exposed:
/// <list type="bullet">
/// <item>
/// the <b>HTTP request-body path</b> (<see cref="ContentEncoding"/> + <see cref="Compress"/>) used by
/// the HTTP driver for binary inserts, where the presence of a compressor is itself the on/off switch —
/// pass <see langword="null"/> where a compressor is accepted to send the body uncompressed;
/// </item>
/// <item>
/// the <b>native-TCP block path</b> (<see cref="MethodByte"/>, <see cref="MaxEncodedLength"/>,
/// <see cref="Encode"/>, <see cref="Decode"/>) used by the native protocol's compression frame. This
/// path is optional: codecs that only support the HTTP path (e.g. the built-in
/// <see cref="GZipCompressor"/> and <see cref="BrotliCompressor"/>) inherit default implementations that
/// throw <see cref="NotSupportedException"/>. Block codecs such as LZ4/ZSTD (shipped as opt-in packages)
/// override them.
/// </item>
/// </list>
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
    /// The native-protocol compression method byte identifying this codec in a ClickHouse compression
    /// frame header (e.g. <c>0x82</c> LZ4, <c>0x90</c> ZSTD, <c>0x02</c> NONE). Only meaningful for codecs
    /// that support the native block path; the default throws <see cref="NotSupportedException"/>.
    /// </summary>
    byte MethodByte => throw new NotSupportedException($"{GetType().Name} does not support native block framing.");

    /// <summary>
    /// Wraps <paramref name="destination"/> in a compressing write stream. Bytes written to the
    /// returned stream are compressed and forwarded to <paramref name="destination"/>. Disposing the
    /// returned stream flushes all remaining compressed bytes; when <paramref name="leaveOpen"/> is
    /// <see langword="true"/>, <paramref name="destination"/> is left open afterwards so the caller
    /// can continue using it (e.g. seek and read the compressed result).
    /// </summary>
    Stream Compress(Stream destination, bool leaveOpen);

    /// <summary>
    /// The maximum number of bytes <see cref="Encode"/> may write for a source of
    /// <paramref name="sourceLength"/> bytes. Callers use this to size the <c>target</c> buffer. Only
    /// meaningful for codecs that support the native block path; the default throws
    /// <see cref="NotSupportedException"/>.
    /// </summary>
    int MaxEncodedLength(int sourceLength) => throw new NotSupportedException($"{GetType().Name} does not support native block framing.");

    /// <summary>
    /// Compresses <paramref name="source"/> into <paramref name="target"/> (which must be at least
    /// <see cref="MaxEncodedLength"/> bytes) and returns the number of bytes written. Only meaningful for
    /// codecs that support the native block path; the default throws <see cref="NotSupportedException"/>.
    /// </summary>
    /// <param name="level">
    /// Codec-defined compression level (<c>0</c> = the codec's default). The meaning is specific to the
    /// implementing codec, which validates it and throws for out-of-range values — a neutral <see cref="int"/>
    /// is used here rather than a fixed enum so each codec can expose its full native level range.
    /// </param>
    int Encode(ReadOnlySpan<byte> source, Span<byte> target, int level = 0)
        => throw new NotSupportedException($"{GetType().Name} does not support native block framing.");

    /// <summary>
    /// Decompresses <paramref name="source"/> into <paramref name="target"/> (which must be sized to the
    /// known uncompressed length) and returns the number of bytes written. Only meaningful for codecs that
    /// support the native block path; the default throws <see cref="NotSupportedException"/>.
    /// </summary>
    int Decode(ReadOnlySpan<byte> source, Span<byte> target)
        => throw new NotSupportedException($"{GetType().Name} does not support native block framing.");
}
