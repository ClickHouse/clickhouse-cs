using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using ClickHouse.Driver.Compression;

namespace ClickHouse.Driver.Http;

/// <summary>
/// The single place that decides how (and whether) an HTTP <b>response</b> body must be decompressed.
/// <para>
/// The decision is driven exclusively by the response's <c>Content-Encoding</c> header, never by what the
/// client asked for: ClickHouse picks the response codec by its own fixed preference order
/// (zstd &gt; br &gt; lz4 &gt; gzip) and ignores both client order and q-values, so the request's
/// <c>Accept-Encoding</c> is not a reliable predictor. The header is also a <i>total</i> signal, because
/// .NET strips <c>Content-Encoding</c> once <c>AutomaticDecompression</c> has already decoded the body —
/// so a header that is still present means the bytes are still compressed.
/// </para>
/// <para>
/// Resolution table:
/// <list type="bullet">
/// <item>absent or <c>identity</c> — the source stream is returned untouched (same instance);</item>
/// <item>the configured response compressor's <see cref="IClickHouseCompressor.ContentEncoding"/> —
/// decoded by that compressor;</item>
/// <item><c>gzip</c> / <c>deflate</c> / <c>br</c> / <c>brotli</c> — decoded by the BCL stream;</item>
/// <item>anything else — unsupported (an actionable error naming the codec).</item>
/// </list>
/// Token comparison is case-insensitive, culture-invariant (ordinal) and tolerates surrounding whitespace.
/// </para>
/// </summary>
internal static class ResponseDecompression
{
    /// <summary>
    /// Returns the single effective <c>Content-Encoding</c> token of <paramref name="response"/>, or
    /// <see langword="null"/> when the body is not transport-compressed. Empty and <c>identity</c> tokens
    /// are treated as "not compressed". When a server stacks several codecs the tokens are joined so the
    /// caller can surface them in an error — that combination is not supported.
    /// </summary>
    public static string GetContentEncoding(HttpResponseMessage response)
    {
        if (response is null)
            return null;

        List<string> effective = null;
        foreach (var token in response.Content.Headers.ContentEncoding)
        {
            var trimmed = token?.Trim();
            if (string.IsNullOrEmpty(trimmed) || IsToken(trimmed, "identity"))
                continue;

            (effective ??= new List<string>(1)).Add(trimmed);
        }

        if (effective is null)
            return null;

        return effective.Count == 1 ? effective[0] : string.Join(", ", effective);
    }

    /// <summary>
    /// Applies the resolution table to <paramref name="source"/>. Returns <paramref name="source"/> itself
    /// (reference-equal) when the body needs no decoding; otherwise a new decompressing read stream that
    /// the caller owns and must dispose.
    /// </summary>
    /// <exception cref="NotSupportedException">The response is encoded with a codec this client cannot decode.</exception>
    public static Stream Wrap(Stream source, HttpResponseMessage response, IClickHouseCompressor responseCompressor, bool leaveOpen)
        => Wrap(source, GetContentEncoding(response), responseCompressor, leaveOpen);

    /// <inheritdoc cref="Wrap(Stream, HttpResponseMessage, IClickHouseCompressor, bool)"/>
    public static Stream Wrap(Stream source, string contentEncoding, IClickHouseCompressor responseCompressor, bool leaveOpen)
    {
        if (TryWrap(source, contentEncoding, responseCompressor, leaveOpen, out var decompressed))
            return decompressed;

        throw new NotSupportedException(DescribeUnsupported(contentEncoding, responseCompressor));
    }

    /// <summary>
    /// Non-throwing form of <see cref="Wrap(Stream, string, IClickHouseCompressor, bool)"/> — and the one
    /// place the resolution table lives. Returns <see langword="false"/> (leaving
    /// <paramref name="decompressed"/> <see langword="null"/>) for a codec this client cannot decode, so
    /// callers that must not fail — reading a server <i>error</i> body, above all — can degrade gracefully
    /// instead of masking the server's message with a decompression crash.
    /// </summary>
    public static bool TryWrap(Stream source, string contentEncoding, IClickHouseCompressor responseCompressor, bool leaveOpen, out Stream decompressed)
    {
        if (source is null)
            throw new ArgumentNullException(nameof(source));

        var token = contentEncoding?.Trim();

        // Absent / identity: the body is plaintext already. Hand back the very same instance so callers
        // can tell "nothing was wrapped" by reference and skip an extra disposal.
        if (string.IsNullOrEmpty(token) || IsToken(token, "identity"))
        {
            decompressed = source;
            return true;
        }

        // The configured response compressor wins whenever the server actually used its codec.
        if (responseCompressor != null && IsToken(token, responseCompressor.ContentEncoding))
        {
            decompressed = responseCompressor.Decompress(source, leaveOpen);
            return true;
        }

        if (IsToken(token, "gzip"))
        {
            decompressed = new GZipStream(source, CompressionMode.Decompress, leaveOpen);
            return true;
        }

        if (IsToken(token, "deflate"))
        {
            decompressed = new DeflateStream(source, CompressionMode.Decompress, leaveOpen);
            return true;
        }

        if (IsToken(token, "br") || IsToken(token, "brotli"))
        {
            decompressed = new BrotliStream(source, CompressionMode.Decompress, leaveOpen);
            return true;
        }

        decompressed = null;
        return false;
    }

    /// <summary>
    /// Builds the actionable message used when the response carries a codec this client cannot decode.
    /// Names the codec and tells the caller how to fix it.
    /// </summary>
    public static string DescribeUnsupported(string contentEncoding, IClickHouseCompressor responseCompressor)
    {
        var configured = responseCompressor is null
            ? "no response compressor is configured"
            : $"the configured response compressor decodes '{responseCompressor.ContentEncoding}'";

        return
            $"ClickHouse returned a response compressed with Content-Encoding: '{contentEncoding?.Trim()}', which this client cannot decode ({configured}). " +
            "Set ClickHouseClientSettings.ResponseCompressor (or QueryOptions.ResponseCompressor, or 'ResponseCompression=lz4|gzip|br' in the connection string) " +
            "to a compressor whose ContentEncoding matches, drop the codec from Accept-Encoding so the server falls back to one the client supports " +
            "(gzip, deflate, br or lz4), or use ExecuteRawResultAsync and decode the body yourself.";
    }

    private static bool IsToken(string value, string expected)
        => string.Equals(value, expected, StringComparison.OrdinalIgnoreCase);
}
