using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using ClickHouse.Driver.Compression;

namespace ClickHouse.Driver.Http;

/// <summary>
/// Decides how (and whether) an HTTP <b>response</b> body must be decompressed.
/// <para>
/// The decision is driven by the response's <c>Content-Encoding</c>, never by what was requested:
/// ClickHouse picks the codec by its own fixed preference order and ignores our ordering and q-values,
/// so <c>Accept-Encoding</c> is not a reliable predictor. The header is also a <i>total</i> signal,
/// because .NET strips <c>Content-Encoding</c> once <c>AutomaticDecompression</c> has decoded a body —
/// a header still present means the bytes are still compressed.
/// </para>
/// <para>
/// Absent or <c>identity</c> passes the source stream through untouched; a codec in
/// <see cref="Decoders"/> is decoded; anything else (<c>snappy</c>, …) is unsupported.
/// Token comparison is ordinal-case-insensitive and tolerates surrounding whitespace.
/// </para>
/// </summary>
internal static class ResponseDecompression
{
    /// <summary>
    /// Codecs the driver can decode, keyed by <c>Content-Encoding</c> token. <c>br</c> is decodable
    /// but deliberately not part of <see cref="DefaultAcceptEncoding"/> — see the remarks
    /// there. HTTP's <c>deflate</c> is the zlib format (RFC 1950), which is what ClickHouse emits and
    /// what a bare <see cref="DeflateStream"/> cannot parse, so it gets a stream that handles both forms.
    /// </summary>
    private static readonly Dictionary<string, Func<Stream, bool, Stream>> Decoders =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["lz4"] = static (source, leaveOpen) => Lz4Compressor.Default.Decompress(source, leaveOpen),
            ["zstd"] = static (source, leaveOpen) => ZstdCompressor.Default.Decompress(source, leaveOpen),
            ["gzip"] = static (source, leaveOpen) => GZipCompressor.Default.Decompress(source, leaveOpen),
            ["deflate"] = static (source, leaveOpen) => new ZLibOrDeflateStream(source, leaveOpen),
            ["br"] = static (source, leaveOpen) => BrotliCompressor.Default.Decompress(source, leaveOpen),
            ["brotli"] = static (source, leaveOpen) => BrotliCompressor.Default.Decompress(source, leaveOpen),
        };

    /// <summary>
    /// The <c>Accept-Encoding</c> the driver advertises when the caller has not chosen one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ClickHouse resolves <c>Accept-Encoding</c> by scanning for tokens in a fixed preference order
    /// (<c>zstd</c> &gt; <c>br</c> &gt; <c>lz4</c> &gt; <c>snappy</c> &gt; <c>gzip</c> &gt;
    /// <c>deflate</c>), ignoring both our ordering and q-values — so the only way to influence its
    /// choice is which tokens we omit. Naming <c>zstd</c> therefore makes it the codec for every
    /// default query; the remaining tokens are the fallback for a server or intermediary that cannot
    /// do zstd.
    /// </para>
    /// <para>
    /// zstd is advertised because at the shipped <c>http_zlib_compression_level</c> (3) it beats lz4
    /// on bytes, server CPU and wall clock simultaneously — measured on real data over a real network
    /// against both a 4-vCPU server and a 16-vCPU Cloud service: 22–26% fewer bytes, 40–51% less
    /// server CPU, 19–59% less fetch time. Two caveats come with that. At
    /// <c>http_zlib_compression_level=1</c> the sweep does not hold — there zstd costs 10–18%
    /// <i>more</i> server CPU than lz4, because lz4 only becomes expensive from level 3, where it
    /// engages LZ4-HC. And zstd is the slower codec to <i>decode</i> client-side (roughly
    /// 0.7–1.4 GB/s against lz4's 1.1–2.3 GB/s), which the driver does on the caller's thread, so a
    /// CPU-bound client on a fast link is the case for asking for <c>lz4</c> explicitly.
    /// </para>
    /// <para>
    /// <c>br</c> stays omitted: it comes earlier still in the scan and is far dearer on both sides.
    /// It is decoded whenever it arrives, and a caller who wants it can ask.
    /// </para>
    /// </remarks>
    public const string DefaultAcceptEncoding = "zstd, lz4, gzip, deflate";

    /// <summary>
    /// Returns the single effective <c>Content-Encoding</c> token of <paramref name="response"/>, or
    /// <see langword="null"/> when the body is not transport-compressed. Empty and <c>identity</c>
    /// tokens count as "not compressed". Stacked codecs are joined so the caller can name them in an
    /// error — that combination is not supported.
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
    /// Wraps <paramref name="source"/> in a decoder when the response is transport-compressed. Returns
    /// <paramref name="source"/> itself (reference-equal) when the body needs no decoding; otherwise a
    /// new stream the caller owns and must dispose.
    /// </summary>
    /// <exception cref="NotSupportedException">The response uses a codec this client cannot decode.</exception>
    public static Stream Wrap(Stream source, HttpResponseMessage response, bool leaveOpen)
        => Wrap(source, GetContentEncoding(response), leaveOpen);

    /// <inheritdoc cref="Wrap(Stream, HttpResponseMessage, bool)"/>
    public static Stream Wrap(Stream source, string contentEncoding, bool leaveOpen)
    {
        if (TryWrap(source, contentEncoding, leaveOpen, out var decompressed))
            return decompressed;

        throw new NotSupportedException(DescribeUnsupported(contentEncoding));
    }

    /// <summary>
    /// Non-throwing form of <see cref="Wrap(Stream, string, bool)"/>. Returns <see langword="false"/>
    /// (leaving <paramref name="decompressed"/> <see langword="null"/>) for a codec this client cannot
    /// decode, so callers that must not fail — reading a server <i>error</i> body above all — can
    /// degrade gracefully instead of masking the server's message with a decompression crash.
    /// </summary>
    public static bool TryWrap(Stream source, string contentEncoding, bool leaveOpen, out Stream decompressed)
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

        if (Decoders.TryGetValue(token, out var decoder))
        {
            decompressed = decoder(source, leaveOpen);
            return true;
        }

        decompressed = null;
        return false;
    }

    /// <summary>
    /// Builds the actionable message used when a response carries a codec this client cannot decode.
    /// </summary>
    public static string DescribeUnsupported(string contentEncoding) =>
        $"ClickHouse returned a response compressed with Content-Encoding: '{contentEncoding?.Trim()}', which this client cannot decode. " +
        $"Only {string.Join(", ", Decoders.Keys)} are supported. Remove the codec from AcceptEncoding — on QueryOptions or " +
        "ClickHouseClientSettings — so the server falls back to one of those, or use ExecuteRawResultAsync and decode the body yourself.";

    private static bool IsToken(string value, string expected)
        => string.Equals(value, expected, StringComparison.OrdinalIgnoreCase);
}
