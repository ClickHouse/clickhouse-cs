using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Http;

namespace ClickHouse.Driver.ADO;

/// <summary>
/// Represents the raw HTTP response from a ClickHouse query executed with a custom FORMAT clause.
/// </summary>
/// <remarks>
/// <para>
/// This class provides direct access to the response content without any parsing,
/// allowing you to handle custom output formats (e.g., FORMAT JSON, FORMAT CSV) yourself.
/// </para>
/// <para>
/// <b>Consume the body through one member.</b> A raw result is fetched with
/// <see cref="HttpCompletionOption.ResponseHeadersRead"/>, so the content is not buffered up front and the
/// stream members — <see cref="ReadAsStreamAsync"/>, <see cref="CopyToAsync"/> and
/// <see cref="ReadDecompressedStreamAsync"/> — read it where the last reader left off. Mixing them after a
/// partial read gives a wrong answer: a decode over a part-consumed body usually throws, while raw bytes
/// taken after a partial decode are silently short, since the decoder reads ahead.
/// <see cref="ReadAsByteArrayAsync"/> and <see cref="ReadAsStringAsync"/> buffer the whole body, so they
/// can safely be followed by another read. Not safe for concurrent use.
/// </para>
/// <para>
/// <b>In-band exceptions and compression.</b> A query that fails after its <c>200 OK</c> has been committed
/// reports the failure inside the body (the <c>http_write_exception_in_output_format</c> setting), and the
/// members below raise the server's error as a <see cref="ClickHouseServerException"/> rather than leaking
/// that block or a truncated body. The server writes it into the <i>encoded</i> body, though, so it can only
/// be found in plaintext: on a transport-compressed response — which a raw request only gets when the caller
/// asked for a codec — the four verbatim members hand the compressed bytes over undetected, as they must,
/// and <see cref="ReadDecompressedStreamAsync"/> is the member that surfaces the error.
/// </para>
/// </remarks>
public class ClickHouseRawResult : IDisposable
{
    private readonly HttpResponseMessage response;
    private readonly string exceptionTag;
    private byte[] bufferedContent;
    private bool contentConsumed;

    /// <summary>
    /// The stream <see cref="ReadDecompressedStreamAsync"/> vended, if any: the decoder it inserted over
    /// the content stream, under the exception-tag scanner when one is engaged. Kept so repeated calls hand
    /// back that same stream rather than stacking a second decoder over an already-partly-consumed body (or
    /// a fresh scanner whose ring buffer has observed nothing).
    /// </summary>
    private Stream decompressedStream;

    /// <summary>
    /// The decoder <see cref="ReadDecompressedStreamAsync"/> inserted, if any — the only stream in that
    /// chain that is ours to release: a decoder holds pooled buffers and has no finalizer to fall back on,
    /// while the content stream below it belongs to <see cref="response"/>.
    /// </summary>
    private Stream ownedDecoder;

    internal ClickHouseRawResult(HttpResponseMessage response)
    {
        this.response = response;

        // When the server-side setting http_write_exception_in_output_format is enabled it sends this
        // leading header carrying a per-query token, and — if the query fails after the 200 OK is already
        // committed and rows are streaming — appends an in-band exception block delimited by that token to
        // the body before closing the connection. Capture the token so the accessors below can surface a
        // ClickHouseServerException instead of leaking the raw block / a truncated body to the caller.
        if (response.Headers.TryGetValues(ExceptionTagAwareStream.HeaderName, out var tagValues))
            exceptionTag = tagValues.FirstOrDefault();
    }

    /// <summary>
    /// HTTP <c>Content-Encoding</c> the response body is encoded with (e.g. <c>"zstd"</c>,
    /// <c>"gzip"</c>), or <see langword="null"/> when the body is not transport-compressed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When a caller-supplied <see cref="HttpClient"/> has <c>AutomaticDecompression</c> enabled for the
    /// negotiated algorithm, the framework strips <c>Content-Encoding</c> after decompressing, so this
    /// property will be <see langword="null"/> even though compression was used on the wire. The handler
    /// the driver builds for itself leaves that mask off, so it does not happen there.
    /// </para>
    /// <para>
    /// The HTTP-standard <c>identity</c> token (meaning "no encoding") is normalized to
    /// <see langword="null"/> so callers don't have to special-case it.
    /// </para>
    /// </remarks>
    public string ContentEncoding
    {
        get
        {
            var value = response.Content.Headers.ContentEncoding.FirstOrDefault();
            return string.Equals(value, "identity", StringComparison.OrdinalIgnoreCase) ? null : value;
        }
    }

    /// <summary>
    /// Reads the response content as a stream.
    /// </summary>
    /// <returns>A task that resolves to the response content stream.</returns>
    /// <remarks>
    /// If the server reports an in-band mid-stream exception, reading the returned stream throws a
    /// <see cref="ClickHouseServerException"/> once the end of the body is reached. Bytes read before
    /// that point are returned as-is, so a caller that parses incrementally may already have consumed
    /// a partial result — including the server's raw in-band exception block — before the throw. The
    /// marker exists only in plaintext, so on a transport-compressed body this pass-through cannot find
    /// it: use <see cref="ReadDecompressedStreamAsync"/>, or look for the block in what you decode.
    /// </remarks>
    public Task<Stream> ReadAsStreamAsync() =>
        string.IsNullOrEmpty(exceptionTag) ? response.Content.ReadAsStreamAsync() : WrapContentStreamAsync();

    private async Task<Stream> WrapContentStreamAsync()
    {
        // If a buffering accessor already materialized (and validated) the whole body, serve the streaming
        // accessors from that buffer too. Re-reading response.Content here would hand back the underlying
        // content stream the buffering read already drained to EOF — yielding an empty body. Buffering once
        // and re-serving keeps all four accessors consistent with each other and with the untagged
        // HttpContent path. The buffer is only ever set after a clean read, so no marker scan is needed.
        if (bufferedContent != null)
            return new MemoryStream(bufferedContent, writable: false);

        var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
        contentConsumed = true;
        return new ExceptionTagAwareStream(stream, exceptionTag, throwAtEndOfStream: true);
    }

    // A streaming accessor (ReadAsStreamAsync/CopyToAsync) already handed out — and thereby consumed — the
    // underlying content stream, which cannot be re-read from the start. An accessor that has to re-materialize
    // the whole body (ReadAsByteArray/ReadAsString/CopyTo) — or decode it from the start
    // (ReadDecompressedStream) — must then fail with the same InvalidOperationException
    // the untagged HttpContent path raises once its stream is consumed, rather than caching/copying only the
    // bytes left after a partial drain — a truncated body the caller cannot tell apart from a complete one.
    // A buffering accessor that ran to completion first leaves bufferedContent set and is served from that.
    private void ThrowIfContentAlreadyConsumed()
    {
        if (contentConsumed && bufferedContent == null)
            throw new InvalidOperationException("The stream was already consumed. It cannot be read again.");
    }

    /// <summary>
    /// Reads the response content as a stream, transparently decoding it when the response is
    /// transport-compressed — unlike <see cref="ReadAsStreamAsync"/>, which is a verbatim pass-through of
    /// the raw bytes. The codec is taken from the response's <c>Content-Encoding</c>: absent or
    /// <c>identity</c> returns the raw stream unchanged, and <c>lz4</c>, <c>gzip</c>, <c>deflate</c> and
    /// <c>br</c> are decoded.
    /// </summary>
    /// <returns>A task that resolves to a plaintext stream over the response content.</returns>
    /// <exception cref="ClickHouseServerException">
    /// The server reported an in-band mid-stream exception, found once the end of the decoded body is
    /// reached. Because this member yields plaintext it detects the block whether or not the body was
    /// compressed — unlike the verbatim members, which can only see it in an uncompressed body.
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// The response uses a codec this client cannot decode (e.g. <c>zstd</c>); the message names it.
    /// </exception>
    /// <remarks>
    /// Disposing this <see cref="ClickHouseRawResult"/> is always sufficient — it releases the response and
    /// any decoder inserted here. Disposing the returned stream is safe too, but note that with nothing to
    /// decode and no exception scanner engaged it <i>is</i> the content stream, so that ends the response
    /// body; the decoder and the scanner both leave what is below them open. Repeated sequential calls
    /// return the same stream rather than stacking a decoder over a partly-consumed body; not safe for
    /// concurrent use.
    /// </remarks>
    public async Task<Stream> ReadDecompressedStreamAsync()
    {
        if (decompressedStream != null)
            return decompressedStream;

        Stream source;
        if (bufferedContent != null)
        {
            // A buffering accessor already materialized the body — still encoded, since those accessors
            // hand the wire bytes over verbatim — and drained the content stream doing so. Decode over that
            // buffer, for the same reason WrapContentStreamAsync serves the streaming accessors from it.
            source = new MemoryStream(bufferedContent, writable: false);
        }
        else
        {
            // Nothing buffered and the content stream already consumed: a decode would start from wherever
            // the previous reader stopped and produce garbage or a truncated body, so fail exactly as the
            // re-materializing accessors do.
            ThrowIfContentAlreadyConsumed();
            source = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
        }

        // Throws for a codec we cannot decode, before anything has been read — so the body is left intact
        // for a caller who wants to decode it themselves, through any read member. Nothing leaks either:
        // the content stream is owned by `response` and released by Dispose(). The consumed flag is set
        // only once past this point, for the same reason: a call that threw here handed nothing out, so it
        // must not lock the other members out of a body that is still whole.
        var wrapped = ResponseDecompression.Wrap(source, response, leaveOpen: true);
        contentConsumed = true;

        // Only a decoder we inserted is ours to dispose; the content stream belongs to the response.
        if (!ReferenceEquals(wrapped, source))
            ownedDecoder = wrapped;

        // The exception-tag scanner sits ABOVE the decoder, exactly as it does on the reader's read path:
        // the server writes its in-band exception block into the response body, so the marker exists only
        // in the decoded plaintext — a scan of the compressed bytes would never match it. This is the
        // member that yields plaintext, so it is the one that can surface a mid-stream failure on a
        // transport-compressed response; the verbatim accessors above can only do so when the body is not
        // compressed. leaveOpen: true — the stream below belongs to the response or to ownedDecoder.
        if (!string.IsNullOrEmpty(exceptionTag))
            wrapped = new ExceptionTagAwareStream(wrapped, exceptionTag, leaveOpen: true, throwAtEndOfStream: true);

        return decompressedStream = wrapped;
    }

    /// <summary>
    /// Reads the response content as a byte array.
    /// </summary>
    /// <returns>A task that resolves to the response content as bytes.</returns>
    /// <remarks>
    /// Throws a <see cref="ClickHouseServerException"/> if the server reports an in-band mid-stream
    /// exception in a plaintext body; no truncated body is returned. A transport-compressed body hides the
    /// marker from this verbatim read — see the remarks on the type. The body is buffered, so repeat calls
    /// return the same bytes.
    /// </remarks>
    public Task<byte[]> ReadAsByteArrayAsync() =>
        string.IsNullOrEmpty(exceptionTag) ? response.Content.ReadAsByteArrayAsync() : ReadAllBytesAsync();

    private async Task<byte[]> ReadAllBytesAsync()
    {
        if (bufferedContent != null)
            return bufferedContent;

        ThrowIfContentAlreadyConsumed();

        // The wrapper is deliberately not disposed: doing so closes the response's content stream, which
        // HttpContent caches and hands back on the next call. Buffering the body here instead preserves
        // the repeat-read behaviour of HttpContent.ReadAsByteArrayAsync/ReadAsStringAsync that callers
        // get on the untagged path. The stream is released when this instance disposes the response.
        var stream = await WrapContentStreamAsync().ConfigureAwait(false);
        using var memory = new MemoryStream();
        await stream.CopyToAsync(memory).ConfigureAwait(false);
        return bufferedContent = memory.ToArray();
    }

    /// <summary>
    /// Reads the response content as a string.
    /// </summary>
    /// <returns>A task that resolves to the response content as a string.</returns>
    /// <remarks>
    /// Throws a <see cref="ClickHouseServerException"/> if the server reports an in-band mid-stream
    /// exception in a plaintext body; no truncated body is returned. A transport-compressed body hides the
    /// marker from this verbatim read — see the remarks on the type. The body is buffered, so repeat calls
    /// return the same string.
    /// </remarks>
    public Task<string> ReadAsStringAsync() =>
        string.IsNullOrEmpty(exceptionTag) ? response.Content.ReadAsStringAsync() : ReadAllStringAsync();

    private async Task<string> ReadAllStringAsync()
    {
        var bytes = await ReadAllBytesAsync().ConfigureAwait(false);

        // Decode the buffered body exactly as HttpContent.ReadAsStringAsync would — honouring the
        // response Content-Type charset and stripping a BOM — so a successful tagged response returns
        // the same string the untagged path returns for identical bytes.
        using var content = new ByteArrayContent(bytes);
        var contentType = response.Content.Headers.ContentType;
        if (contentType != null)
            content.Headers.ContentType = new MediaTypeHeaderValue(contentType.MediaType) { CharSet = contentType.CharSet };
        return await content.ReadAsStringAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Copies the response content to the specified stream.
    /// </summary>
    /// <param name="stream">The destination stream to copy the content to.</param>
    /// <returns>A task that completes when the copy operation is finished.</returns>
    /// <remarks>
    /// Throws a <see cref="ClickHouseServerException"/> if the server reports an in-band mid-stream
    /// exception in a plaintext body (a transport-compressed body hides the marker from this verbatim copy —
    /// see the remarks on the type). The copy is streamed, so the destination may already hold the partial result — including
    /// the server's raw in-band exception block — by the time the exception is raised; treat a destination
    /// written by a failed copy as incomplete.
    /// </remarks>
    public Task CopyToAsync(Stream stream) =>
        string.IsNullOrEmpty(exceptionTag) ? response.Content.CopyToAsync(stream) : CopyViaWrapperAsync(stream);

    private async Task CopyViaWrapperAsync(Stream destination)
    {
        ThrowIfContentAlreadyConsumed();

        // Not disposed, for the same reason as ReadAllBytesAsync: HttpContent.CopyToAsync leaves the
        // content stream open too, so closing it here would break any subsequent read of this result.
        var source = await WrapContentStreamAsync().ConfigureAwait(false);
        await source.CopyToAsync(destination).ConfigureAwait(false);
    }

    public void Dispose()
    {
        // Decoder first (it reads from the content stream), then the response that owns that stream.
        // Nulled so a second Dispose() does not release a decoder's pooled buffers twice.
        var decoder = ownedDecoder;
        ownedDecoder = null;
        decompressedStream = null;
        decoder?.Dispose();

        response?.Dispose();
        GC.SuppressFinalize(this);
    }
}
