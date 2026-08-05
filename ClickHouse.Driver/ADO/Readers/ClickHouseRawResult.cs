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
/// </remarks>
public class ClickHouseRawResult : IDisposable
{
    private readonly HttpResponseMessage response;
    private readonly string exceptionTag;
    private byte[] bufferedContent;
    private bool contentConsumed;

    /// <summary>
    /// The decoder <see cref="ReadDecompressedStreamAsync"/> inserted over the content stream, if any.
    /// Kept so repeated calls hand back the same decoder rather than stacking a second one over an
    /// already-partly-consumed body, and so <see cref="Dispose"/> releases it — a decoder holds pooled
    /// buffers and has no finalizer to fall back on.
    /// </summary>
    private Stream decompressedStream;

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
    /// a partial result — including the server's raw in-band exception block — before the throw.
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
    // the whole body (ReadAsByteArray/ReadAsString/CopyTo) must then fail with the same InvalidOperationException
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
    /// <exception cref="NotSupportedException">
    /// The response uses a codec this client cannot decode (e.g. <c>zstd</c>); the message names it.
    /// </exception>
    /// <remarks>
    /// Disposing this <see cref="ClickHouseRawResult"/> is always sufficient — it releases the response and
    /// any decoder inserted here. Disposing the returned stream is safe too, but note that with nothing to
    /// decode it <i>is</i> the content stream, so that ends the response body. Repeated sequential calls
    /// return the same stream rather than stacking a decoder over a partly-consumed body; not safe for
    /// concurrent use.
    /// </remarks>
    public async Task<Stream> ReadDecompressedStreamAsync()
    {
        if (decompressedStream != null)
            return decompressedStream;

        var rawStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

        // Throws for a codec we cannot decode, before anything has been read — so the body is left intact
        // for a caller who wants to decode it themselves, through any read member. Nothing leaks either:
        // rawStream is owned by `response` and released by Dispose().
        var wrapped = ResponseDecompression.Wrap(rawStream, response, leaveOpen: true);

        // Only a decoder we inserted is ours to dispose; the content stream belongs to the response.
        if (!ReferenceEquals(wrapped, rawStream))
            decompressedStream = wrapped;

        return wrapped;
    }

    /// <summary>
    /// Reads the response content as a byte array.
    /// </summary>
    /// <returns>A task that resolves to the response content as bytes.</returns>
    /// <remarks>
    /// Throws a <see cref="ClickHouseServerException"/> if the server reports an in-band mid-stream
    /// exception; no truncated body is returned. The body is buffered, so repeat calls return the same bytes.
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
    /// exception; no truncated body is returned. The body is buffered, so repeat calls return the same string.
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
    /// exception. The copy is streamed, so the destination may already hold the partial result — including
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
        var decoder = decompressedStream;
        decompressedStream = null;
        decoder?.Dispose();

        response?.Dispose();
        GC.SuppressFinalize(this);
    }
}
