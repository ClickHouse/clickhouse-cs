using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
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
    public Task<Stream> ReadAsStreamAsync() => ReadAsStreamAsync(CancellationToken.None);

    /// <summary>
    /// Reads the response content as a stream.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that resolves to the response content stream.</returns>
    public Task<Stream> ReadAsStreamAsync(CancellationToken cancellationToken) => response.Content.ReadAsStreamAsync(cancellationToken);

    /// <summary>
    /// Reads the response content as a stream, transparently decoding it when the response is
    /// transport-compressed — unlike <see cref="ReadAsStreamAsync"/>, which is a verbatim pass-through of
    /// the raw bytes. The codec is taken from the response's <c>Content-Encoding</c>: absent or
    /// <c>identity</c> returns the raw stream unchanged, and <c>lz4</c>, <c>zstd</c>, <c>gzip</c>,
    /// <c>deflate</c> and <c>br</c> are decoded.
    /// </summary>
    /// <returns>A task that resolves to a plaintext stream over the response content.</returns>
    /// <exception cref="NotSupportedException">
    /// The response uses a codec this client cannot decode (e.g. <c>snappy</c>); the message names it.
    /// </exception>
    /// <remarks>
    /// Disposing this <see cref="ClickHouseRawResult"/> is always sufficient — it releases the response and
    /// any decoder inserted here. Disposing the returned stream is safe too, but note that with nothing to
    /// decode it <i>is</i> the content stream, so that ends the response body. Repeated sequential calls
    /// return the same stream rather than stacking a decoder over a partly-consumed body; not safe for
    /// concurrent use.
    /// </remarks>
    public Task<Stream> ReadDecompressedStreamAsync() => ReadDecompressedStreamAsync(CancellationToken.None);

    /// <summary>
    /// Reads the response content as a stream, transparently decoding it when the response is
    /// transport-compressed — unlike <see cref="ReadAsStreamAsync"/>, which is a verbatim pass-through of
    /// the raw bytes. The codec is taken from the response's <c>Content-Encoding</c>: absent or
    /// <c>identity</c> returns the raw stream unchanged, and <c>lz4</c>, <c>zstd</c>, <c>gzip</c>,
    /// <c>deflate</c> and <c>br</c> are decoded.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that resolves to a plaintext stream over the response content.</returns>
    /// <exception cref="NotSupportedException">
    /// The response uses a codec this client cannot decode (e.g. <c>snappy</c>); the message names it.
    /// </exception>
    /// <remarks>
    /// Disposing this <see cref="ClickHouseRawResult"/> is always sufficient — it releases the response and
    /// any decoder inserted here. Disposing the returned stream is safe too, but note that with nothing to
    /// decode it <i>is</i> the content stream, so that ends the response body. Repeated sequential calls
    /// return the same stream rather than stacking a decoder over a partly-consumed body; not safe for
    /// concurrent use.
    /// </remarks>
    public async Task<Stream> ReadDecompressedStreamAsync(CancellationToken cancellationToken)
    {
        if (decompressedStream != null)
            return decompressedStream;

        var rawStream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);

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
    public Task<byte[]> ReadAsByteArrayAsync() => ReadAsByteArrayAsync(CancellationToken.None);

    /// <summary>
    /// Reads the response content as a byte array.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that resolves to the response content as bytes.</returns>
    public Task<byte[]> ReadAsByteArrayAsync(CancellationToken cancellationToken) => response.Content.ReadAsByteArrayAsync(cancellationToken);

    /// <summary>
    /// Reads the response content as a string.
    /// </summary>
    /// <returns>A task that resolves to the response content as a string.</returns>
    public Task<string> ReadAsStringAsync() => ReadAsStringAsync(CancellationToken.None);

    /// <summary>
    /// Reads the response content as a string.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that resolves to the response content as a string.</returns>
    public Task<string> ReadAsStringAsync(CancellationToken cancellationToken) => response.Content.ReadAsStringAsync(cancellationToken);

    /// <summary>
    /// Copies the response content to the specified stream.
    /// </summary>
    /// <param name="stream">The destination stream to copy the content to.</param>
    /// <returns>A task that completes when the copy operation is finished.</returns>
    public Task CopyToAsync(Stream stream) => CopyToAsync(stream, CancellationToken.None);

    /// <summary>
    /// Copies the response content to the specified stream.
    /// </summary>
    /// <param name="stream">The destination stream to copy the content to.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the copy operation is finished.</returns>
    public Task CopyToAsync(Stream stream, CancellationToken cancellationToken) => response.Content.CopyToAsync(stream, cancellationToken);

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
