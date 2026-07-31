using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Http;

namespace ClickHouse.Driver.ADO;

/// <summary>
/// Represents the raw HTTP response from a ClickHouse query executed with a custom FORMAT clause.
/// </summary>
/// <remarks>
/// This class provides direct access to the response content without any parsing,
/// allowing you to handle custom output formats (e.g., FORMAT JSON, FORMAT CSV) yourself.
/// </remarks>
public class ClickHouseRawResult : IDisposable
{
    private readonly HttpResponseMessage response;
    private readonly IClickHouseCompressor responseCompressor;

    /// <summary>
    /// The decoder <see cref="ReadDecompressedStreamAsync"/> inserted over the content stream, if any.
    /// Kept so (a) repeated calls hand back the same decoder rather than stacking a second one over an
    /// already-partly-consumed body, and (b) <see cref="Dispose"/> releases it — a decoder holds pooled
    /// buffers (the LZ4 one rents from <c>ArrayPool&lt;byte&gt;.Shared</c> and returns them only on
    /// disposal, with no finalizer to fall back on).
    /// </summary>
    private Stream decompressedStream;

    internal ClickHouseRawResult(HttpResponseMessage response)
        : this(response, responseCompressor: null)
    {
    }

    internal ClickHouseRawResult(HttpResponseMessage response, IClickHouseCompressor responseCompressor)
    {
        this.response = response;
        this.responseCompressor = responseCompressor;
    }

    /// <summary>
    /// HTTP <c>Content-Encoding</c> the response body is encoded with (e.g. <c>"zstd"</c>,
    /// <c>"gzip"</c>), or <see langword="null"/> when the body is not transport-compressed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When the underlying <see cref="HttpClient"/> has <c>AutomaticDecompression</c> enabled
    /// for the negotiated algorithm (e.g. gzip/deflate by default), the framework strips
    /// <c>Content-Encoding</c> after decompressing, so this property will be <see langword="null"/>
    /// even though compression was used on the wire.
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
    public Task<Stream> ReadAsStreamAsync() => response.Content.ReadAsStreamAsync();

    /// <summary>
    /// Reads the response content as a stream, transparently decoding it when the response is
    /// transport-compressed. Unlike <see cref="ReadAsStreamAsync"/> — which is a verbatim pass-through of
    /// the raw bytes — this applies the driver's response-decompression rules, driven by the response's
    /// <c>Content-Encoding</c> header:
    /// <list type="bullet">
    /// <item>no encoding (or <c>identity</c>): the raw content stream is returned unchanged;</item>
    /// <item>the codec of the configured response compressor (see
    /// <see cref="ClickHouseClientSettings.ResponseCompressor"/> / <see cref="QueryOptions.ResponseCompressor"/>,
    /// e.g. <c>lz4</c>): decoded by that compressor;</item>
    /// <item><c>gzip</c>, <c>deflate</c>, <c>br</c>: decoded with the BCL codec.</item>
    /// </list>
    /// </summary>
    /// <returns>A task that resolves to a plaintext stream over the response content.</returns>
    /// <exception cref="NotSupportedException">
    /// The response is encoded with a codec this client cannot decode (e.g. <c>zstd</c>); the message names
    /// the codec and how to configure it.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Disposing this <see cref="ClickHouseRawResult"/> is always sufficient: it releases both the
    /// response and any decoder this method inserted. Disposing the returned stream yourself is also
    /// safe (and is what the <c>await using</c> examples do), with one difference between the two cases:
    /// <list type="bullet">
    /// <item>when a decoder is added it is created with <c>leaveOpen</c>, so disposing the returned
    /// stream releases the decoder but leaves the underlying HTTP content stream open;</item>
    /// <item>when the response is <b>not</b> transport-compressed the raw HTTP content stream itself is
    /// returned (reference-equal to <see cref="ReadAsStreamAsync"/>'s result), so disposing it
    /// <i>does</i> dispose the content stream and ends the response body — dispose only this
    /// <see cref="ClickHouseRawResult"/> if you still need the other members afterwards.</item>
    /// </list>
    /// </para>
    /// <para>
    /// Repeated (sequential) calls return the same stream, rather than stacking a second decoder over a
    /// body the first one has already partly consumed. Like the rest of this type, the method is not
    /// safe to call concurrently from several threads.
    /// </para>
    /// </remarks>
    public async Task<Stream> ReadDecompressedStreamAsync()
    {
        if (decompressedStream != null)
            return decompressedStream;

        var rawStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

        // Throws for a codec we cannot decode. Nothing is leaked in that case: rawStream is owned by
        // `response` and released by Dispose(), and leaving it open is what lets a caller recover by
        // reading the still-compressed body and decoding it themselves. That recovery has to go through
        // ReadAsStreamAsync() — which hands back this very same stream — because a raw result is fetched
        // with HttpCompletionOption.ResponseHeadersRead, so the content is unbuffered and
        // ReadAsByteArrayAsync()/ReadAsStringAsync() cannot re-read a body that has been consumed.
        var wrapped = ResponseDecompression.Wrap(rawStream, response, responseCompressor, leaveOpen: true);

        // Only a decoder we inserted is ours to dispose; the content stream belongs to the response.
        if (!ReferenceEquals(wrapped, rawStream))
            decompressedStream = wrapped;

        return wrapped;
    }

    /// <summary>
    /// Reads the response content as a byte array.
    /// </summary>
    /// <returns>A task that resolves to the response content as bytes.</returns>
    public Task<byte[]> ReadAsByteArrayAsync() => response.Content.ReadAsByteArrayAsync();

    /// <summary>
    /// Reads the response content as a string.
    /// </summary>
    /// <returns>A task that resolves to the response content as a string.</returns>
    public Task<string> ReadAsStringAsync() => response.Content.ReadAsStringAsync();

    /// <summary>
    /// Copies the response content to the specified stream.
    /// </summary>
    /// <param name="stream">The destination stream to copy the content to.</param>
    /// <returns>A task that completes when the copy operation is finished.</returns>
    public Task CopyToAsync(Stream stream) => response.Content.CopyToAsync(stream);

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
