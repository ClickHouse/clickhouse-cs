using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Compression;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// <see cref="ClickHouseRawResult"/>'s two contracts side by side: the four original members hand back the
/// bytes on the wire verbatim, and <see cref="ClickHouseRawResult.ReadDecompressedStreamAsync"/> decodes.
/// A raw request does not advertise the driver's default codecs, so a compressed raw body only happens
/// when the caller asked for one — see <c>AcceptEncodingTests</c> for that negotiation.
/// </summary>
[TestFixture]
public class ClickHouseRawResultDecompressionTests
{
    private static readonly byte[] Plaintext = Encoding.UTF8.GetBytes("0\n1\n2\n");

    private static byte[] Lz4Encoded()
    {
        using var buffer = new MemoryStream();
        using (var encoder = Lz4Compressor.Default.Compress(buffer, leaveOpen: true))
        {
            encoder.Write(Plaintext, 0, Plaintext.Length);
        }

        return buffer.ToArray();
    }

    private static byte[] ZstdEncoded()
    {
        using var buffer = new MemoryStream();
        using (var encoder = ZstdCompressor.Default.Compress(buffer, leaveOpen: true))
        {
            encoder.Write(Plaintext, 0, Plaintext.Length);
        }

        return buffer.ToArray();
    }

    /// <summary>The zlib form of <c>deflate</c>, which is what ClickHouse emits.</summary>
    private static byte[] ZLibEncoded()
    {
        using var buffer = new MemoryStream();
        using (var encoder = new ZLibStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
        {
            encoder.Write(Plaintext, 0, Plaintext.Length);
        }

        return buffer.ToArray();
    }

    private static HttpResponseMessage CreateResponse(byte[] body, string contentEncoding)
    {
        var content = new ByteArrayContent(body);
        if (contentEncoding != null)
            content.Headers.ContentEncoding.Add(contentEncoding);

        return new HttpResponseMessage(HttpStatusCode.OK) { Content = content };
    }

    /// <summary>
    /// A response whose content is a live stream rather than a buffered array, which is what
    /// <see cref="HttpCompletionOption.ResponseHeadersRead"/> actually produces. It matters:
    /// <see cref="ByteArrayContent"/> hands out a rewindable stream, so a body read twice through it
    /// succeeds and hides anything that depends on the body being single-pass.
    /// </summary>
    private static HttpResponseMessage CreateStreamedResponse(byte[] body, string contentEncoding)
    {
        var content = new StreamContent(new MemoryStream(body));
        if (contentEncoding != null)
            content.Headers.ContentEncoding.Add(contentEncoding);

        return new HttpResponseMessage(HttpStatusCode.OK) { Content = content };
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_WithLz4Response_ReturnsThePlaintext()
    {
        using var response = CreateResponse(Lz4Encoded(), "lz4");
        using var raw = new ClickHouseRawResult(response);

        using var decompressed = await raw.ReadDecompressedStreamAsync();
        using var buffer = new MemoryStream();
        await decompressed.CopyToAsync(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(Plaintext));
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_WithUnencodedResponse_ReturnsTheRawContentStream()
    {
        using var response = CreateResponse(Plaintext, contentEncoding: null);
        using var raw = new ClickHouseRawResult(response);

        var contentStream = await raw.ReadAsStreamAsync();
        var decompressed = await raw.ReadDecompressedStreamAsync();

        Assert.That(decompressed, Is.SameAs(contentStream), "nothing to decode, so nothing to wrap");
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_CalledTwice_ReturnsTheSameDecoder()
    {
        using var response = CreateResponse(Lz4Encoded(), "lz4");
        using var raw = new ClickHouseRawResult(response);

        var first = await raw.ReadDecompressedStreamAsync();
        var second = await raw.ReadDecompressedStreamAsync();

        Assert.That(second, Is.SameAs(first), "a second decoder would sit over a partly-consumed body");
    }

    [Test]
    public void ReadDecompressedStreamAsync_WithUnsupportedCodec_ThrowsNamingTheCodec()
    {
        // snappy, not zstd: zstd became decodable when the vendored ZstdSharp codec landed.
        using var response = CreateResponse(new byte[] { 0xFF, 0x06, 0x00, 0x00 }, "snappy");
        using var raw = new ClickHouseRawResult(response);

        var ex = Assert.ThrowsAsync<NotSupportedException>(() => raw.ReadDecompressedStreamAsync());

        Assert.That(ex.Message, Does.Contain("snappy"));
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_WithZstdResponse_ReturnsThePlaintext()
    {
        using var response = CreateResponse(ZstdEncoded(), "zstd");
        using var raw = new ClickHouseRawResult(response);

        using var decompressed = await raw.ReadDecompressedStreamAsync();
        using var buffer = new MemoryStream();
        await decompressed.CopyToAsync(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(Plaintext));
    }

    /// <summary>
    /// The verbatim contract holds for zstd too: the four original members hand back the frame on the
    /// wire, which is what makes a compressed export usable as a file.
    /// </summary>
    [Test]
    public async Task ReadAsByteArrayAsync_WithZstdResponse_ReturnsTheCompressedBytesVerbatim()
    {
        var compressed = ZstdEncoded();
        using var response = CreateResponse(compressed, "zstd");
        using var raw = new ClickHouseRawResult(response);

        Assert.That(await raw.ReadAsByteArrayAsync(), Is.EqualTo(compressed));
    }

    /// <summary>
    /// The throwing case must leave the body readable, so a caller can fall back to decoding it
    /// themselves. It has to be <see cref="ClickHouseRawResult.ReadAsStreamAsync"/> rather than
    /// <c>ReadAsByteArrayAsync</c>, because a raw result is fetched with
    /// <see cref="HttpCompletionOption.ResponseHeadersRead"/> and the content is not buffered.
    /// </summary>
    [Test]
    public async Task ReadDecompressedStreamAsync_WithUnsupportedCodec_LeavesTheBodyReadableForManualDecoding()
    {
        var compressed = Lz4Encoded();
        using var response = CreateResponse(compressed, "snappy");
        using var raw = new ClickHouseRawResult(response);

        Assert.ThrowsAsync<NotSupportedException>(() => raw.ReadDecompressedStreamAsync());

        using var buffer = new MemoryStream();
        using (var stream = await raw.ReadAsStreamAsync())
        {
            await stream.CopyToAsync(buffer);
        }

        Assert.That(buffer.ToArray(), Is.EqualTo(compressed), "the undecodable body must still be there");
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_WithUnsupportedCodec_LeavesTheBodyReadableThroughEveryMember()
    {
        var compressed = Lz4Encoded();
        using var response = CreateStreamedResponse(compressed, "snappy");
        using var raw = new ClickHouseRawResult(response);

        Assert.ThrowsAsync<NotSupportedException>(() => raw.ReadDecompressedStreamAsync());

        // Nothing was read before the throw, so the buffering members see the whole body too.
        Assert.That(await raw.ReadAsByteArrayAsync(), Is.EqualTo(compressed));
    }

    /// <summary>
    /// Contrast case: the four original members are verbatim pass-throughs and must stay that way —
    /// <c>examples/Select/Select_005_CompressedRawExport.cs</c> writes the compressed bytes to a file.
    /// </summary>
    [Test]
    public async Task TheOriginalRawResultMembers_WithCompressedResponse_ReturnTheRawBytesVerbatim()
    {
        var compressed = Lz4Encoded();

        using (var response = CreateResponse(compressed, "lz4"))
        using (var raw = new ClickHouseRawResult(response))
        {
            Assert.That(await raw.ReadAsByteArrayAsync(), Is.EqualTo(compressed), "ReadAsByteArrayAsync must not decode");
        }

        using (var response = CreateResponse(compressed, "lz4"))
        using (var raw = new ClickHouseRawResult(response))
        {
            using var buffer = new MemoryStream();
            using (var stream = await raw.ReadAsStreamAsync())
            {
                await stream.CopyToAsync(buffer);
            }

            Assert.That(buffer.ToArray(), Is.EqualTo(compressed), "ReadAsStreamAsync must not decode");
        }

        using (var response = CreateResponse(compressed, "lz4"))
        using (var raw = new ClickHouseRawResult(response))
        {
            using var buffer = new MemoryStream();
            await raw.CopyToAsync(buffer);

            Assert.That(buffer.ToArray(), Is.EqualTo(compressed), "CopyToAsync must not decode");
        }

        using (var response = CreateResponse(compressed, "lz4"))
        using (var raw = new ClickHouseRawResult(response))
        {
            var asString = await raw.ReadAsStringAsync();

            // HttpContent decodes the body as UTF-8, and compressed bytes are not valid UTF-8, so the
            // exact expectation is "the UTF-8 reading of the raw bytes" — replacement characters and all.
            // Pinning that rather than merely `Is.Not.EqualTo(plaintext)` makes the assertion fail if the
            // body were decoded, instead of passing for almost any behaviour.
            Assert.That(asString, Is.EqualTo(Encoding.UTF8.GetString(compressed)), "ReadAsStringAsync must not decode");
            Assert.That(asString, Does.Contain("\uFFFD"), "sanity: raw lz4 bytes are not valid UTF-8");
        }
    }

    /// <summary>
    /// An unbuffered body is single-pass, so a verbatim member and the decoding one draw on the same
    /// unrewindable stream and whichever runs second continues where the first stopped. Pinned as the
    /// documented contract of this type rather than as a defect of the decoding member: the four original
    /// members already do this to each other, and it is inherent to
    /// <see cref="HttpCompletionOption.ResponseHeadersRead"/>. Truncation is silent, which is exactly why
    /// the class remarks tell callers to pick one member and stick to it.
    /// </summary>
    /// <summary>
    /// The plausible way to get this wrong: read part of the body verbatim, then ask for it decoded. The
    /// decoder starts mid-frame and fails — loudly, which is the good case. Pinned so the class remarks
    /// stay true, and to record that it is not silent.
    /// </summary>
    [Test]
    public async Task ReadDecompressedStreamAsync_AfterAPartialVerbatimRead_FailsLoudly()
    {
        using var response = CreateStreamedResponse(Lz4Encoded(), "lz4");
        using var raw = new ClickHouseRawResult(response);

        (await raw.ReadAsStreamAsync()).ReadByte();

        var decoder = await raw.ReadDecompressedStreamAsync();
        Assert.Throws<InvalidDataException>(() => decoder.CopyTo(Stream.Null));
    }

    /// <summary>
    /// The buffering members are the exception: they pull the whole body into memory, so a read after one
    /// of them still sees all of it. Recorded because the rule is per-member, not blanket.
    /// </summary>
    [Test]
    public async Task ReadDecompressedStreamAsync_AfterABufferingMember_StillSeesTheWholeBody()
    {
        using var response = CreateStreamedResponse(Lz4Encoded(), "lz4");
        using var raw = new ClickHouseRawResult(response);

        Assert.That(await raw.ReadAsByteArrayAsync(), Is.Not.Empty);

        using var decompressed = await raw.ReadDecompressedStreamAsync();
        using var buffer = new MemoryStream();
        await decompressed.CopyToAsync(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(Plaintext));
    }

    /// <summary>
    /// The counterpart: used on its own against the same unbuffered content, the decoding member works.
    /// Without this, the test above would also pass if decoding were broken outright.
    /// </summary>
    [Test]
    public async Task ReadDecompressedStreamAsync_OverAnUnbufferedBody_ReturnsThePlaintext()
    {
        using var response = CreateStreamedResponse(Lz4Encoded(), "lz4");
        using var raw = new ClickHouseRawResult(response);

        using var decompressed = await raw.ReadDecompressedStreamAsync();
        using var buffer = new MemoryStream();
        await decompressed.CopyToAsync(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(Plaintext));
    }

    /// <summary>
    /// A decoder holds unmanaged or pooled state — the LZ4 one rents from
    /// <c>ArrayPool&lt;byte&gt;.Shared</c> and returns only on disposal, with no finalizer — so the raw
    /// result must release the decoder it inserted, not just the HTTP response. Uses the zlib decoder to
    /// observe it: that one reliably throws once disposed, whereas the LZ4 stream simply stops yielding.
    /// </summary>
    [Test]
    public async Task RawResultDispose_DisposesTheDecoderItInserted()
    {
        using var response = CreateResponse(ZLibEncoded(), "deflate");
        var raw = new ClickHouseRawResult(response);

        var decoder = await raw.ReadDecompressedStreamAsync();
        Assert.That(decoder.ReadByte(), Is.EqualTo(Plaintext[0]), "the decoder must be usable while the result is alive");

        raw.Dispose();

        Assert.Throws<ObjectDisposedException>(() => decoder.ReadByte(), "the raw result owns the decoder it created");
    }

    /// <summary>
    /// A response carrying <c>X-ClickHouse-Exception-Tag</c> — sent whenever
    /// <c>http_write_exception_in_output_format</c> is on, i.e. on every response of such a query, failing or
    /// not. It engages the in-band exception scanner, and with it the single-consumption bookkeeping the
    /// verbatim members already do, which the tests below extend to the decoding member.
    /// </summary>
    private static HttpResponseMessage CreateTaggedStreamedResponse(byte[] body, string contentEncoding)
    {
        var response = CreateStreamedResponse(body, contentEncoding);
        response.Headers.Add("X-ClickHouse-Exception-Tag", "PU1FNUFH98");
        return response;
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_OnATaggedResponse_AfterAPartialVerbatimRead_ThrowsLikeConsumedContent()
    {
        // With the scanner engaged, a verbatim read marks the content stream consumed. Decoding it from
        // there would start mid-frame, so this must fail the same way the re-materializing members do —
        // rather than surfacing a decoder-internal error, or worse a short body.
        using var response = CreateTaggedStreamedResponse(Lz4Encoded(), "lz4");
        using var raw = new ClickHouseRawResult(response);

        (await raw.ReadAsStreamAsync()).ReadByte();

        Assert.ThrowsAsync<InvalidOperationException>(() => raw.ReadDecompressedStreamAsync());
    }

    [Test]
    public async Task ReadAsByteArrayAsync_OnATaggedResponse_AfterAPartialDecode_ThrowsRatherThanTruncating()
    {
        // The mirror image: the decoding member consumes the content stream too (and reads ahead), so a
        // member that has to re-materialize the whole body afterwards must fail loudly instead of caching
        // whatever bytes are left.
        using var response = CreateTaggedStreamedResponse(Lz4Encoded(), "lz4");
        using var raw = new ClickHouseRawResult(response);

        Assert.That((await raw.ReadDecompressedStreamAsync()).ReadByte(), Is.EqualTo(Plaintext[0]));

        Assert.ThrowsAsync<InvalidOperationException>(() => raw.ReadAsByteArrayAsync());
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_OnATaggedResponse_AfterABufferingMember_StillSeesTheWholeBody()
    {
        // A buffering member on the tagged path caches the body itself — still encoded, since it hands the
        // wire bytes over verbatim — so the decode must run over that buffer rather than the content stream
        // it drained.
        using var response = CreateTaggedStreamedResponse(Lz4Encoded(), "lz4");
        using var raw = new ClickHouseRawResult(response);

        Assert.That(await raw.ReadAsByteArrayAsync(), Is.EqualTo(Lz4Encoded()));

        using var buffer = new MemoryStream();
        await (await raw.ReadDecompressedStreamAsync()).CopyToAsync(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(Plaintext));
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_OnATaggedResponse_WithUnsupportedCodec_LeavesTheBodyReadable()
    {
        // The undecodable-codec throw hands nothing out, so — exactly as on an untagged response — it must
        // not mark the content consumed and lock the other members out of a body that is still whole.
        // snappy, not zstd: zstd became decodable when the vendored ZstdSharp codec landed.
        var compressed = Lz4Encoded();
        using var response = CreateTaggedStreamedResponse(compressed, "snappy");
        using var raw = new ClickHouseRawResult(response);

        Assert.ThrowsAsync<NotSupportedException>(() => raw.ReadDecompressedStreamAsync());

        Assert.That(await raw.ReadAsByteArrayAsync(), Is.EqualTo(compressed));
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_OnATaggedResponse_CalledTwice_ReturnsTheSameStream()
    {
        // Repeat calls must hand back the same scanner: a fresh one would have observed nothing, so the
        // marker recorded so far — the whole point of the wrapper — would be lost.
        using var response = CreateTaggedStreamedResponse(Lz4Encoded(), "lz4");
        using var raw = new ClickHouseRawResult(response);

        var first = await raw.ReadDecompressedStreamAsync();
        var second = await raw.ReadDecompressedStreamAsync();

        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public async Task RawResultDispose_AfterTheCallerAlreadyDisposedTheDecoder_IsHarmless()
    {
        using var response = CreateResponse(Lz4Encoded(), "lz4");
        var raw = new ClickHouseRawResult(response);

        var decoder = await raw.ReadDecompressedStreamAsync();
        decoder.Dispose();

        Assert.DoesNotThrow(raw.Dispose);
        Assert.DoesNotThrow(raw.Dispose);
    }
}
