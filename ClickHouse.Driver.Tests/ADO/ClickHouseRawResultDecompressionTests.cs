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
        using var response = CreateResponse(new byte[] { 0x28, 0xB5, 0x2F, 0xFD }, "zstd");
        using var raw = new ClickHouseRawResult(response);

        var ex = Assert.ThrowsAsync<NotSupportedException>(() => raw.ReadDecompressedStreamAsync());

        Assert.That(ex.Message, Does.Contain("zstd"));
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
        using var response = CreateResponse(compressed, "zstd");
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
        using var response = CreateStreamedResponse(compressed, "zstd");
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
