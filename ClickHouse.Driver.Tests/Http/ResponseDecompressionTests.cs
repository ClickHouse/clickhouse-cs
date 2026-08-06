using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Http;
using ClickHouse.Driver.Tests.Utilities;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Http;

/// <summary>
/// Response-body decompression, driven by the response's <c>Content-Encoding</c> header. These cases use
/// a stub <see cref="HttpMessageHandler"/> (<see cref="TrackingHandler"/>) so the response's headers and
/// bytes can be dictated exactly, including codecs and malformed bodies a real server would never send.
/// The end-to-end proof that the driver decodes what a real server actually sends lives in
/// <c>ConnectionTests.ExecuteReaderAsync_WithAnHttpClientThatCannotDecodeTheCodec_DecodesItInTheDriver</c> and
/// <see cref="ResponseDecompressionIntegrationTests"/>.
/// </summary>
[TestFixture]
public class ResponseDecompressionTests
{
    /// <summary>
    /// A single-column <c>Int32</c> RowBinaryWithNamesAndTypes payload holding the given values —
    /// the wire format the driver's reader expects.
    /// </summary>
    private static byte[] BuildRowBinaryResult(params int[] values)
    {
        using var ms = new MemoryStream();
        using (var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write((byte)1);              // column count
            writer.Write((byte)1);              // name length
            writer.Write((byte)'n');            // name
            var type = Encoding.UTF8.GetBytes("Int32");
            writer.Write((byte)type.Length);
            writer.Write(type);
            foreach (var value in values)
                writer.Write(value);
        }

        return ms.ToArray();
    }

    private static byte[] Encode(string contentEncoding, byte[] plaintext)
    {
        using var buffer = new MemoryStream();
        using (var encoder = CreateEncoder(buffer, contentEncoding))
        {
            encoder.Write(plaintext, 0, plaintext.Length);
        }

        return buffer.ToArray();
    }

    private static Stream CreateEncoder(Stream destination, string contentEncoding) => contentEncoding switch
    {
        "gzip" => new GZipStream(destination, CompressionLevel.Fastest, leaveOpen: true),

        // Raw DEFLATE (RFC 1951). ClickHouse sends the zlib form instead; both are covered, see
        // Wrap_WithZLibWrappedDeflate_DecodesIt.
        "deflate" => new DeflateStream(destination, CompressionLevel.Fastest, leaveOpen: true),
        "br" or "brotli" => new BrotliStream(destination, CompressionLevel.Fastest, leaveOpen: true),
        "lz4" => Lz4Compressor.Default.Compress(destination, leaveOpen: true),
        "zstd" => ZstdCompressor.Default.Compress(destination, leaveOpen: true),
        _ => throw new ArgumentOutOfRangeException(nameof(contentEncoding), contentEncoding, null),
    };

    private static HttpResponseMessage CreateResponse(byte[] body, string contentEncoding, HttpStatusCode status = HttpStatusCode.OK)
    {
        var content = new ByteArrayContent(body);
        if (contentEncoding != null)
            content.Headers.ContentEncoding.Add(contentEncoding);

        return new HttpResponseMessage(status) { Content = content };
    }

    private static ClickHouseClient CreateClient(HttpResponseMessage response)
    {
        var httpClient = new HttpClient(new TrackingHandler(response));
        return new ClickHouseClient(new ClickHouseClientSettings { HttpClient = httpClient });
    }

    /// <summary>Every codec the driver decodes, with the token a server may answer with.</summary>
    private static IEnumerable<TestCaseData> DecodableCodecs()
    {
        yield return new TestCaseData("lz4").SetName("{m}(lz4)");
        yield return new TestCaseData("zstd").SetName("{m}(zstd)");
        yield return new TestCaseData("gzip").SetName("{m}(gzip)");
        yield return new TestCaseData("deflate").SetName("{m}(deflate)");
        yield return new TestCaseData("br").SetName("{m}(br)");
        yield return new TestCaseData("brotli").SetName("{m}(brotli alias)");
    }

    // ---------------------------------------------------------------------------------------------
    // The resolver itself: the resolution table, verified directly.
    // ---------------------------------------------------------------------------------------------

    [TestCase(null, TestName = "{m}(absent)")]
    [TestCase("", TestName = "{m}(empty)")]
    [TestCase("identity", TestName = "{m}(identity)")]
    [TestCase("  IDENTITY  ", TestName = "{m}(identity padded and upper-cased)")]
    public void Wrap_WithNoEffectiveContentEncoding_ReturnsTheSourceStreamInstance(string contentEncoding)
    {
        using var source = new MemoryStream(new byte[] { 1, 2, 3 });

        var wrapped = ResponseDecompression.Wrap(source, contentEncoding, leaveOpen: true);

        Assert.That(wrapped, Is.SameAs(source), "an unencoded body must not be wrapped at all");
    }

    [TestCaseSource(nameof(DecodableCodecs))]
    public void Wrap_WithADecodableCodec_YieldsThePlaintext(string contentEncoding)
    {
        var plaintext = Encoding.UTF8.GetBytes("the quick brown fox jumps over the lazy dog");
        var encoded = Encode(contentEncoding == "brotli" ? "br" : contentEncoding, plaintext);
        using var source = new MemoryStream(encoded);

        using var wrapped = ResponseDecompression.Wrap(source, contentEncoding, leaveOpen: true);
        using var decoded = new MemoryStream();
        wrapped.CopyTo(decoded);

        Assert.Multiple(() =>
        {
            Assert.That(wrapped, Is.Not.SameAs(source), "a compressed body must be wrapped");
            Assert.That(decoded.ToArray(), Is.EqualTo(plaintext));
        });
    }

    [TestCase("LZ4", TestName = "{m}(upper-cased)")]
    [TestCase("  lz4 ", TestName = "{m}(surrounded by whitespace)")]
    public void Wrap_WithATokenThatNeedsNormalizing_StillMatchesTheCodec(string contentEncoding)
    {
        var plaintext = Encoding.UTF8.GetBytes("normalize me");
        using var source = new MemoryStream(Encode("lz4", plaintext));

        using var wrapped = ResponseDecompression.Wrap(source, contentEncoding, leaveOpen: true);
        using var decoded = new MemoryStream();
        wrapped.CopyTo(decoded);

        Assert.That(decoded.ToArray(), Is.EqualTo(plaintext));
    }

    /// <summary>
    /// HTTP's <c>deflate</c> is the zlib format (RFC 1950), which is what ClickHouse emits — its bodies
    /// start <c>78 5E</c>. A bare <see cref="DeflateStream"/> cannot read that, so the resolver uses a
    /// stream that accepts both forms; the raw-DEFLATE half is covered by
    /// <see cref="Wrap_WithADecodableCodec_YieldsThePlaintext"/>.
    /// </summary>
    [Test]
    public void Wrap_WithZLibWrappedDeflate_DecodesIt()
    {
        var plaintext = Encoding.UTF8.GetBytes("zlib-wrapped, as ClickHouse sends it");
        using var buffer = new MemoryStream();
        using (var encoder = new ZLibStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
        {
            encoder.Write(plaintext, 0, plaintext.Length);
        }

        var encoded = buffer.ToArray();
        using var source = new MemoryStream(encoded);
        using var wrapped = ResponseDecompression.Wrap(source, "deflate", leaveOpen: true);
        using var decoded = new MemoryStream();
        wrapped.CopyTo(decoded);

        Assert.Multiple(() =>
        {
            Assert.That(encoded[0], Is.EqualTo(0x78), "expected a zlib header, not raw DEFLATE");
            Assert.That(decoded.ToArray(), Is.EqualTo(plaintext));
        });
    }

    [Test]
    public void Wrap_WithStackedContentEncodings_ThrowsNamingBoth()
    {
        var body = Encode("gzip", Encoding.UTF8.GetBytes("stacked"));
        using var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(body) };
        response.Content.Headers.ContentEncoding.Add("gzip");
        response.Content.Headers.ContentEncoding.Add("br");
        using var source = new MemoryStream(body);

        var ex = Assert.Throws<NotSupportedException>(
            () => ResponseDecompression.Wrap(source, response, leaveOpen: true));

        Assert.That(ex.Message, Does.Contain("gzip, br"));
    }

    // zstd used to be listed here; it is decodable since the vendored ZstdSharp codec landed, so it
    // moved to DecodableCodecs and `compress` (RFC 9110, which ClickHouse never emits) took its slot.
    [TestCase("compress")]
    [TestCase("snappy")]
    [TestCase("xz")]
    public void Wrap_WithUnsupportedCodec_ThrowsNamingTheCodecAndTheFix(string contentEncoding)
    {
        using var source = new MemoryStream(new byte[] { 1, 2, 3 });

        var ex = Assert.Throws<NotSupportedException>(
            () => ResponseDecompression.Wrap(source, contentEncoding, leaveOpen: true));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain(contentEncoding));
            Assert.That(ex.Message, Does.Contain("lz4"), "the message must name what it can decode");
            Assert.That(ex.Message, Does.Contain("AcceptEncoding"), "and how to change the request");
            Assert.That(ex.Message, Does.Contain("ExecuteRawResultAsync"), "and the escape hatch");
        });
    }

    [Test]
    public void TryWrap_WithUnsupportedCodec_ReturnsFalseInsteadOfThrowing()
    {
        using var source = new MemoryStream(new byte[] { 1, 2, 3 });

        var wrapped = ResponseDecompression.TryWrap(source, "snappy", leaveOpen: true, out var decompressed);

        Assert.Multiple(() =>
        {
            Assert.That(wrapped, Is.False);
            Assert.That(decompressed, Is.Null);
        });
    }

    /// <summary>
    /// The default advertises only codecs the resolver can decode — otherwise the driver would negotiate
    /// its way into its own "unsupported codec" error. <c>br</c> is decodable but deliberately absent, so
    /// that a default request is not answered with brotli (see the remarks on
    /// <c>ResponseDecompression.DefaultAcceptEncoding</c>).
    /// </summary>
    [Test]
    public void DefaultAcceptEncoding_AdvertisesOnlyDecodableCodecsAndNotBrotli()
    {
        var advertised = ResponseDecompression.DefaultAcceptEncoding
            .Split(',')
            .Select(token => token.Trim())
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(advertised, Is.EqualTo(new[] { "zstd", "lz4", "gzip", "deflate" }));
            foreach (var token in advertised)
            {
                using var source = new MemoryStream(Encode(token, Encoding.UTF8.GetBytes("x")));
                Assert.That(
                    ResponseDecompression.TryWrap(source, token, leaveOpen: true, out _),
                    Is.True,
                    $"advertised '{token}' but cannot decode it");
            }
        });
    }

    // ---------------------------------------------------------------------------------------------
    // Decoder ownership. This is the layer where it is observable: the driver picks decoders from a
    // fixed table, so there is no user-supplied codec to instrument higher up.
    // ---------------------------------------------------------------------------------------------

    [TestCaseSource(nameof(DecodableCodecs))]
    public void Wrap_WithLeaveOpenTrue_DoesNotDisposeTheTransportStream(string contentEncoding)
    {
        var encoded = Encode(contentEncoding == "brotli" ? "br" : contentEncoding, Encoding.UTF8.GetBytes("payload"));
        using var source = new DisposeCountingStream(encoded);

        using (var wrapped = ResponseDecompression.Wrap(source, contentEncoding, leaveOpen: true))
        {
            wrapped.CopyTo(Stream.Null);
        }

        Assert.That(source.DisposeCount, Is.Zero, "the HTTP response owns the transport stream");
    }

    [TestCaseSource(nameof(DecodableCodecs))]
    public void Wrap_WithLeaveOpenFalse_DisposesTheTransportStreamExactlyOnce(string contentEncoding)
    {
        var encoded = Encode(contentEncoding == "brotli" ? "br" : contentEncoding, Encoding.UTF8.GetBytes("payload"));
        using var source = new DisposeCountingStream(encoded);

        var wrapped = ResponseDecompression.Wrap(source, contentEncoding, leaveOpen: false);
        wrapped.CopyTo(Stream.Null);
        wrapped.Dispose();
        wrapped.Dispose();

        Assert.That(source.DisposeCount, Is.EqualTo(1), "repeated disposal must not cascade twice");
    }

    /// <summary>
    /// The LZ4 decoder rents from <see cref="ArrayPool{T}"/> and returns on disposal, so it is the codec
    /// where a second disposal releasing again would do real damage. Only the observable half is asserted
    /// — that repeated disposal is accepted — because whether an array went back to the pool twice can
    /// only be seen through <see cref="ArrayPool{T}.Shared"/>, which the whole test suite shares, and any
    /// assertion on which array it hands out next is a race rather than a check.
    /// </summary>
    [Test]
    public void Decompress_WhenDisposedTwice_IsAccepted()
    {
        using var source = new MemoryStream(Encode("lz4", Encoding.UTF8.GetBytes("pooled")));
        var decoder = Lz4Compressor.Default.Decompress(source, leaveOpen: true);
        decoder.CopyTo(Stream.Null);

        decoder.Dispose();

        Assert.DoesNotThrow(() => decoder.Dispose());
    }

    // ---------------------------------------------------------------------------------------------
    // Through the real entry points.
    // ---------------------------------------------------------------------------------------------

    [TestCase(null, TestName = "{m}(no Content-Encoding)")]
    [TestCase("identity", TestName = "{m}(identity)")]
    [TestCase("lz4", TestName = "{m}(lz4)")]
    [TestCase("zstd", TestName = "{m}(zstd)")]
    [TestCase("gzip", TestName = "{m}(gzip)")]
    [TestCase("deflate", TestName = "{m}(deflate)")]
    [TestCase("br", TestName = "{m}(br)")]
    public async Task ExecuteReaderAsync_WithAnEncodedResponse_ReadsEveryRow(string contentEncoding)
    {
        var plaintext = BuildRowBinaryResult(1, 2, 3);
        var body = contentEncoding is null or "identity" ? plaintext : Encode(contentEncoding, plaintext);

        using var client = CreateClient(CreateResponse(body, contentEncoding));

        Assert.That(await ReadAllAsync(client), Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ExecuteReaderAsync_WithUnsupportedResponseCodec_ThrowsActionableErrorNamingTheCodec()
    {
        using var client = CreateClient(CreateResponse([0xFF, 0x06, 0x00, 0x00], "snappy"));

        var ex = Assert.ThrowsAsync<NotSupportedException>(() => ReadAllAsync(client));

        Assert.That(ex.Message, Does.Contain("snappy"));
    }

    [Test]
    public async Task ExecuteNonQueryAsync_WithLz4EncodedResponse_DecodesTheBody()
    {
        // ExecuteNonQueryAsync reads a 7-bit-encoded row count, not a result set.
        using var client = CreateClient(CreateResponse(Encode("lz4", new byte[] { 42 }), "lz4"));

        Assert.That(await client.ExecuteNonQueryAsync("INSERT INTO t VALUES (1)"), Is.EqualTo(42));
    }

    [Test]
    public void HandleError_WithLz4CompressedErrorBody_DecodesItIntoTheExceptionMessage()
    {
        const string serverError = "Code: 60. DB::Exception: Table default.missing does not exist.";
        using var client = CreateClient(CreateResponse(
            Encode("lz4", Encoding.UTF8.GetBytes(serverError)), "lz4", HttpStatusCode.NotFound));

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT * FROM missing"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("Table default.missing does not exist"));
            Assert.That(ex.ErrorCode, Is.EqualTo(60));
        });
    }

    /// <summary>
    /// An error body encoded with a codec we cannot decode must not replace the server's error with a
    /// decompression failure — the status line is the part that matters. Distinct from the
    /// undecodable-codec placeholder (<c>AcceptEncodingTests</c>): here the codec is supported and the
    /// payload is corrupt, which is the catch path rather than the resolver's "unsupported" path.
    /// </summary>
    [Test]
    public void HandleError_WithCorruptCompressedErrorBody_StillSurfacesTheServerError()
    {
        var corrupt = Encode("lz4", Encoding.UTF8.GetBytes("Code: 60. DB::Exception: truncated"));
        Array.Resize(ref corrupt, corrupt.Length / 2);

        using var client = CreateClient(CreateResponse(corrupt, "lz4", HttpStatusCode.InternalServerError));

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("could not be decoded"), "the catch path's own wording");
            Assert.That(ex.Message, Does.Contain("Content-Encoding: lz4"));
            Assert.That(ex.Message, Does.Contain("500"));
        });
    }

    // ---------------------------------------------------------------------------------------------
    // The reader's stream chain: the decompressor sits innermost, so every layer above sees plaintext.
    // ---------------------------------------------------------------------------------------------

    [Test]
    public async Task FromHttpResponseAsync_WithCompressedMidStreamException_StillDetectsTheInBandMarker()
    {
        const string tag = "PU1FNUFH98";
        var body = BuildRowBinaryResult(1, 2, 3)
            .Concat(Encoding.UTF8.GetBytes($"__exception__{tag}\nCode: 395. boom compressed\n14 {tag}__exception__"))
            .ToArray();

        using var response = CreateResponse(Encode("lz4", body), "lz4");
        response.Headers.Add("X-ClickHouse-Exception-Tag", tag);

        using var reader = await ClickHouseDataReader.FromHttpResponseAsync(
            response, TypeSettings.Default, pocoRegistry: null, readBufferSize: 64, readValueConverter: null);

        var ex = Assert.Throws<ClickHouseServerException>(() =>
        {
            while (reader.Read())
            {
                // drain until the in-band exception surfaces
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("boom compressed"));
            Assert.That(ex.ErrorCode, Is.EqualTo(395));
        });
    }

    [Test]
    public async Task FromHttpResponseAsync_WhenTheReaderIsDisposedTwice_IsHarmless()
    {
        using var response = CreateResponse(Encode("lz4", BuildRowBinaryResult(1, 2)), "lz4");

        var reader = await ClickHouseDataReader.FromHttpResponseAsync(
            response, TypeSettings.Default, pocoRegistry: null, readBufferSize: 64, readValueConverter: null);
        while (reader.Read())
        {
            // drain
        }

        reader.Dispose();
        Assert.DoesNotThrow(() => reader.Dispose());
    }

    /// <summary>
    /// Header parsing throws inside <c>FromHttpResponseAsync</c>, before any reader is handed out, so
    /// everything it created must be released there or it leaks with no owner. Asserted as "compression
    /// changes nothing" against the uncompressed path, because that is the part this PR could break: the
    /// absolute count is a property of the pre-existing stream chain, which disposes the transport both
    /// through the response and through the read buffer.
    /// </summary>
    [Test]
    public void FromHttpResponseAsync_WhenHeaderParsingFails_ReleasesTheBodyJustAsTheUncompressedPathDoes()
    {
        // Column count says 1 but the body ends immediately, so ReadHeaders throws.
        var uncompressed = FailingHeaderDisposeCount(new byte[] { 0x01 }, contentEncoding: null);
        var compressed = FailingHeaderDisposeCount(Encode("lz4", new byte[] { 0x01 }), "lz4");

        Assert.Multiple(() =>
        {
            Assert.That(uncompressed, Is.Not.Zero, "the failed call must not leak the body");
            Assert.That(compressed, Is.EqualTo(uncompressed), "adding a decompressor must not change disposal");
        });
    }

    private static int FailingHeaderDisposeCount(byte[] body, string contentEncoding)
    {
        var content = new DisposeCountingStream(body);
        var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(content) };
        if (contentEncoding != null)
            response.Content.Headers.ContentEncoding.Add(contentEncoding);

        Assert.ThrowsAsync<EndOfStreamException>(() => ClickHouseDataReader.FromHttpResponseAsync(
            response, TypeSettings.Default, pocoRegistry: null, readBufferSize: 64, readValueConverter: null));

        return content.DisposeCount;
    }

    private static async Task<int[]> ReadAllAsync(ClickHouseClient client)
    {
        using var reader = await client.ExecuteReaderAsync("SELECT n FROM t");
        var values = new List<int>();
        while (reader.Read())
            values.Add(reader.GetInt32(0));

        return values.ToArray();
    }

    /// <summary>Counts disposals so decoder ownership can be asserted.</summary>
    private sealed class DisposeCountingStream : MemoryStream
    {
        public DisposeCountingStream(byte[] buffer)
            : base(buffer)
        {
        }

        public int DisposeCount { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                DisposeCount++;

            base.Dispose(disposing);
        }
    }
}
