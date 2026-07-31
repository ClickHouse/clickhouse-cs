using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Http;
using ClickHouse.Driver.Tests.Utilities;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// Response-body decompression, driven by the response's <c>Content-Encoding</c> header. Uses a stub
/// <see cref="HttpMessageHandler"/> (see <see cref="TrackingHandler"/>) so the codec of the response is
/// controlled exactly — a real handler's <c>AutomaticDecompression</c> would strip the header for
/// gzip/deflate and make these cases unobservable.
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
        "deflate" => new DeflateStream(destination, CompressionLevel.Fastest, leaveOpen: true),
        "br" or "brotli" => new BrotliStream(destination, CompressionLevel.Fastest, leaveOpen: true),
        "lz4" => Lz4Compressor.Default.Compress(destination, leaveOpen: true),
        _ => throw new ArgumentOutOfRangeException(nameof(contentEncoding), contentEncoding, null),
    };

    private static HttpResponseMessage CreateResponse(byte[] body, string contentEncoding, HttpStatusCode status = HttpStatusCode.OK)
    {
        var content = new ByteArrayContent(body);
        if (contentEncoding != null)
            content.Headers.ContentEncoding.Add(contentEncoding);

        return new HttpResponseMessage(status) { Content = content };
    }

    private static ClickHouseClient CreateClient(HttpResponseMessage response, IClickHouseCompressor responseCompressor = null)
    {
        var httpClient = new HttpClient(new TrackingHandler(response));
        return new ClickHouseClient(new ClickHouseClientSettings
        {
            HttpClient = httpClient,
            ResponseCompressor = responseCompressor,
        });
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

        var result = ResponseDecompression.Wrap(source, contentEncoding, Lz4Compressor.Default, leaveOpen: true);

        Assert.That(result, Is.SameAs(source), "an unencoded body must pass through untouched");
    }

    [TestCase("lz4", TestName = "{m}(lz4)")]
    [TestCase("LZ4", TestName = "{m}(upper-cased)")]
    [TestCase("  lz4 ", TestName = "{m}(surrounded by whitespace)")]
    public void Wrap_WhenTokenMatchesConfiguredCompressor_DecodesWithThatCompressor(string contentEncoding)
    {
        var plaintext = Encoding.UTF8.GetBytes("payload the BCL cannot decode");
        using var source = new MemoryStream(Encode("lz4", plaintext));

        using var result = ResponseDecompression.Wrap(source, contentEncoding, Lz4Compressor.Default, leaveOpen: true);
        using var decoded = new MemoryStream();
        result.CopyTo(decoded);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.SameAs(source));
            Assert.That(decoded.ToArray(), Is.EqualTo(plaintext));
        });
    }

    /// <summary>
    /// The whitespace tolerance is symmetric: the padding may sit on the <i>compressor's</i> declared token
    /// rather than on the response header. A custom codec declaring <c>"  lz4  "</c> must still be selected
    /// for the server's clean <c>lz4</c> — otherwise the driver silently skips the configured decoder and
    /// falls through to "unsupported codec".
    /// </summary>
    [TestCase("lz4", TestName = "{m}(clean response token)")]
    [TestCase("  lz4 ", TestName = "{m}(both sides padded)")]
    public void Wrap_WhenConfiguredCompressorTokenHasSurroundingWhitespace_StillMatches(string contentEncoding)
    {
        var plaintext = Encoding.UTF8.GetBytes("payload the BCL cannot decode");
        using var source = new MemoryStream(Encode("lz4", plaintext));

        using var result = ResponseDecompression.Wrap(source, contentEncoding, new PaddedTokenLz4Compressor(), leaveOpen: true);
        using var decoded = new MemoryStream();
        result.CopyTo(decoded);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.SameAs(source));
            Assert.That(decoded.ToArray(), Is.EqualTo(plaintext));
        });
    }

    [TestCase("gzip")]
    [TestCase("deflate")]
    [TestCase("br")]
    [TestCase("brotli")]
    public void Wrap_WithBclCodecAndNoConfiguredCompressor_DecodesWithTheBclStream(string contentEncoding)
    {
        var plaintext = Encoding.UTF8.GetBytes("decoded by the BCL");
        using var source = new MemoryStream(Encode(contentEncoding, plaintext));

        using var result = ResponseDecompression.Wrap(source, contentEncoding, responseCompressor: null, leaveOpen: true);
        using var decoded = new MemoryStream();
        result.CopyTo(decoded);

        Assert.That(decoded.ToArray(), Is.EqualTo(plaintext));
    }

    /// <summary>
    /// HTTP's <c>deflate</c> is the zlib format (RFC 1950), NOT raw DEFLATE, and zlib is what ClickHouse
    /// emits — its bodies begin <c>78 5E</c>. A bare <see cref="DeflateStream"/> throws
    /// <see cref="InvalidDataException"/> on those bytes, so this pins the zlib-wrapped form specifically.
    /// The raw-DEFLATE spelling stays covered by
    /// <see cref="Wrap_WithBclCodecAndNoConfiguredCompressor_DecodesWithTheBclStream"/>, whose encoder is a
    /// raw <see cref="DeflateStream"/>; between them both forms are exercised rather than only the one our
    /// own encoder happens to produce.
    /// </summary>
    [Test]
    public void Wrap_WithZLibWrappedDeflate_DecodesIt()
    {
        var plaintext = Encoding.UTF8.GetBytes("zlib-wrapped deflate, exactly as ClickHouse sends it");
        using var buffer = new MemoryStream();
        using (var encoder = new ZLibStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
        {
            encoder.Write(plaintext, 0, plaintext.Length);
        }

        var encoded = buffer.ToArray();
        Assert.That(encoded[0], Is.EqualTo(0x78), "a zlib stream must start with the 0x78 CMF byte");

        using var source = new MemoryStream(encoded);
        using var decoded = ResponseDecompression.Wrap(source, "deflate", responseCompressor: null, leaveOpen: true);
        using var result = new MemoryStream();
        decoded.CopyTo(result);

        Assert.That(result.ToArray(), Is.EqualTo(plaintext));
    }

    [Test]
    public async Task ExecuteReaderAsync_WithZLibWrappedDeflateResponse_DecodesTheRows()
    {
        var plaintext = BuildRowBinaryResult(11, 22, 33);
        using var buffer = new MemoryStream();
        using (var encoder = new ZLibStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
        {
            encoder.Write(plaintext, 0, plaintext.Length);
        }

        using var response = CreateResponse(buffer.ToArray(), "deflate");
        using var client = CreateClient(response);

        var values = new List<int>();
        using var reader = await client.ExecuteReaderAsync("SELECT n");
        while (await reader.ReadAsync())
            values.Add(reader.GetInt32(0));

        Assert.That(values, Is.EqualTo(new[] { 11, 22, 33 }));
    }

    /// <summary>
    /// A server that stacks codecs (<c>Content-Encoding: gzip, lz4</c>) is not supported, but it must say
    /// so rather than decode with only one of them and hand back garbage.
    /// </summary>
    [Test]
    public void Wrap_WithStackedContentEncodings_ThrowsNamingBoth()
    {
        var body = new byte[] { 0x1F, 0x8B, 0x00 };
        using var source = new MemoryStream(body);
        var content = new ByteArrayContent(body);
        content.Headers.ContentEncoding.Add("gzip");
        content.Headers.ContentEncoding.Add("lz4");
        using var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = content };

        Assert.That(ResponseDecompression.GetContentEncoding(response), Is.EqualTo("gzip, lz4"));

        var ex = Assert.Throws<NotSupportedException>(
            () => ResponseDecompression.Wrap(source, response, Lz4Compressor.Default, leaveOpen: true));
        Assert.That(ex.Message, Does.Contain("gzip, lz4"));
    }

    [TestCase("zstd")]
    [TestCase("snappy")]
    [TestCase("xz")]
    public void Wrap_WithUnsupportedCodec_ThrowsNamingTheCodecAndTheFix(string contentEncoding)
    {
        using var source = new MemoryStream(new byte[] { 0x28, 0xB5, 0x2F, 0xFD });

        var ex = Assert.Throws<NotSupportedException>(
            () => ResponseDecompression.Wrap(source, contentEncoding, Lz4Compressor.Default, leaveOpen: true));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain(contentEncoding));
            Assert.That(ex.Message, Does.Contain("ResponseCompressor"));
        });
    }

    /// <summary>
    /// The remediation advice must not imply that dropping down to <c>lz4</c> needs no configuration: lz4
    /// is only decodable once a matching <c>ResponseCompressor</c> is set, so only gzip/deflate/br may be
    /// offered as the zero-configuration fallback. Pinned because a message that mis-states which codecs
    /// work out of the box actively misleads whoever is debugging the failure.
    /// </summary>
    [Test]
    public void DescribeUnsupported_WithNoConfiguredCompressor_OffersOnlyZeroConfigurationCodecsAsTheFallback()
    {
        var message = ResponseDecompression.DescribeUnsupported("zstd", responseCompressor: null);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("no response compressor is configured"));
            Assert.That(
                message,
                Does.Contain("needs no configuration (gzip, deflate or br)"),
                "lz4 must not be listed among the codecs that work without a ResponseCompressor");
            Assert.That(
                message,
                Does.Contain("lz4 is only decodable once such a compressor is configured"),
                "the message must say what lz4 requires");
        });
    }

    [Test]
    public void TryWrap_WithUnsupportedCodec_ReturnsFalseInsteadOfThrowing()
    {
        using var source = new MemoryStream(new byte[] { 0x28, 0xB5, 0x2F, 0xFD });

        var wrapped = ResponseDecompression.TryWrap(source, "zstd", responseCompressor: null, leaveOpen: true, out var result);

        Assert.Multiple(() =>
        {
            Assert.That(wrapped, Is.False);
            Assert.That(result, Is.Null);
        });
    }

    // ---------------------------------------------------------------------------------------------
    // ExecuteReaderAsync: the full reader path (decompressor -> pooled buffer -> reader).
    // ---------------------------------------------------------------------------------------------

    [Test]
    public async Task ExecuteReaderAsync_WhenResponseHasNoContentEncoding_ReadsTheBodyUntouched()
    {
        using var response = CreateResponse(BuildRowBinaryResult(7, 8, 9), contentEncoding: null);
        using var client = CreateClient(response, Lz4Compressor.Default);

        Assert.That(await ReadAllAsync(client), Is.EqualTo(new[] { 7, 8, 9 }));
    }

    [Test]
    public async Task ExecuteReaderAsync_WhenResponseIsIdentityEncoded_ReadsTheBodyUntouched()
    {
        using var response = CreateResponse(BuildRowBinaryResult(7, 8, 9), "identity");
        using var client = CreateClient(response, Lz4Compressor.Default);

        Assert.That(await ReadAllAsync(client), Is.EqualTo(new[] { 7, 8, 9 }));
    }

    [Test]
    public async Task ExecuteReaderAsync_WithLz4EncodedResponse_DecodesTheRows()
    {
        using var response = CreateResponse(Encode("lz4", BuildRowBinaryResult(1, 2, 3)), "lz4");
        using var client = CreateClient(response, Lz4Compressor.Default);

        Assert.That(await ReadAllAsync(client), Is.EqualTo(new[] { 1, 2, 3 }));
    }

    /// <summary>
    /// <c>br</c> is advertised as supported but is not in the HTTP handler's <c>AutomaticDecompression</c>
    /// mask (gzip|deflate only), so before this feature a brotli response reached the reader still
    /// compressed. It must now decode both with a configured Brotli compressor and via the BCL branch.
    /// </summary>
    [Test]
    public async Task ExecuteReaderAsync_WithBrotliEncodedResponse_DecodesTheRows(
        [Values("br", "brotli")] string contentEncoding,
        [Values(true, false)] bool configureCompressor)
    {
        using var response = CreateResponse(Encode(contentEncoding, BuildRowBinaryResult(4, 5)), contentEncoding);
        using var client = CreateClient(response, configureCompressor ? BrotliCompressor.Default : null);

        Assert.That(await ReadAllAsync(client), Is.EqualTo(new[] { 4, 5 }));
    }

    [Test]
    public async Task ExecuteReaderAsync_WithGzipEncodedResponse_DecodesTheRows()
    {
        using var response = CreateResponse(Encode("gzip", BuildRowBinaryResult(11, 12)), "gzip");
        using var client = CreateClient(response);

        Assert.That(await ReadAllAsync(client), Is.EqualTo(new[] { 11, 12 }));
    }

    /// <summary>
    /// Before the resolver existed, an undecodable body was fed straight to the binary reader and blew up
    /// as a bogus type-parse failure (or a gzip-magic-byte guess). It must now name the actual codec.
    /// </summary>
    [Test]
    public void ExecuteReaderAsync_WithUnsupportedResponseCodec_ThrowsActionableErrorNamingTheCodec()
    {
        using var response = CreateResponse(new byte[] { 0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x01 }, "zstd");
        using var client = CreateClient(response, Lz4Compressor.Default);

        var ex = Assert.ThrowsAsync<NotSupportedException>(() => client.ExecuteReaderAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("zstd"));
            Assert.That(ex.Message, Does.Contain("ResponseCompression"));
        });
    }

    [Test]
    public async Task ExecuteNonQueryAsync_WithLz4EncodedResponse_DecodesTheBody()
    {
        // ExecuteNonQueryAsync reads a 7-bit-encoded row count straight off the body.
        using var response = CreateResponse(Encode("lz4", new byte[] { 42 }), "lz4");
        using var client = CreateClient(response, Lz4Compressor.Default);

        Assert.That(await client.ExecuteNonQueryAsync("INSERT INTO t VALUES"), Is.EqualTo(42));
    }

    /// <summary>
    /// A compressed <b>error</b> body must still reach the caller as a readable server message.
    /// </summary>
    [Test]
    public void HandleError_WithLz4CompressedErrorBody_DecodesItIntoTheExceptionMessage()
    {
        const string serverMessage = "Code: 60. DB::Exception: Table default.nope does not exist";
        using var response = CreateResponse(
            Encode("lz4", Encoding.UTF8.GetBytes(serverMessage)), "lz4", HttpStatusCode.InternalServerError);
        using var client = CreateClient(response, Lz4Compressor.Default);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.That(ex.Message, Does.Contain("Table default.nope does not exist"));
    }

    /// <summary>
    /// The graceful fallback: an error body in a codec we cannot decode must not turn the server's error
    /// into a decompression crash.
    /// </summary>
    [Test]
    public void HandleError_WithUndecodableErrorBody_StillSurfacesTheServerErrorAsAPlaceholder()
    {
        using var response = CreateResponse(
            new byte[] { 0x28, 0xB5, 0x2F, 0xFD }, "zstd", HttpStatusCode.InternalServerError);
        using var client = CreateClient(response, Lz4Compressor.Default);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.That(ex.Message, Does.Contain("unsupported Content-Encoding: zstd"));
    }

    // ---------------------------------------------------------------------------------------------
    // Layering invariants and resource regressions.
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// The decompressor must sit INNERMOST, below <c>ExceptionTagAwareStream</c>: the in-band exception
    /// marker only exists in the plaintext, so if the scanner were layered under the decoder (or the
    /// decoder omitted) the marker would never be found and the caller would see a truncated-stream error
    /// instead of the server's message.
    /// </summary>
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
            response, TypeSettings.Default, pocoRegistry: null, readBufferSize: 64, readValueConverter: null,
            responseCompressor: Lz4Compressor.Default);

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

    /// <summary>
    /// The pooled read buffer is rented from <see cref="ArrayPool{T}"/>; adding the decompressor to the
    /// chain must not lose the return. Renting the same size straight after disposal has to hand back the
    /// very array the reader used.
    /// </summary>
    [Test]
    public async Task FromHttpResponseAsync_OverACompressedBody_ReturnsThePooledReadBufferToTheArrayPool()
    {
        const int bufferSize = 4096;

        // Drain the pool bucket first so the identity comparison below is meaningful.
        var primer = ArrayPool<byte>.Shared.Rent(bufferSize);
        ArrayPool<byte>.Shared.Return(primer);

        using (var response = CreateResponse(Encode("lz4", BuildRowBinaryResult(1, 2, 3)), "lz4"))
        {
            using var reader = await ClickHouseDataReader.FromHttpResponseAsync(
                response, TypeSettings.Default, pocoRegistry: null, readBufferSize: bufferSize,
                readValueConverter: null, responseCompressor: Lz4Compressor.Default);

            while (reader.Read())
            {
                // drain
            }
        }

        var rented = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            Assert.That(rented, Is.SameAs(primer), "the reader's pooled buffer was not returned to the pool");
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Normal path: the decompressor the reader created is disposed when the reader is, so the codec's
    /// own state is released and nothing is leaked per query.
    /// </summary>
    [Test]
    public async Task FromHttpResponseAsync_WhenTheReaderIsDisposed_DisposesTheDecompressorExactlyOnce()
    {
        var compressor = new TrackingCompressor();
        using var response = CreateResponse(BuildRowBinaryResult(1, 2), compressor.ContentEncoding);

        using (var reader = await ClickHouseDataReader.FromHttpResponseAsync(
            response, TypeSettings.Default, pocoRegistry: null, readBufferSize: 64, readValueConverter: null,
            responseCompressor: compressor))
        {
            while (reader.Read())
            {
                // drain
            }

            Assert.That(compressor.Last.DisposeCount, Is.Zero, "must stay open while the reader is alive");
        }

        Assert.That(compressor.Last.DisposeCount, Is.EqualTo(1));
    }

    /// <summary>
    /// Catch path: header parsing throws inside <c>FromHttpResponseAsync</c>, before any reader is handed
    /// out, so the decompressor must be disposed there or it leaks with no owner.
    /// </summary>
    [Test]
    public void FromHttpResponseAsync_WhenHeaderParsingFails_DisposesTheDecompressor()
    {
        var compressor = new TrackingCompressor();

        // Column count says 1 but the body ends immediately, so ReadHeaders throws.
        using var response = CreateResponse(new byte[] { 0x01 }, compressor.ContentEncoding);

        Assert.ThrowsAsync<EndOfStreamException>(() => ClickHouseDataReader.FromHttpResponseAsync(
            response, TypeSettings.Default, pocoRegistry: null, readBufferSize: 64, readValueConverter: null,
            responseCompressor: compressor));

        Assert.That(compressor.Last.DisposeCount, Is.EqualTo(1));
    }

    // ---------------------------------------------------------------------------------------------
    // Accept-Encoding / enable_http_compression wiring.
    // ---------------------------------------------------------------------------------------------

    private static (ClickHouseClient client, TrackingHandler handler) CreateTrackingClient(
        IClickHouseCompressor responseCompressor, bool useCompression)
    {
        var handler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent(Array.Empty<byte>()),
        });
        var client = new ClickHouseClient(new ClickHouseClientSettings
        {
            HttpClient = new HttpClient(handler),
            UseCompression = useCompression,
            ResponseCompressor = responseCompressor,
        });
        return (client, handler);
    }

    [Test]
    public async Task AddDefaultHttpHeaders_WithNoResponseCompressor_LeavesTheDefaultAcceptEncodingUnchanged()
    {
        var (client, handler) = CreateTrackingClient(responseCompressor: null, useCompression: true);
        using (client)
        {
            await client.ExecuteNonQueryAsync("SELECT 1");
        }

        var encodings = handler.Requests.Single().Headers.AcceptEncoding.Select(e => e.Value).ToArray();
        Assert.That(encodings, Is.EqualTo(new[] { "gzip", "deflate" }), "lz4 must never be advertised by default");
    }

    [Test]
    public async Task AddDefaultHttpHeaders_WithResponseCompressor_AddsItsTokenAndForcesHttpCompression()
    {
        var (client, handler) = CreateTrackingClient(Lz4Compressor.Default, useCompression: true);
        using (client)
        {
            await client.ExecuteNonQueryAsync("SELECT 1");
        }

        var request = handler.Requests.Single();
        Assert.Multiple(() =>
        {
            Assert.That(request.Headers.AcceptEncoding.Select(e => e.Value), Is.EqualTo(new[] { "gzip", "deflate", "lz4" }));
            Assert.That(request.RequestUri.Query, Does.Contain("enable_http_compression=true"));
        });
    }

    /// <summary>
    /// A padded compressor token is normalized once and used for both the duplicate check and the header
    /// value, so it is advertised exactly once and in clean form — never as a second, whitespace-bearing
    /// copy of a codec already in the list.
    /// </summary>
    [Test]
    public async Task AddDefaultHttpHeaders_WithPaddedResponseCompressorToken_AdvertisesItOnceTrimmed()
    {
        var (client, handler) = CreateTrackingClient(new PaddedTokenLz4Compressor(), useCompression: true);
        using (client)
        {
            await client.ExecuteNonQueryAsync("SELECT 1");
        }

        Assert.That(
            handler.Requests.Single().Headers.AcceptEncoding.Select(e => e.Value),
            Is.EqualTo(new[] { "gzip", "deflate", "lz4" }));
    }

    [Test]
    public async Task AddDefaultHttpHeaders_WithPaddedResponseCompressorTokenAlreadyAdvertised_DoesNotDuplicateIt()
    {
        var (client, handler) = CreateTrackingClient(new PaddedTokenGZipCompressor(), useCompression: true);
        using (client)
        {
            await client.ExecuteNonQueryAsync("SELECT 1");
        }

        Assert.That(
            handler.Requests.Single().Headers.AcceptEncoding.Select(e => e.Value),
            Is.EqualTo(new[] { "gzip", "deflate" }),
            "gzip is already advertised by default, so the padded token must not add a second entry");
    }

    [Test]
    public async Task AddDefaultHttpHeaders_WithResponseCompressorAndCompressionDisabled_StillForcesHttpCompression()
    {
        var (client, handler) = CreateTrackingClient(Lz4Compressor.Default, useCompression: false);
        using (client)
        {
            await client.ExecuteNonQueryAsync("SELECT 1");
        }

        var request = handler.Requests.Single();
        Assert.Multiple(() =>
        {
            Assert.That(request.Headers.AcceptEncoding.Select(e => e.Value), Is.EqualTo(new[] { "lz4" }));
            Assert.That(request.RequestUri.Query, Does.Contain("enable_http_compression=true"));
        });
    }

    [Test]
    public async Task AddDefaultHttpHeaders_WithExplicitAcceptEncoding_OverridesTheResponseCompressorToken()
    {
        var (client, handler) = CreateTrackingClient(Lz4Compressor.Default, useCompression: true);
        using (client)
        {
            await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = "gzip" });
        }

        var encodings = handler.Requests.Single().Headers.AcceptEncoding.Select(e => e.Value).ToArray();
        Assert.That(encodings, Is.EqualTo(new[] { "gzip" }), "an explicit Accept-Encoding still wins for the header");
    }

    [Test]
    public async Task QueryOptionsResponseCompressor_WhenSet_OverridesTheClientLevelCompressor()
    {
        // The client is configured for gzip; the per-query override asks for lz4 and the response arrives
        // as lz4 — only the override can decode it.
        using var response = CreateResponse(Encode("lz4", BuildRowBinaryResult(21)), "lz4");
        using var client = CreateClient(response, GZipCompressor.Default);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT 1", options: new QueryOptions { ResponseCompressor = Lz4Compressor.Default });

        Assert.That(reader.Read() ? reader.GetInt32(0) : -1, Is.EqualTo(21));
    }

    // ---------------------------------------------------------------------------------------------
    // ClickHouseRawResult: new decoding member vs. the four verbatim pass-throughs.
    // ---------------------------------------------------------------------------------------------

    [Test]
    public async Task ReadDecompressedStreamAsync_WithLz4Response_ReturnsThePlaintext()
    {
        var plaintext = Encoding.UTF8.GetBytes("0\n1\n2\n");
        using var response = CreateResponse(Encode("lz4", plaintext), "lz4");
        using var raw = new ClickHouseRawResult(response, Lz4Compressor.Default);

        using var decompressed = await raw.ReadDecompressedStreamAsync();
        using var buffer = new MemoryStream();
        await decompressed.CopyToAsync(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(plaintext));
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_WithUnencodedResponse_ReturnsTheRawContentStream()
    {
        var plaintext = Encoding.UTF8.GetBytes("0\n1\n2\n");
        using var response = CreateResponse(plaintext, contentEncoding: null);
        using var raw = new ClickHouseRawResult(response, Lz4Compressor.Default);

        using var buffer = new MemoryStream();
        using (var stream = await raw.ReadDecompressedStreamAsync())
        {
            await stream.CopyToAsync(buffer);
        }

        Assert.That(buffer.ToArray(), Is.EqualTo(plaintext));
    }

    /// <summary>
    /// Contrast case: the four pre-existing members are verbatim pass-throughs and must stay that way —
    /// <c>examples/Select/Select_005_CompressedRawExport.cs</c> relies on getting the raw compressed bytes.
    /// </summary>
    [Test]
    public async Task PreExistingRawResultMembers_WithCompressedResponse_StillReturnTheRawBytesVerbatim()
    {
        var compressed = Encode("lz4", Encoding.UTF8.GetBytes("0\n1\n2\n"));

        using (var response = CreateResponse(compressed, "lz4"))
        using (var raw = new ClickHouseRawResult(response, Lz4Compressor.Default))
        {
            Assert.That(await raw.ReadAsByteArrayAsync(), Is.EqualTo(compressed), "ReadAsByteArrayAsync must not decode");
        }

        using (var response = CreateResponse(compressed, "lz4"))
        using (var raw = new ClickHouseRawResult(response, Lz4Compressor.Default))
        {
            using var buffer = new MemoryStream();
            using (var stream = await raw.ReadAsStreamAsync())
            {
                await stream.CopyToAsync(buffer);
            }

            Assert.That(buffer.ToArray(), Is.EqualTo(compressed), "ReadAsStreamAsync must not decode");
        }

        using (var response = CreateResponse(compressed, "lz4"))
        using (var raw = new ClickHouseRawResult(response, Lz4Compressor.Default))
        {
            using var buffer = new MemoryStream();
            await raw.CopyToAsync(buffer);

            Assert.That(buffer.ToArray(), Is.EqualTo(compressed), "CopyToAsync must not decode");
        }

        using (var response = CreateResponse(compressed, "lz4"))
        using (var raw = new ClickHouseRawResult(response, Lz4Compressor.Default))
        {
            var asString = await raw.ReadAsStringAsync();

            Assert.That(Encoding.UTF8.GetBytes(asString), Is.Not.EqualTo(Encoding.UTF8.GetBytes("0\n1\n2\n")),
                "ReadAsStringAsync must not decode");
        }
    }

    /// <summary>
    /// A decoder holds pooled buffers — the LZ4 one rents from <c>ArrayPool&lt;byte&gt;.Shared</c> and
    /// returns them only on disposal, with no finalizer — so the raw result must release the decoder it
    /// inserted, not just the HTTP response.
    /// </summary>
    [Test]
    public async Task RawResultDispose_DisposesTheDecoderItInserted()
    {
        var codec = new TrackingCompressor();
        using var response = CreateResponse(Encoding.UTF8.GetBytes("0\n1\n2\n"), codec.ContentEncoding);
        var raw = new ClickHouseRawResult(response, codec);

        await raw.ReadDecompressedStreamAsync();
        Assume.That(codec.Last, Is.Not.Null, "the codec's Decompress must have been used");
        Assume.That(codec.Last.DisposeCount, Is.Zero, "not disposed yet");

        raw.Dispose();

        Assert.That(codec.Last.DisposeCount, Is.EqualTo(1), "the raw result owns the decoder it created");

        raw.Dispose();

        Assert.That(codec.Last.DisposeCount, Is.EqualTo(1),
            "a second Dispose() must not release the decoder's pooled buffers twice");
    }

    /// <summary>
    /// A second call must not stack a fresh decoder over a body the first one has already partly
    /// consumed (which would both mis-decode and leak the first decoder).
    /// </summary>
    [Test]
    public async Task ReadDecompressedStreamAsync_CalledTwice_ReturnsTheSameDecoder()
    {
        var codec = new TrackingCompressor();
        using var response = CreateResponse(Encoding.UTF8.GetBytes("0\n1\n2\n"), codec.ContentEncoding);
        using var raw = new ClickHouseRawResult(response, codec);

        var first = await raw.ReadDecompressedStreamAsync();
        var second = await raw.ReadDecompressedStreamAsync();

        Assert.That(second, Is.SameAs(first));
    }

    /// <summary>
    /// When the codec is undecodable the call throws — but it must leave the body readable, because the
    /// caller's recourse is to read the still-compressed bytes and decode them itself. The content here is
    /// deliberately <b>unbuffered</b> (<see cref="StreamContent"/> over a forward-only stream), matching a
    /// real raw result, which is fetched with <c>HttpCompletionOption.ResponseHeadersRead</c> — so the
    /// recovery has to work through <c>ReadAsStreamAsync</c> rather than a re-read of a buffered body.
    /// Disposal stays the caller's single obligation and must not fail either.
    /// </summary>
    [Test]
    public async Task ReadDecompressedStreamAsync_WithUnsupportedCodec_LeavesTheBodyReadableForManualDecoding()
    {
        var body = Encoding.UTF8.GetBytes("not really zstd");
        var content = new StreamContent(new ForwardOnlyStream(body));
        content.Headers.ContentEncoding.Add("zstd");
        var raw = new ClickHouseRawResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content }, Lz4Compressor.Default);

        Assert.ThrowsAsync<NotSupportedException>(() => raw.ReadDecompressedStreamAsync());

        using var buffer = new MemoryStream();
        await (await raw.ReadAsStreamAsync()).CopyToAsync(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(body),
            "the content stream must not have been disposed by the failed call");
        Assert.DoesNotThrow(raw.Dispose);
    }

    /// <summary>
    /// The examples use <c>await using</c> on the returned stream, so the decoder can be disposed by the
    /// caller and then again by the raw result that owns it. That must be harmless — a decoder holding
    /// pooled buffers must not return the same array to the pool twice.
    /// </summary>
    [Test]
    public async Task RawResultDispose_AfterTheCallerAlreadyDisposedTheDecoder_IsHarmless()
    {
        var codec = new TrackingCompressor();
        using var response = CreateResponse(Encoding.UTF8.GetBytes("0\n1\n2\n"), codec.ContentEncoding);
        var raw = new ClickHouseRawResult(response, codec);

        (await raw.ReadDecompressedStreamAsync()).Dispose();

        Assert.DoesNotThrow(raw.Dispose);
        Assert.That(codec.Last.DisposeCount, Is.EqualTo(2),
            "both the caller's and the owner's disposal reach the decoder, which must tolerate it");
    }

    // ---------------------------------------------------------------------------------------------
    // Settings plumbing. There is precedent (the AcceptEncoding bug, CHANGELOG.md) for a new property
    // being silently dropped by exactly these copy helpers.
    // ---------------------------------------------------------------------------------------------

    [Test]
    public void ClickHouseClientSettings_CopyConstructor_PreservesResponseCompressor()
    {
        var original = new ClickHouseClientSettings { ResponseCompressor = Lz4Compressor.Default };

        var copy = new ClickHouseClientSettings(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.ResponseCompressor, Is.SameAs(Lz4Compressor.Default));
            Assert.That(copy, Is.EqualTo(original));
        });
    }

    [Test]
    public void ClickHouseClientSettings_Equality_DistinguishesResponseCompressor()
    {
        var lz4 = new ClickHouseClientSettings { ResponseCompressor = Lz4Compressor.Default };
        var gzip = new ClickHouseClientSettings { ResponseCompressor = GZipCompressor.Default };
        var none = new ClickHouseClientSettings();

        Assert.Multiple(() =>
        {
            Assert.That(lz4, Is.Not.EqualTo(gzip));
            Assert.That(lz4, Is.Not.EqualTo(none));
            Assert.That(lz4.GetHashCode(), Is.Not.EqualTo(none.GetHashCode()));
        });
    }

    [Test]
    public void ClickHouseClientSettings_ToString_IncludesResponseCompressionOnlyWhenSet()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new ClickHouseClientSettings().ToString(), Does.Not.Contain("ResponseCompression"));
            Assert.That(
                new ClickHouseClientSettings { ResponseCompressor = Lz4Compressor.Default }.ToString(),
                Does.Contain("ResponseCompression=lz4"));
        });
    }

    /// <summary>
    /// A CORRUPT compressed error body must not turn the server's error into a codec crash. The status
    /// line is the information that matters, so it has to survive a failed decode — the decompressor
    /// here is handed lz4-declared bytes that are not valid lz4.
    /// </summary>
    [Test]
    public void HandleError_WithCorruptCompressedErrorBody_StillSurfacesTheServerError()
    {
        using var response = CreateResponse(
            new byte[] { 0x04, 0x22, 0x4D, 0x18, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02 },
            "lz4",
            HttpStatusCode.InternalServerError);
        response.ReasonPhrase = "Internal Server Error";
        using var client = CreateClient(response, Lz4Compressor.Default);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("500"));
            Assert.That(ex.Message, Does.Contain("system.query_log"));
        });
    }

    /// <summary>
    /// <c>ToString()</c> must stay parseable by this type's own connection-string parser. A custom codec
    /// has no connection-string spelling, so it is omitted rather than emitted as a token
    /// (e.g. <c>ResponseCompression=zstd</c>) that <c>Parse</c> would reject.
    /// </summary>
    [Test]
    public void ClickHouseClientSettings_ToString_OmitsCustomResponseCompressorSoTheResultStaysParseable()
    {
        var settings = new ClickHouseClientSettings { ResponseCompressor = new TrackingCompressor() };

        var text = settings.ToString();

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Not.Contain("ResponseCompression"));
            Assert.DoesNotThrow(() => new ClickHouseClientSettings(text));
        });
    }

    [Test]
    public void ClickHouseClientSettings_Validate_WithBlankContentEncoding_Throws()
    {
        var settings = new ClickHouseClientSettings { ResponseCompressor = new BlankTokenCompressor() };

        Assert.Throws<InvalidOperationException>(settings.Validate);
    }

    [Test]
    public void QueryOptionsWithQueryId_PreservesResponseCompressor()
    {
        var options = new QueryOptions { ResponseCompressor = Lz4Compressor.Default };

        Assert.That(options.WithQueryId("q-1").ResponseCompressor, Is.SameAs(Lz4Compressor.Default));
    }

    [Test]
    public void InsertOptionsCopyHelpers_PreserveResponseCompressor()
    {
        var options = new InsertOptions { ResponseCompressor = Lz4Compressor.Default };

        Assert.Multiple(() =>
        {
            Assert.That(options.WithQueryId("q-1").ResponseCompressor, Is.SameAs(Lz4Compressor.Default));
            Assert.That(
                options.WithColumnTypes(new Dictionary<string, string> { ["a"] = "Int32" }).ResponseCompressor,
                Is.SameAs(Lz4Compressor.Default));
        });
    }

    // ---------------------------------------------------------------------------------------------
    // Connection-string parity: ORM users cannot reach the settings object.
    // ---------------------------------------------------------------------------------------------

    [TestCase("lz4", typeof(Lz4Compressor))]
    [TestCase("LZ4", typeof(Lz4Compressor))]
    [TestCase("gzip", typeof(GZipCompressor))]
    [TestCase("br", typeof(BrotliCompressor))]
    [TestCase("brotli", typeof(BrotliCompressor))]
    public void FromConnectionString_WithResponseCompression_ResolvesTheCompressor(string value, Type expected)
    {
        var settings = new ClickHouseClientSettings($"Host=localhost;ResponseCompression={value}");

        Assert.That(settings.ResponseCompressor, Is.InstanceOf(expected));
    }

    [TestCase("none")]
    [TestCase("None")]
    [TestCase("identity")]
    public void FromConnectionString_WithResponseCompressionNone_ResolvesToNull(string value)
    {
        var settings = new ClickHouseClientSettings($"Host=localhost;ResponseCompression={value}");

        Assert.That(settings.ResponseCompressor, Is.Null);
    }

    [Test]
    public void FromConnectionString_WithoutResponseCompression_ResolvesToNull()
    {
        Assert.That(new ClickHouseClientSettings("Host=localhost").ResponseCompressor, Is.Null);
    }

    [Test]
    public void FromConnectionString_WithUnknownResponseCompression_ThrowsNamingTheSupportedValues()
    {
        var ex = Assert.Throws<ArgumentException>(() => new ClickHouseClientSettings("Host=localhost;ResponseCompression=zstd"));

        Assert.That(ex.Message, Does.Contain("lz4"));
    }

    [Test]
    public void ConnectionStringBuilder_FromSettings_RoundTripsResponseCompression()
    {
        var settings = new ClickHouseClientSettings { ResponseCompressor = Lz4Compressor.Default };

        var builder = ClickHouseConnectionStringBuilder.FromSettings(settings);

        Assert.Multiple(() =>
        {
            Assert.That(builder.ResponseCompression, Is.EqualTo("lz4"));
            Assert.That(builder.ToSettings().ResponseCompressor, Is.SameAs(Lz4Compressor.Default));
        });
    }

    private static async Task<int[]> ReadAllAsync(ClickHouseClient client)
    {
        using var reader = await client.ExecuteReaderAsync("SELECT n FROM t");
        var values = new List<int>();
        while (reader.Read())
            values.Add(reader.GetInt32(0));

        return values.ToArray();
    }

    private sealed class BlankTokenCompressor : IClickHouseCompressor
    {
        public string ContentEncoding => "  ";

        public Stream Compress(Stream destination, bool leaveOpen) => destination;
    }

    /// <summary>
    /// A real lz4 codec that declares its token with sloppy surrounding whitespace — the shape a
    /// third-party <see cref="IClickHouseCompressor"/> can easily have.
    /// </summary>
    private sealed class PaddedTokenLz4Compressor : IClickHouseCompressor
    {
        public string ContentEncoding => "  lz4  ";

        public Stream Compress(Stream destination, bool leaveOpen) => Lz4Compressor.Default.Compress(destination, leaveOpen);

        public Stream Decompress(Stream source, bool leaveOpen) => Lz4Compressor.Default.Decompress(source, leaveOpen);
    }

    /// <summary>
    /// Same idea, but for a codec already present in the default <c>Accept-Encoding</c> list, so the
    /// duplicate check has to normalize before comparing.
    /// </summary>
    private sealed class PaddedTokenGZipCompressor : IClickHouseCompressor
    {
        public string ContentEncoding => " gzip ";

        public Stream Compress(Stream destination, bool leaveOpen) => GZipCompressor.Default.Compress(destination, leaveOpen);

        public Stream Decompress(Stream source, bool leaveOpen) => GZipCompressor.Default.Decompress(source, leaveOpen);
    }

    /// <summary>
    /// A pass-through "codec" whose <c>Decompress</c> hands back an observable stream, so disposal of the
    /// wrapper the driver inserted can be asserted directly rather than inferred.
    /// </summary>
    private sealed class TrackingCompressor : IClickHouseCompressor
    {
        public string ContentEncoding => "x-tracking";

        public CountingStream Last { get; private set; }

        public Stream Compress(Stream destination, bool leaveOpen) => destination;

        public Stream Decompress(Stream source, bool leaveOpen) => Last = new CountingStream(source);
    }

    /// <summary>
    /// A non-seekable, forward-only source, so <see cref="StreamContent"/> over it behaves like the
    /// unbuffered content of a real streamed response instead of a re-readable buffer.
    /// </summary>
    private sealed class ForwardOnlyStream : Stream
    {
        private readonly byte[] bytes;
        private int offset;

        public ForwardOnlyStream(byte[] bytes) => this.bytes = bytes;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int bufferOffset, int count)
        {
            var take = Math.Min(count, bytes.Length - offset);
            Array.Copy(bytes, offset, buffer, bufferOffset, take);
            offset += take;
            return take;
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class CountingStream : Stream
    {
        private readonly Stream inner;

        public CountingStream(Stream inner) => this.inner = inner;

        public int DisposeCount { get; private set; }

        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => inner.Flush();

        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            DisposeCount++;

            // leaveOpen semantics: the inner (transport) stream is owned by the HTTP response.
            base.Dispose(disposing);
        }
    }
}

/// <summary>
/// End-to-end response compression against a real ClickHouse server: the server encodes, the driver
/// decodes. Values are compared against the same query read without compression, so the expectations come
/// from the server rather than from the code under test.
/// </summary>
[TestFixture]
public class ResponseDecompressionIntegrationTests : AbstractConnectionTestFixture
{
    private static ClickHouseClient CreateLz4Client()
        => new(new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            ResponseCompressor = Lz4Compressor.Default,
        });

    [Test]
    public async Task ExecuteReaderAsync_WithLz4ResponseCompressor_ReadsValuesIdenticalToUncompressed()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int64, s String, d DateTime64(3)) ENGINE Memory");
        await client.ExecuteNonQueryAsync(
            $"INSERT INTO {table} SELECT number, concat('row-', toString(number)), toDateTime64('2024-01-01 00:00:00.123', 3) + number FROM numbers(5000)");

        var expected = await ReadRowsAsync(client, table);

        using var lz4Client = CreateLz4Client();
        var actual = await ReadRowsAsync(lz4Client, table);

        Assert.That(actual, Is.EqualTo(expected));
    }

    /// <summary>
    /// A payload well over 1 MB, so the LZ4 body the server sends is split into SEVERAL blocks and the
    /// decoder has to stream across block boundaries instead of decoding one self-contained block. Do not
    /// shrink this: a small body fits in a single block and would not exercise streaming decode at all.
    /// The test first proves on the wire that the response really is a multi-block LZ4 body — otherwise a
    /// silently-uncompressed response would let a broken decoder pass.
    /// </summary>
    [Test]
    public async Task ExecuteReaderAsync_WithMultiMegabyteLz4Response_DecodesEveryRowAcrossBlockBoundaries()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int64, payload String) ENGINE Memory");

        // 20,000 rows x ~70 bytes of non-repeating hex ~= 1.4 MB of plaintext.
        await client.ExecuteNonQueryAsync(
            $"INSERT INTO {table} SELECT number, concat(toString(number), '-', hex(MD5(toString(number))), '-', hex(MD5(toString(number + 1)))) FROM numbers(20000)");

        var plaintextBytes = Convert.ToInt64(
            await client.ExecuteScalarAsync($"SELECT sum(length(payload) + 1) FROM {table}"),
            System.Globalization.CultureInfo.InvariantCulture);
        Assert.That(plaintextBytes, Is.GreaterThan(1024 * 1024), "the payload must exceed 1 MiB");

        using var lz4Client = CreateLz4Client();

        using (var rawLz4 = await lz4Client.ExecuteRawResultAsync($"SELECT payload FROM {table} ORDER BY id FORMAT TSV"))
        {
            Assert.That(rawLz4.ContentEncoding, Is.EqualTo("lz4"), "the server must have answered with lz4");

            var frame = await rawLz4.ReadAsByteArrayAsync();
            Assert.That(CountLz4Blocks(frame), Is.GreaterThan(1), "the body must span multiple LZ4 blocks");
        }

        var expected = await ReadPayloadsAsync(client, table);
        var actual = await ReadPayloadsAsync(lz4Client, table);

        Assert.Multiple(() =>
        {
            Assert.That(actual, Has.Count.EqualTo(20000));
            Assert.That(actual, Is.EqualTo(expected));
        });
    }

    /// <summary>
    /// Walks the LZ4 frame format — magic(4) FLG(1) BD(1) [contentSize(8)] [dictId(4)] HC(1) followed by
    /// length-prefixed blocks terminated by a zero length — and returns the number of data blocks.
    /// </summary>
    private static int CountLz4Blocks(byte[] frame)
    {
        Assert.That(frame[..4], Is.EqualTo(new byte[] { 0x04, 0x22, 0x4D, 0x18 }), "expected an LZ4 frame magic");

        var flg = frame[4];
        var offset = 6;
        if ((flg & 0x08) != 0)
            offset += 8; // content size present
        if ((flg & 0x01) != 0)
            offset += 4; // dictionary id present
        offset += 1;     // header checksum

        var blocks = 0;
        while (offset + 4 <= frame.Length)
        {
            var size = BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(offset));
            offset += 4;
            if (size == 0)
                break; // end mark

            offset += (int)(size & 0x7FFF_FFFF);
            blocks++;
        }

        return blocks;
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_AgainstRealServer_YieldsTheSameBytesAsAnUncompressedExport()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int64) ENGINE Memory");
        await client.ExecuteNonQueryAsync($"INSERT INTO {table} SELECT number FROM numbers(3000)");

        using var plainResult = await client.ExecuteRawResultAsync($"SELECT id FROM {table} ORDER BY id FORMAT TSV");
        var expected = await plainResult.ReadAsByteArrayAsync();

        using var lz4Client = CreateLz4Client();
        using var lz4Result = await lz4Client.ExecuteRawResultAsync($"SELECT id FROM {table} ORDER BY id FORMAT TSV");

        // The server may still answer uncompressed for a tiny body; when it does compress, it must be lz4.
        if (lz4Result.ContentEncoding != null)
            Assert.That(lz4Result.ContentEncoding, Is.EqualTo("lz4"));

        using var decompressed = await lz4Result.ReadDecompressedStreamAsync();
        using var buffer = new MemoryStream();
        await decompressed.CopyToAsync(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithLz4ResponseCompressor_ReturnsTheServerValue()
    {
        using var lz4Client = CreateLz4Client();

        var value = await lz4Client.ExecuteScalarAsync("SELECT count() FROM numbers(1234)");

        Assert.That(Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture), Is.EqualTo(1234));
    }

    [Test]
    public async Task ConnectionString_WithResponseCompressionLz4_ReadsThroughTheAdoLayer()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.ResponseCompression = "lz4";

        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int64) ENGINE Memory");
        await client.ExecuteNonQueryAsync($"INSERT INTO {table} SELECT number FROM numbers(4000)");

        using var lz4Connection = new ClickHouseConnection(builder.ToString());
        lz4Connection.Open();
        using var command = lz4Connection.CreateCommand();
        command.CommandText = $"SELECT sum(id) FROM {table}";

        Assert.That(
            Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture),
            Is.EqualTo(4000L * 3999 / 2));
    }

    private static async Task<List<string>> ReadRowsAsync(ClickHouseClient source, string table)
    {
        using var reader = await source.ExecuteReaderAsync($"SELECT id, s, d FROM {table} ORDER BY id");
        var rows = new List<string>();
        while (reader.Read())
        {
            rows.Add(string.Create(
                System.Globalization.CultureInfo.InvariantCulture,
                $"{reader.GetInt64(0)}|{reader.GetString(1)}|{reader.GetDateTime(2):O}"));
        }

        return rows;
    }

    private static async Task<List<string>> ReadPayloadsAsync(ClickHouseClient source, string table)
    {
        using var reader = await source.ExecuteReaderAsync($"SELECT payload FROM {table} ORDER BY id");
        var rows = new List<string>();
        while (reader.Read())
            rows.Add(reader.GetString(0));

        return rows;
    }
}
