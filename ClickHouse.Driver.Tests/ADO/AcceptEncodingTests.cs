using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

[TestFixture]
public class AcceptEncodingTests
{
    private static HttpResponseMessage CreateFakeSuccessResponse()
    {
        return new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(string.Empty),
        };
    }

    private static (ClickHouseClient client, TrackingHandler handler) CreateClient(bool useCompression = false)
    {
        var trackingHandler = new TrackingHandler(CreateFakeSuccessResponse());
        var httpClient = new HttpClient(trackingHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
            UseCompression = useCompression,
        };
        return (new ClickHouseClient(settings), trackingHandler);
    }

    [Test]
    public async Task QueryOptionsAcceptEncoding_WhenSet_ReplacesDefaultAcceptEncodingHeader()
    {
        var (client, handler) = CreateClient(useCompression: true);

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = "zstd" });

        var request = handler.Requests.Single();
        var encodings = request.Headers.AcceptEncoding.Select(e => e.Value).ToArray();
        Assert.That(encodings, Is.EqualTo(new[] { "zstd" }));
    }

    [Test]
    public async Task QueryOptionsAcceptEncoding_WhenSet_ForcesEnableHttpCompressionEvenWithoutClientCompression()
    {
        var (client, handler) = CreateClient(useCompression: false);

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = "gzip" });

        var request = handler.Requests.Single();
        Assert.That(request.RequestUri.Query, Does.Contain("enable_http_compression=true"));
    }

    [Test]
    public async Task QueryOptionsAcceptEncoding_WhenNullOrEmpty_PreservesDefaultAcceptEncodingHeader()
    {
        var (client, handler) = CreateClient(useCompression: true);

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = null });

        var request = handler.Requests.Single();
        var encodings = request.Headers.AcceptEncoding.Select(e => e.Value).ToArray();
        Assert.That(encodings, Is.EquivalentTo(new[] { "gzip", "deflate" }));
    }

    [Test]
    public async Task QueryOptionsAcceptEncoding_WithMultipleValues_ParsesEachEntryWithQualityWeights()
    {
        var (client, handler) = CreateClient(useCompression: true);

        await client.ExecuteNonQueryAsync(
            "SELECT 1",
            options: new QueryOptions { AcceptEncoding = "zstd, gzip;q=0.5" });

        var request = handler.Requests.Single();
        var encodings = request.Headers.AcceptEncoding.ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(encodings.Select(e => e.Value).ToArray(), Is.EqualTo(new[] { "zstd", "gzip" }));
            Assert.That(encodings[1].Quality, Is.EqualTo(0.5));
        });
    }

    [Test]
    public async Task CommandAcceptEncoding_WhenSet_FlowsThroughToHttpHeader()
    {
        var (client, handler) = CreateClient(useCompression: false);
        using var connection = client.CreateConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        command.AcceptEncoding = "br";

        await command.ExecuteNonQueryAsync(CancellationToken.None);

        var request = handler.Requests.Single();
        var encodings = request.Headers.AcceptEncoding.Select(e => e.Value).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(encodings, Is.EqualTo(new[] { "br" }));
            Assert.That(request.RequestUri.Query, Does.Contain("enable_http_compression=true"));
        });
    }

    [TestCase("gzip")]
    [TestCase("deflate")]
    [TestCase("br")]
    [TestCase("brotli")]
    public async Task HandleError_WithSupportedContentEncoding_DecompressesIntoExceptionMessage(string contentEncoding)
    {
        var serverMessage = "Code: 62. DB::Exception: Syntax error: failed at position 1";
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = BuildCompressedContent(serverMessage, contentEncoding),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT bad_syntax",
                options: new QueryOptions { AcceptEncoding = "gzip" }));

        Assert.That(ex.Message, Does.Contain("Syntax error"));
    }

    [Test]
    public async Task HandleError_WithCompressedNonUtf8Body_UsesContentTypeCharset()
    {
        var serverMessage = "Code: 62. DB::Exception: Неверный синтаксис";
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = BuildCompressedContent(serverMessage, "gzip", Encoding.Unicode, "utf-16"),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT bad_syntax",
                options: new QueryOptions { AcceptEncoding = "gzip" }));

        Assert.That(ex.Message, Does.Contain("Неверный синтаксис"));
    }

    [Test]
    public async Task HandleError_WithCompressedBodyAndInvalidCharset_FallsBackToUtf8()
    {
        var serverMessage = "Code: 62. DB::Exception: Syntax error";
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = BuildCompressedContent(serverMessage, "gzip", charset: "invalid-charset"),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT bad_syntax",
                options: new QueryOptions { AcceptEncoding = "gzip" }));

        Assert.That(ex.Message, Does.Contain("Syntax error"));
    }

    [Test]
    public void HandleError_WithUnsupportedContentEncoding_ThrowsExceptionWithPlaceholderMessage()
    {
        var fakeHandler = new TrackingHandler(_ =>
        {
            var content = new ByteArrayContent(new byte[] { 0x28, 0xB5, 0x2F, 0xFD }); // arbitrary zstd-looking bytes
            content.Headers.ContentEncoding.Add("zstd");
            return new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                ReasonPhrase = "Internal Server Error",
                Content = content,
            };
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1",
                options: new QueryOptions { AcceptEncoding = "zstd" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("unsupported Content-Encoding: zstd"));
            Assert.That(ex.Message, Does.Contain("system.query_log"));
        });
    }

    [Test]
    public void HandleError_WithUnsupportedContentEncoding_DrainsBodyBeforeReturningPlaceholderMessage()
    {
        var body = new byte[] { 0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x01 };
        var trackingStream = new TrackingReadStream(body);
        var fakeHandler = new TrackingHandler(_ =>
        {
            var content = new StreamContent(trackingStream);
            content.Headers.ContentEncoding.Add("zstd");
            return new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                ReasonPhrase = "Internal Server Error",
                Content = content,
            };
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1",
                options: new QueryOptions { AcceptEncoding = "zstd" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("unsupported Content-Encoding: zstd"));
            Assert.That(trackingStream.BytesRead, Is.EqualTo(body.Length));
            Assert.That(trackingStream.IsDisposed, Is.True);
        });
    }

    [Test]
    public void HandleError_WithoutContentEncoding_ReadsBodyVerbatim()
    {
        var serverMessage = "Code: 62. DB::Exception: plain text";
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = new StringContent(serverMessage, Encoding.UTF8),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.That(ex.Message, Does.Contain("plain text"));
    }

    [Test]
    public void ContentEncoding_WhenHeaderPresent_ReturnsHeaderValue()
    {
        var content = new StringContent("body");
        content.Headers.ContentEncoding.Add("zstd");
        using var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = content };
        using var raw = new ClickHouseRawResult(response);

        Assert.That(raw.ContentEncoding, Is.EqualTo("zstd"));
    }

    [Test]
    public void ContentEncoding_WhenHeaderAbsent_ReturnsNull()
    {
        using var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("body"),
        };
        using var raw = new ClickHouseRawResult(response);

        Assert.That(raw.ContentEncoding, Is.Null);
    }

    [Test]
    public void ContentEncoding_WhenIdentity_NormalizedToNull()
    {
        var content = new StringContent("body");
        content.Headers.ContentEncoding.Add("identity");
        using var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = content };
        using var raw = new ClickHouseRawResult(response);

        Assert.That(raw.ContentEncoding, Is.Null);
    }

    [Test]
    public async Task AcceptEncodingGzip_AgainstRealServer_ResponseIsActuallyGzipCompressed()
    {
        using var rawHandler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.None };
        using var rawHttpClient = new HttpClient(rawHandler);
        var settings = new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            HttpClient = rawHttpClient,
        };
        using var client = new ClickHouseClient(settings);

        using var result = await client.ExecuteRawResultAsync(
            "SELECT number FROM numbers(5) FORMAT TSV",
            options: new QueryOptions { AcceptEncoding = "gzip" });

        Assert.That(result.ContentEncoding, Is.EqualTo("gzip"));

        await using var compressed = await result.ReadAsStreamAsync();
        await using var decompressor = new GZipStream(compressed, CompressionMode.Decompress);
        using var reader = new StreamReader(decompressor, Encoding.UTF8);
        var body = await reader.ReadToEndAsync();

        Assert.That(body, Is.EqualTo("0\n1\n2\n3\n4\n"));
    }

    [Test]
    public void AcceptEncodingZstd_AgainstRealServer_ErrorBodyYieldsUnsupportedCodecPlaceholder()
    {
        using var rawHandler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.None };
        using var rawHttpClient = new HttpClient(rawHandler);
        var settings = new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            HttpClient = rawHttpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync(
                "SELECT * FROM no_such_table_for_acceptencoding_test",
                options: new QueryOptions { AcceptEncoding = "zstd" }));

        Assert.That(ex.Message, Does.Contain("unsupported Content-Encoding: zstd"));
    }

    private static ByteArrayContent BuildCompressedContent(
        string text,
        string contentEncoding,
        Encoding textEncoding = null,
        string charset = null)
    {
        textEncoding ??= Encoding.UTF8;
        using var buffer = new MemoryStream();
        using (var compressed = CreateCompressionStream(buffer, contentEncoding))
        {
            var bytes = textEncoding.GetBytes(text);
            compressed.Write(bytes, 0, bytes.Length);
        }

        var content = new ByteArrayContent(buffer.ToArray());
        if (charset != null)
        {
            content.Headers.ContentType = new MediaTypeHeaderValue("text/plain")
            {
                CharSet = charset,
            };
        }

        content.Headers.ContentEncoding.Add(contentEncoding);
        return content;
    }

    private static Stream CreateCompressionStream(Stream stream, string contentEncoding)
    {
        return contentEncoding switch
        {
            "gzip" => new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true),
            "deflate" => new DeflateStream(stream, CompressionLevel.Fastest, leaveOpen: true),
            "br" or "brotli" => new BrotliStream(stream, CompressionLevel.Fastest, leaveOpen: true),
            _ => throw new ArgumentOutOfRangeException(nameof(contentEncoding), contentEncoding, null),
        };
    }

    private sealed class TrackingReadStream : Stream
    {
        private readonly MemoryStream inner;

        public TrackingReadStream(byte[] bytes)
        {
            inner = new MemoryStream(bytes);
        }

        public int BytesRead { get; private set; }

        public bool IsDisposed { get; private set; }

        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => inner.CanSeek;

        public override bool CanWrite => false;

        public override long Length => inner.Length;

        public override long Position
        {
            get => inner.Position;
            set => inner.Position = value;
        }

        public override void Flush() => inner.Flush();

        public override int Read(byte[] buffer, int offset, int count)
        {
            var bytesRead = inner.Read(buffer, offset, count);
            BytesRead += bytesRead;
            return bytesRead;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var bytesRead = inner.Read(buffer.Span);
            BytesRead += bytesRead;
            return ValueTask.FromResult(bytesRead);
        }

        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

        public override void SetLength(long value) => inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            IsDisposed = true;
            inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
