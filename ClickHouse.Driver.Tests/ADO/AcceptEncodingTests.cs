using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
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

    [Test]
    public async Task HandleError_WithGzipEncodedBody_DecompressesIntoExceptionMessage()
    {
        var serverMessage = "Code: 62. DB::Exception: Syntax error: failed at position 1";
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = BuildGzipContent(serverMessage),
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
    public void HandleError_WithUnsupportedContentEncoding_ReturnsPlaceholderMessageWithoutThrowing()
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

    private static ByteArrayContent BuildGzipContent(string text)
    {
        using var buffer = new MemoryStream();
        using (var gzip = new GZipStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
        {
            var bytes = Encoding.UTF8.GetBytes(text);
            gzip.Write(bytes, 0, bytes.Length);
        }

        var content = new ByteArrayContent(buffer.ToArray());
        content.Headers.ContentEncoding.Add("gzip");
        return content;
    }
}
