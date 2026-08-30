using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
namespace ClickHouse.Driver.Tests;

public class ClickHouseClientTests : AbstractConnectionTestFixture
{
    [Test]
    public void InsertRawStreamAsync_WithNullTable_ShouldThrowArgumentException()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("1,2,3"));
        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.InsertRawStreamAsync(table: null, stream: stream, format: "CSV"));
        Assert.That(ex.ParamName, Is.EqualTo("table"));
    }

    [Test]
    public void InsertRawStreamAsync_WithEmptyTable_ShouldThrowArgumentException()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("1,2,3"));
        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.InsertRawStreamAsync(table: "", stream: stream, format: "CSV"));
        Assert.That(ex.ParamName, Is.EqualTo("table"));
    }

    [Test]
    public void InsertRawStreamAsync_WithNullStream_ShouldThrowArgumentNullException()
    {
        var ex = Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await client.InsertRawStreamAsync(table: "test", stream: null, format: "CSV"));
        Assert.That(ex.ParamName, Is.EqualTo("stream"));
    }

    [Test]
    public void InsertRawStreamAsync_WithNullFormat_ShouldThrowArgumentException()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("1,2,3"));
        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.InsertRawStreamAsync(table: "test", stream: stream, format: null));
        Assert.That(ex.ParamName, Is.EqualTo("format"));
    }

    [Test]
    public void InsertRawStreamAsync_WithEmptyFormat_ShouldThrowArgumentException()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("1,2,3"));
        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.InsertRawStreamAsync(table: "test", stream: stream, format: ""));
        Assert.That(ex.ParamName, Is.EqualTo("format"));
    }

    [Test]
    public async Task PingAsync_ReturnsTrue_WhenServerResponds()
    {
        var trackingHandler = new TrackingHandler(request =>
        {
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("Ok.\n") };
        });
        using var httpClient = new HttpClient(trackingHandler);
        var settings = new ClickHouseClientSettings { Host = "localhost", HttpClient = httpClient };
        using var client = new ClickHouseClient(settings);

        var result = await client.PingAsync();

        Assert.That(result, Is.True);
        Assert.That(trackingHandler.Requests.Last().RequestUri.PathAndQuery, Is.EqualTo("/ping"));
    }

    [TestCase("/ch", ExpectedResult = "http://localhost:8123/ch/ping")]
    [TestCase("ch", ExpectedResult = "http://localhost:8123/ch/ping")]
    [TestCase("/ch/", ExpectedResult = "http://localhost:8123/ch/ping")]
    [TestCase("/proxy/ch", ExpectedResult = "http://localhost:8123/proxy/ch/ping")]
    [TestCase("//ch", ExpectedResult = "http://localhost:8123//ch/ping")]
    public async Task<string> PingAsync_WithConfiguredPath_PreservesPathPrefix(string path)
    {
        var trackingHandler = new TrackingHandler(request =>
        {
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("Ok.\n") };
        });
        using var httpClient = new HttpClient(trackingHandler);
        // Protocol and Port are set explicitly so the expected absolute URI does not
        // depend on the client defaults. The assertion covers the whole absolute URI
        // (not only the path) to pin that a path such as "//ch" never becomes an authority.
        var settings = new ClickHouseClientSettings { Protocol = "http", Host = "localhost", Port = 8123, Path = path, HttpClient = httpClient };
        using var client = new ClickHouseClient(settings);

        var result = await client.PingAsync();

        Assert.That(result, Is.True);
        return trackingHandler.Requests.Last().RequestUri.AbsoluteUri;
    }

    [Test]
    public async Task PingAsync_ReturnsFalse_WhenServerReturnsError()
    {
        var trackingHandler = new TrackingHandler(request =>
        {
            return new HttpResponseMessage(HttpStatusCode.InternalServerError);
        });
        using var httpClient = new HttpClient(trackingHandler);
        var settings = new ClickHouseClientSettings { Host = "localhost", HttpClient = httpClient };
        using var client = new ClickHouseClient(settings);

        var result = await client.PingAsync();

        Assert.That(result, Is.False);
    }

    [Test]
    public async Task PingAsync_WithRealServer_ReturnsTrue()
    {
        var result = await client.PingAsync();

        Assert.That(result, Is.True);
    }
    
    [Test]
    public void ShouldExcludePasswordFromRedactedConnectionString()
    {
        const string MOCK = "verysecurepassword";
        var settings = new ClickHouseClientSettings()
        {
            Password = MOCK,
        };
        using var client = new ClickHouseClient(settings);
        Assert.Multiple(() =>
        {
            Assert.That(client.RedactedConnectionString, Is.Not.Contains($"Password={MOCK}"));
        });
    }

    [Test]
    public async Task Constructor_WithHttpClient_ShouldUseProvidedHttpClient()
    {
        var trackingHandler = new TrackingHandler(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        });
        using var httpClient = new HttpClient(trackingHandler);

        var connectionString = TestUtilities.GetConnectionStringBuilder().ToString();
        using var client = new ClickHouseClient(connectionString, httpClient);

        await client.ExecuteScalarAsync("SELECT 1");

        Assert.That(trackingHandler.RequestCount, Is.GreaterThan(0), "Provided HttpClient should have been used");
    }

    [Test]
    public async Task Constructor_WithHttpClientFactory_ShouldUseProvidedFactory()
    {
        var factory = new TestHttpClientFactory();

        var connectionString = TestUtilities.GetConnectionStringBuilder().ToString();
        using var client = new ClickHouseClient(connectionString, factory, "test-client");

        Assert.That(factory.CreateClientCallCount, Is.EqualTo(0));

        await client.ExecuteScalarAsync("SELECT 1");

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreateClientCallCount, Is.GreaterThan(0), "Provided HttpClientFactory was not used");
            Assert.That(factory.LastRequestedClientName, Is.EqualTo("test-client"));
        });
    }

    [Test]
    public async Task Constructor_WithHttpClientFactoryDefaultName_ShouldUseEmptyName()
    {
        var factory = new TestHttpClientFactory();

        var connectionString = TestUtilities.GetConnectionStringBuilder().ToString();
        using var client = new ClickHouseClient(connectionString, factory);

        await client.ExecuteScalarAsync("SELECT 1");

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreateClientCallCount, Is.GreaterThan(0), "Provided HttpClientFactory was not used");
            Assert.That(factory.LastRequestedClientName, Is.EqualTo(""));
        });
    }
}
