using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;

namespace ClickHouse.Driver.Tests;

/// <summary>
/// Verifies which code paths own the <see cref="HttpResponseMessage"/> produced by a query, and that
/// the owning path releases it. Disposal is observed with <see cref="ResponseDisposalTrackingHandler"/>,
/// so these tests run against a real server.
/// </summary>
/// <remarks>
/// The inherited fixture client (which has its own <see cref="HttpClient"/>) does all setup and
/// cleanup, so the tracking client only ever issues the requests of the operation under test — which
/// is what makes the exact response counts below meaningful.
/// </remarks>
public class ResponseDisposalTests : AbstractConnectionTestFixture
{
    /// <summary>
    /// Builds a client over a tracking handler. The <see cref="HttpClient"/> is handed back because
    /// <see cref="ClickHouseClient"/> deliberately does not own a caller-provided instance (it is
    /// wrapped in a <c>CannedHttpClientFactory</c> and never added to the client's disposables), so
    /// the test has to dispose it — otherwise every test would leak a handler and its sockets.
    /// </summary>
    private static (ClickHouseClient client, HttpClient httpClient, ResponseDisposalTrackingHandler handler) CreateTrackingClient()
    {
        var handler = new ResponseDisposalTrackingHandler(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        });
        var httpClient = new HttpClient(handler);
        var settings = new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            HttpClient = httpClient,
        };
        return (new ClickHouseClient(settings), httpClient, handler);
    }

    [Test]
    public async Task ExecuteNonQueryAsync_WhenQuerySucceeds_DisposesHttpResponse()
    {
        var (trackingClient, httpClient, handler) = CreateTrackingClient();
        using (httpClient)
        using (trackingClient)
        {
            await trackingClient.ExecuteNonQueryAsync("SELECT 1");

            Assert.That(handler.Responses, Has.Count.EqualTo(1));
            Assert.That(handler.Responses[0].IsDisposed, Is.True);
        }
    }

    [Test]
    public void ExecuteNonQueryAsync_WhenServerReturnsError_DisposesHttpResponse()
    {
        var (trackingClient, httpClient, handler) = CreateTrackingClient();
        using (httpClient)
        using (trackingClient)
        {
            // Never created, so the query is guaranteed to fail with UNKNOWN_TABLE regardless of what
            // the concurrently executing suites of the other target frameworks are doing. Deliberately
            // not registered with the fixture for cleanup, since it never exists.
            var missingTable = TestUtilities.CreateTableName();

            Assert.ThrowsAsync<ClickHouseServerException>(
                () => trackingClient.ExecuteNonQueryAsync($"SELECT * FROM {missingTable}"));

            Assert.That(handler.Responses, Has.Count.EqualTo(1));
            Assert.That(handler.Responses[0].IsDisposed, Is.True);
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WhenBatchIsSent_DisposesHttpResponse()
    {
        var targetTable = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (value Int32) ENGINE Memory");

        var (trackingClient, httpClient, handler) = CreateTrackingClient();
        using (httpClient)
        using (trackingClient)
        {
            await trackingClient.InsertBinaryAsync(targetTable, new[] { "value" }, new[] { new object[] { 1 } });

            // Fetching the target's schema and sending the batch, one request each.
            Assert.That(handler.Responses, Has.Count.EqualTo(2));
            Assert.That(handler.Responses.Select(c => c.IsDisposed), Is.All.True);
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WhenPocoBatchIsSent_DisposesHttpResponse()
    {
        var targetTable = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (Value Int32) ENGINE Memory");

        var (trackingClient, httpClient, handler) = CreateTrackingClient();
        using (httpClient)
        using (trackingClient)
        {
            trackingClient.RegisterBinaryInsertType<PocoRow>();

            await trackingClient.InsertBinaryAsync(targetTable, new[] { new PocoRow { Value = 1 } });

            // Fetching the target's schema and sending the batch, one request each.
            Assert.That(handler.Responses, Has.Count.EqualTo(2));
            Assert.That(handler.Responses.Select(c => c.IsDisposed), Is.All.True);
        }
    }

    [Test]
    public void InsertRawStreamAsync_WhenServerReturnsError_DisposesHttpResponse()
    {
        var (trackingClient, httpClient, handler) = CreateTrackingClient();
        using (httpClient)
        using (trackingClient)
        {
            using var payload = new MemoryStream(Encoding.UTF8.GetBytes("1\n"));

            // Never created, so the insert is guaranteed to fail with UNKNOWN_TABLE regardless of what
            // the concurrently executing suites of the other target frameworks are doing. Deliberately
            // not registered with the fixture for cleanup, since it never exists.
            var missingTable = TestUtilities.CreateTableName();

            Assert.ThrowsAsync<ClickHouseServerException>(
                () => trackingClient.InsertRawStreamAsync(missingTable, payload, "TSV"));

            Assert.That(handler.Responses, Has.Count.EqualTo(1));
            Assert.That(handler.Responses[0].IsDisposed, Is.True);
        }
    }

    [Test]
    public async Task ExecuteReaderAsync_WhileReaderIsOpen_KeepsHttpResponseAliveUntilReaderIsDisposed()
    {
        var (trackingClient, httpClient, handler) = CreateTrackingClient();
        using (httpClient)
        using (trackingClient)
        {
            var reader = await trackingClient.ExecuteReaderAsync("SELECT number FROM system.numbers LIMIT 10");
            try
            {
                Assert.That(handler.Responses, Has.Count.EqualTo(1));
                Assert.That(handler.Responses[0].IsDisposed, Is.False);
                Assert.That(reader.Read(), Is.True);
            }
            finally
            {
                reader.Dispose();
            }

            Assert.That(handler.Responses, Has.Count.EqualTo(1));
            Assert.That(handler.Responses[0].IsDisposed, Is.True);
        }
    }

    [Test]
    public async Task ExecuteRawResultAsync_WhileResultIsOpen_KeepsHttpResponseAliveUntilResultIsDisposed()
    {
        var (trackingClient, httpClient, handler) = CreateTrackingClient();
        using (httpClient)
        using (trackingClient)
        {
            var rawResult = await trackingClient.ExecuteRawResultAsync("SELECT 1 FORMAT CSV");
            try
            {
                Assert.That(handler.Responses, Has.Count.EqualTo(1));
                Assert.That(handler.Responses[0].IsDisposed, Is.False);
                Assert.That(await rawResult.ReadAsStringAsync(), Does.Contain("1"));
            }
            finally
            {
                rawResult.Dispose();
            }

            Assert.That(handler.Responses, Has.Count.EqualTo(1));
            Assert.That(handler.Responses[0].IsDisposed, Is.True);
        }
    }

    private sealed class PocoRow
    {
        public int Value { get; set; }
    }
}
