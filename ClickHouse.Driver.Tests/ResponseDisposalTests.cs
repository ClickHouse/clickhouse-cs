using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

/// <summary>
/// Verifies which code paths own the <see cref="HttpResponseMessage"/> produced by a query, and that
/// the owning path releases it. Disposal is observed by replacing the response content with a
/// <see cref="HttpContent"/> that records its own disposal (disposing a response disposes its
/// content), so these tests run against a real server.
/// </summary>
[TestFixture]
public class ResponseDisposalTests
{
    /// <summary>
    /// Builds a client over a tracking handler. The <see cref="HttpClient"/> is handed back because
    /// <see cref="ClickHouseClient"/> deliberately does not own a caller-provided instance (it is
    /// wrapped in a <c>CannedHttpClientFactory</c> and never added to the client's disposables), so
    /// the test has to dispose it — otherwise every test would leak a handler and its sockets.
    /// </summary>
    private static (ClickHouseClient client, HttpClient httpClient, ResponseContentTrackingHandler handler) CreateClient()
    {
        var handler = new ResponseContentTrackingHandler(new HttpClientHandler
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
        var (client, httpClient, handler) = CreateClient();
        using (httpClient)
        using (client)
        {
            await client.ExecuteNonQueryAsync("SELECT 1");

            Assert.That(handler.Responses.Single().IsDisposed, Is.True);
        }
    }

    [Test]
    public void ExecuteNonQueryAsync_WhenServerReturnsError_DisposesHttpResponse()
    {
        var (client, httpClient, handler) = CreateClient();
        using (httpClient)
        using (client)
        {
            // Never created, so the query is guaranteed to fail with UNKNOWN_TABLE regardless of what
            // the concurrently executing suites of the other target frameworks are doing.
            var missingTable = TestUtilities.CreateTableName();

            Assert.ThrowsAsync<ClickHouseServerException>(
                () => client.ExecuteNonQueryAsync($"SELECT * FROM {missingTable}"));

            Assert.That(handler.Responses.Single().IsDisposed, Is.True);
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WhenBatchIsSent_DisposesHttpResponse()
    {
        var (client, httpClient, handler) = CreateClient();
        using (httpClient)
        using (client)
        {
            var targetTable = TestUtilities.CreateTableName();
            try
            {
                await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (value Int32) ENGINE Memory");
                await client.InsertBinaryAsync(targetTable, new[] { "value" }, new[] { new object[] { 1 } });

                Assert.That(handler.Responses, Is.Not.Empty);
                Assert.That(handler.Responses.Select(c => c.IsDisposed), Is.All.True);
            }
            finally
            {
                await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {targetTable}");
            }
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WhenPocoBatchIsSent_DisposesHttpResponse()
    {
        var (client, httpClient, handler) = CreateClient();
        using (httpClient)
        using (client)
        {
            client.RegisterBinaryInsertType<PocoRow>();
            var targetTable = TestUtilities.CreateTableName();
            try
            {
                await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (Value Int32) ENGINE Memory");
                await client.InsertBinaryAsync(targetTable, new[] { new PocoRow { Value = 1 } });

                Assert.That(handler.Responses, Is.Not.Empty);
                Assert.That(handler.Responses.Select(c => c.IsDisposed), Is.All.True);
            }
            finally
            {
                await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {targetTable}");
            }
        }
    }

    [Test]
    public void InsertRawStreamAsync_WhenServerReturnsError_DisposesHttpResponse()
    {
        var (client, httpClient, handler) = CreateClient();
        using (httpClient)
        using (client)
        {
            using var payload = new MemoryStream(Encoding.UTF8.GetBytes("1\n"));

            // Never created, so the insert is guaranteed to fail with UNKNOWN_TABLE regardless of what
            // the concurrently executing suites of the other target frameworks are doing.
            var missingTable = TestUtilities.CreateTableName();

            Assert.ThrowsAsync<ClickHouseServerException>(
                () => client.InsertRawStreamAsync(missingTable, payload, "TSV"));

            Assert.That(handler.Responses.Single().IsDisposed, Is.True);
        }
    }

    [Test]
    public async Task ExecuteReaderAsync_WhileReaderIsOpen_KeepsHttpResponseAliveUntilReaderIsDisposed()
    {
        var (client, httpClient, handler) = CreateClient();
        using (httpClient)
        using (client)
        {
            var reader = await client.ExecuteReaderAsync("SELECT number FROM system.numbers LIMIT 10");
            try
            {
                Assert.That(handler.Responses.Single().IsDisposed, Is.False);
                Assert.That(reader.Read(), Is.True);
            }
            finally
            {
                reader.Dispose();
            }

            Assert.That(handler.Responses.Single().IsDisposed, Is.True);
        }
    }

    [Test]
    public async Task ExecuteRawResultAsync_WhileResultIsOpen_KeepsHttpResponseAliveUntilResultIsDisposed()
    {
        var (client, httpClient, handler) = CreateClient();
        using (httpClient)
        using (client)
        {
            var rawResult = await client.ExecuteRawResultAsync("SELECT 1 FORMAT CSV");
            try
            {
                Assert.That(handler.Responses.Single().IsDisposed, Is.False);
                Assert.That(await rawResult.ReadAsStringAsync(), Does.Contain("1"));
            }
            finally
            {
                rawResult.Dispose();
            }

            Assert.That(handler.Responses.Single().IsDisposed, Is.True);
        }
    }

    /// <summary>
    /// Replaces every response's content with a <see cref="DisposalTrackingContent"/>, so disposal of
    /// the response (which disposes its content) becomes observable from the test.
    /// </summary>
    private sealed class ResponseContentTrackingHandler : DelegatingHandler
    {
        private readonly ConcurrentQueue<DisposalTrackingContent> responses = new();

        public ResponseContentTrackingHandler(HttpMessageHandler innerHandler) : base(innerHandler) { }

        public IEnumerable<DisposalTrackingContent> Responses => responses.ToArray();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);

            var trackingContent = new DisposalTrackingContent(
                await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false));
            foreach (var header in response.Content.Headers)
            {
                trackingContent.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }

            responses.Enqueue(trackingContent);
            response.Content = trackingContent;
            return response;
        }
    }

    private sealed class PocoRow
    {
        public int Value { get; set; }
    }

    private sealed class DisposalTrackingContent : StreamContent
    {
        public DisposalTrackingContent(Stream content) : base(content) { }

        public bool IsDisposed { get; private set; }

        protected override void Dispose(bool disposing)
        {
            IsDisposed = true;
            base.Dispose(disposing);
        }
    }
}
