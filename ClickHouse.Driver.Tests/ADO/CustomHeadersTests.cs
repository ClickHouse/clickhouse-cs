using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
using ClickHouse.Driver.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tests.ADO;

[TestFixture]
public class CustomHeadersTests
{
    private static HttpResponseMessage CreateFakeVersionResponse()
    {
        return new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("25.12.1.217\tEurope/Amsterdam"),
        };
    }

    [Test]
    public async Task CustomHeaders_AreAppliedToRequests()
    {
        var trackingHandler = new TrackingHandler(CreateFakeVersionResponse());
        var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string>
            {
                { "X-Custom-Header", "custom-value" },
                { "X-Correlation-Id", "abc-123" }
            },
            HttpClient = httpClient
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        await connection.ExecuteStatementAsync("SELECT version() FORMAT TSV");

        Assert.That(trackingHandler.RequestCount, Is.GreaterThan(0));
        var request = trackingHandler.Requests.First();
        Assert.Multiple(() =>
        {
            Assert.That(request.Headers.Contains("X-Custom-Header"), Is.True);
            Assert.That(request.Headers.GetValues("X-Custom-Header").First(), Is.EqualTo("custom-value"));
            Assert.That(request.Headers.Contains("X-Correlation-Id"), Is.True);
            Assert.That(request.Headers.GetValues("X-Correlation-Id").First(), Is.EqualTo("abc-123"));
        });
    }

    [Test]
    public async Task CustomHeaders_BlockedHeaders_AreIgnored()
    {
        var trackingHandler = new TrackingHandler(CreateFakeVersionResponse());
        var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string>
            {
                { "Authorization", "Bearer malicious-token" },
                { "User-Agent", "malicious-agent" },
                { "Connection", "user-set-value" },
                { "X-Safe-Header", "safe-value" }
            },
            HttpClient = httpClient
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        await connection.ExecuteStatementAsync("SELECT version() FORMAT TSV");

        Assert.That(trackingHandler.RequestCount, Is.GreaterThan(0));
        var request = trackingHandler.Requests.First();
        Assert.Multiple(() =>
        {
            // Blocked headers should not be the custom values
            Assert.That(request.Headers.Authorization?.Scheme, Is.Not.EqualTo("Bearer malicious-token"),
                "Authorization header should not be overridden");
            Assert.That(request.Headers.UserAgent.ToString(), Does.Not.Contain("malicious-agent"),
                "User-Agent header should not be overridden");
            Assert.That(request.Headers.Connection.ToString(), Does.Not.Contain("user-set-value"),
                "Connection header should not be overridden");

            // Safe header should be present
            Assert.That(request.Headers.Contains("X-Safe-Header"), Is.True);
            Assert.That(request.Headers.GetValues("X-Safe-Header").First(), Is.EqualTo("safe-value"));
        });
    }

    [Test]
    public async Task CustomHeaders_BlockedHeaders_CaseInsensitive()
    {
        var trackingHandler = new TrackingHandler(CreateFakeVersionResponse());
        var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string>
            {
                { "AUTHORIZATION", "Bearer evil" },
                { "UsEr-AgEnT", "evil-agent" },
                { "CONNECTION", "evil" },
            },
            HttpClient = httpClient
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        await connection.ExecuteStatementAsync("SELECT version() FORMAT TSV");

        Assert.That(trackingHandler.RequestCount, Is.GreaterThan(0));
        var request = trackingHandler.Requests.First();
        Assert.Multiple(() =>
        {
            // All blocked headers should still use default values, not custom ones
            Assert.That(request.Headers.Authorization?.Scheme, Is.EqualTo("Basic"));
            Assert.That(request.Headers.UserAgent.ToString(), Does.Not.Contain("evil"));
        });
    }

    [Test]
    public async Task CustomHeaders_EmptyDictionary_Works()
    {
        var trackingHandler = new TrackingHandler(CreateFakeVersionResponse());
        var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string>(),
            HttpClient = httpClient
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        await connection.ExecuteStatementAsync("SELECT version() FORMAT TSV");

        Assert.That(trackingHandler.RequestCount, Is.GreaterThan(0));
        // Should not throw, and default headers should be present
        var request = trackingHandler.Requests.First();
        Assert.That(request.Headers.Authorization, Is.Not.Null);
        Assert.That(request.Headers.UserAgent.Count, Is.GreaterThan(0));
    }

    [Test]
    public async Task CustomHeaders_AllKnownContentHeaders_AreSilentlyIgnored_AndDoNotThrow()
    {
        var contentHeaders = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Allow"] = "GET, POST",
            ["Content-Disposition"] = "attachment; filename=\"file.bin\"",
            ["Content-Encoding"] = "gzip",
            ["Content-Language"] = "en-US",
            ["Content-Length"] = "42",
            ["Content-Location"] = "https://example.com/resource",
            ["Content-MD5"] = "Q2hlY2sgSW50ZWdyaXR5IQ==",
            ["Content-Range"] = "bytes 0-499/1234",
            ["Content-Type"] = "application/json",
            ["Expires"] = "Wed, 21 Oct 2026 07:28:00 GMT",
            ["Last-Modified"] = "Tue, 15 Nov 1994 12:45:26 GMT",
            ["X-Still-Works"] = "yes"          
        };

        var trackingHandler = new TrackingHandler(CreateFakeVersionResponse());
        var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            CustomHeaders = contentHeaders,
            HttpClient = httpClient
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        await connection.ExecuteStatementAsync("SELECT version() FORMAT TSV");

        Assert.That(trackingHandler.RequestCount, Is.GreaterThan(0));
        var request = trackingHandler.Requests[0];

        using (Assert.EnterMultipleScope())
        {
            foreach (var header in contentHeaders.Keys.Where(k => !k.StartsWith("X-", StringComparison.OrdinalIgnoreCase)))
            {
                Assert.That(request.Headers.Any(h => string.Equals(h.Key, header, StringComparison.OrdinalIgnoreCase)), Is.False,
                    $"Content header '{header}' should have been silently ignored");
            }

            Assert.That(request.Headers.Any(h => string.Equals(h.Key, "X-Still-Works", StringComparison.OrdinalIgnoreCase)), Is.True);
            Assert.That(request.Headers.GetValues("X-Still-Works").First(), Is.EqualTo("yes"));
        }
    }

    [Test]
    public async Task CustomHeaders_ContentHeaders_CaseInsensitive()
    {
        var trackingHandler = new TrackingHandler(CreateFakeVersionResponse());
        var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string>
            {
                ["content-type"] = "text/plain",
                ["CONTENT-ENCODING"] = "br",
                ["CoNtEnT-LeNgTh"] = "42",
                ["X-Normal"] = "ok"
            },
            HttpClient = httpClient
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        await connection.ExecuteStatementAsync("SELECT version() FORMAT TSV");

        var request = trackingHandler.Requests[0];

        using (Assert.EnterMultipleScope())
        {
            Assert.That(request.Headers.Any(h => string.Equals(h.Key, "content-type", StringComparison.OrdinalIgnoreCase)), Is.False);
            Assert.That(request.Headers.Any(h => string.Equals(h.Key, "CONTENT-ENCODING", StringComparison.OrdinalIgnoreCase)), Is.False);
            Assert.That(request.Headers.Any(h => string.Equals(h.Key, "CoNtEnT-LeNgTh", StringComparison.OrdinalIgnoreCase)), Is.False);
            Assert.That(request.Headers.Any(h => string.Equals(h.Key, "X-Normal", StringComparison.OrdinalIgnoreCase)), Is.True);
            Assert.That(request.Headers.GetValues("X-Normal").First(), Is.EqualTo("ok"));
        }
    }

    [Test]
    public async Task CustomHeaders_MixedBlockedAndContentHeaders_DoNotThrow()
    {
        var trackingHandler = new TrackingHandler(CreateFakeVersionResponse());
        var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string>
            {
                ["Authorization"] = "Bearer evil-token",
                ["Content-Type"] = "application/json",
                ["User-Agent"] = "evil-agent",
                ["X-Custom"] = "good",
                ["Content-Encoding"] = "gzip"
            },
            HttpClient = httpClient
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        await connection.ExecuteStatementAsync("SELECT version() FORMAT TSV");

        var request = trackingHandler.Requests[0];

        using (Assert.EnterMultipleScope())
        {
            Assert.That(request.Headers.Any(h => string.Equals(h.Key, "X-Custom", StringComparison.OrdinalIgnoreCase)), Is.True);
            Assert.That(request.Headers.GetValues("X-Custom").First(), Is.EqualTo("good"));

            Assert.That(request.Headers.Any(h => string.Equals(h.Key, "Content-Type", StringComparison.OrdinalIgnoreCase)), Is.False);
            Assert.That(request.Headers.Any(h => string.Equals(h.Key, "Content-Encoding", StringComparison.OrdinalIgnoreCase)), Is.False);

            Assert.That(request.Headers.Authorization?.Scheme, Is.Not.EqualTo("Bearer"),
                "Authorization must not be overwritten by a malicious value.");
            Assert.That(request.Headers.UserAgent.ToString(), Does.Not.Contain("evil-agent"),
                "User-Agent must not be overwritten by a malicious value.");
        }
    }

    [Test]
    public async Task CustomHeaders_PerQuery_ContentHeaders_DoNotThrow()
    {
        var trackingHandler = new TrackingHandler(CreateFakeVersionResponse());
        var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            HttpClient = httpClient
        };

        using var client = new ClickHouseClient(settings);

        var options = new QueryOptions
        {
            CustomHeaders = new Dictionary<string, string>
            {
                ["Content-Type"] = "application/octet-stream",
                ["X-Per-Query"] = "query-value"
            }
        };

        await client.ExecuteNonQueryAsync("SELECT 1", options: options);

        Assert.That(trackingHandler.RequestCount, Is.GreaterThan(0));
        var request = trackingHandler.Requests[0];

        using (Assert.EnterMultipleScope())
        {
            Assert.That(request.Headers.Any(h => string.Equals(h.Key, "Content-Type", StringComparison.OrdinalIgnoreCase)), Is.False, "Content-Type (content header) should be silently ignored");
            Assert.That(request.Headers.Any(h => string.Equals(h.Key, "X-Per-Query", StringComparison.OrdinalIgnoreCase)), Is.True);
            Assert.That(request.Headers.GetValues("X-Per-Query").First(), Is.EqualTo("query-value"));
        }
    }
}
