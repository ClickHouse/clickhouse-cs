using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
using NUnit.Framework;

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

        Assert.That(trackingHandler.RequestCount, Is.GreaterThan(0));
        // Should not throw, and default headers should be present
        var request = trackingHandler.Requests.First();
        Assert.That(request.Headers.Authorization, Is.Not.Null);
        Assert.That(request.Headers.UserAgent.Count, Is.GreaterThan(0));
    }
}
