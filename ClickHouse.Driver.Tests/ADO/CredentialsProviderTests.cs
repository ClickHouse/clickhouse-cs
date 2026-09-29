using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

public class CredentialsProviderTests
{
    private static TrackingHandler CreateTrackingHandler() =>
        new(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("25.10\tUTC") });

    private static string BasicOf(string username, string password) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));

    [Test]
    public async Task Request_WithProviderBasicCredentials_ShouldUseProviderNotStaticSettings()
    {
        var trackingHandler = CreateTrackingHandler();
        using var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Username = "staticuser",
            Password = "staticpass",
            CredentialsProvider = () => ClickHouseCredentials.CreateBasic("provideruser", "providerpass"),
            HttpClient = httpClient
        };
        using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        await conn.ExecuteStatementAsync("SELECT 1");

        Assert.That(trackingHandler.Requests, Has.Count.GreaterThan(0));
        foreach (var request in trackingHandler.Requests)
        {
            Assert.That(request.Headers.Authorization, Is.Not.Null);
            Assert.That(request.Headers.Authorization.Scheme, Is.EqualTo("Basic"));
            Assert.That(request.Headers.Authorization.Parameter, Is.EqualTo(BasicOf("provideruser", "providerpass")));
        }
    }

    [Test]
    public async Task Request_WithProviderBearerCredentials_ShouldUseBearerAuth()
    {
        var trackingHandler = CreateTrackingHandler();
        using var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Username = "staticuser",
            Password = "staticpass",
            CredentialsProvider = () => ClickHouseCredentials.CreateBearer("provider-token"),
            HttpClient = httpClient
        };
        using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        await conn.ExecuteStatementAsync("SELECT 1");

        Assert.That(trackingHandler.Requests, Has.Count.GreaterThan(0));
        foreach (var request in trackingHandler.Requests)
        {
            Assert.That(request.Headers.Authorization, Is.Not.Null);
            Assert.That(request.Headers.Authorization.Scheme, Is.EqualTo("Bearer"));
            Assert.That(request.Headers.Authorization.Parameter, Is.EqualTo("provider-token"));
        }
    }

    [Test]
    public async Task Request_ProviderResultChanges_ShouldUseLatestCredentialsPerRequest()
    {
        var trackingHandler = CreateTrackingHandler();
        using var httpClient = new HttpClient(trackingHandler);

        var currentPassword = "password-a";
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            CredentialsProvider = () => ClickHouseCredentials.CreateBasic("rotatinguser", currentPassword),
            HttpClient = httpClient
        };
        using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        await conn.ExecuteStatementAsync("SELECT 1");

        currentPassword = "password-b";
        await conn.ExecuteStatementAsync("SELECT 1");

        var authValues = trackingHandler.Requests.Select(r => r.Headers.Authorization.Parameter).ToList();
        Assert.That(authValues.First(), Is.EqualTo(BasicOf("rotatinguser", "password-a")));
        Assert.That(authValues.Last(), Is.EqualTo(BasicOf("rotatinguser", "password-b")));
    }

    [Test]
    public async Task Request_MultipleRequests_ShouldInvokeProviderPerRequest()
    {
        var trackingHandler = CreateTrackingHandler();
        using var httpClient = new HttpClient(trackingHandler);

        var invocationCount = 0;
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            CredentialsProvider = () =>
            {
                invocationCount++;
                return ClickHouseCredentials.CreateBasic("user", "pass");
            },
            HttpClient = httpClient
        };
        using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        await conn.ExecuteStatementAsync("SELECT 1");
        await conn.ExecuteStatementAsync("SELECT 1");

        Assert.That(invocationCount, Is.EqualTo(trackingHandler.RequestCount));
        Assert.That(invocationCount, Is.GreaterThanOrEqualTo(2));
    }

    [Test]
    public async Task Command_WithBearerTokenOverride_ShouldTakePrecedenceOverProvider()
    {
        var trackingHandler = CreateTrackingHandler();
        using var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            CredentialsProvider = () => ClickHouseCredentials.CreateBearer("provider-token"),
            HttpClient = httpClient
        };
        using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        var command = conn.CreateCommand();
        command.CommandText = "SELECT 1";
        command.BearerToken = "command-level-token";
        await command.ExecuteNonQueryAsync();

        var request = trackingHandler.Requests[^1];
        Assert.That(request.Headers.Authorization, Is.Not.Null);
        Assert.That(request.Headers.Authorization.Scheme, Is.EqualTo("Bearer"));
        Assert.That(request.Headers.Authorization.Parameter, Is.EqualTo("command-level-token"));
    }

    [Test]
    public async Task Request_ProviderBasic_ShouldOverrideStaticBearerToken()
    {
        var trackingHandler = CreateTrackingHandler();
        using var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            BearerToken = "static-token",
            CredentialsProvider = () => ClickHouseCredentials.CreateBasic("provideruser", "providerpass"),
            HttpClient = httpClient
        };
        using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        await conn.ExecuteStatementAsync("SELECT 1");

        var request = trackingHandler.Requests[^1];
        Assert.That(request.Headers.Authorization, Is.Not.Null);
        Assert.That(request.Headers.Authorization.Scheme, Is.EqualTo("Basic"));
        Assert.That(request.Headers.Authorization.Parameter, Is.EqualTo(BasicOf("provideruser", "providerpass")));
    }

    [Test]
    public async Task Request_ProviderReturnsNull_ShouldFallBackToStaticSettings()
    {
        var trackingHandler = CreateTrackingHandler();
        using var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Username = "staticuser",
            Password = "staticpass",
            CredentialsProvider = () => null,
            HttpClient = httpClient
        };
        using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        await conn.ExecuteStatementAsync("SELECT 1");

        var request = trackingHandler.Requests[^1];
        Assert.That(request.Headers.Authorization, Is.Not.Null);
        Assert.That(request.Headers.Authorization.Scheme, Is.EqualTo("Basic"));
        Assert.That(request.Headers.Authorization.Parameter, Is.EqualTo(BasicOf("staticuser", "staticpass")));
    }

    [Test]
    public async Task Request_ProviderReturnsNull_WithStaticBearerToken_ShouldFallBackToBearer()
    {
        var trackingHandler = CreateTrackingHandler();
        using var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            BearerToken = "static-token",
            CredentialsProvider = () => null,
            HttpClient = httpClient
        };
        using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        await conn.ExecuteStatementAsync("SELECT 1");

        var request = trackingHandler.Requests[^1];
        Assert.That(request.Headers.Authorization, Is.Not.Null);
        Assert.That(request.Headers.Authorization.Scheme, Is.EqualTo("Bearer"));
        Assert.That(request.Headers.Authorization.Parameter, Is.EqualTo("static-token"));
    }

    [Test]
    public void Request_ProviderThrows_ShouldPropagateException()
    {
        var trackingHandler = CreateTrackingHandler();
        using var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            CredentialsProvider = () => throw new InvalidOperationException("credential store unavailable"),
            HttpClient = httpClient
        };
        using var client = new ClickHouseClient(settings);

        Assert.ThrowsAsync<InvalidOperationException>(() => client.ExecuteScalarAsync("SELECT 1"));
    }

    [Test]
    public async Task Command_WithEmptyBearerTokenOverride_ShouldFallBackToStaticBearerToken()
    {
        var trackingHandler = CreateTrackingHandler();
        using var httpClient = new HttpClient(trackingHandler);

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            BearerToken = "static-token",
            HttpClient = httpClient
        };
        using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        // An explicitly empty per-query override is treated as unset: the static bearer token applies.
        var command = conn.CreateCommand();
        command.CommandText = "SELECT 1";
        command.BearerToken = string.Empty;
        await command.ExecuteNonQueryAsync();

        var request = trackingHandler.Requests[^1];
        Assert.That(request.Headers.Authorization, Is.Not.Null);
        Assert.That(request.Headers.Authorization.Scheme, Is.EqualTo("Bearer"));
        Assert.That(request.Headers.Authorization.Parameter, Is.EqualTo("static-token"));
    }

    [Test]
    public void CreateBasic_WithNullOrEmptyUsername_ShouldThrow()
    {
        Assert.Throws<ArgumentException>(() => ClickHouseCredentials.CreateBasic(null, "pass"));
        Assert.Throws<ArgumentException>(() => ClickHouseCredentials.CreateBasic(string.Empty, "pass"));
    }

    [Test]
    public void CreateBasic_WithNullPassword_ShouldCoerceToEmpty()
    {
        var credentials = ClickHouseCredentials.CreateBasic("user", null);
        Assert.That(credentials.Password, Is.EqualTo(string.Empty));
        Assert.That(credentials.Username, Is.EqualTo("user"));
        Assert.That(credentials.BearerToken, Is.Null);
    }

    [Test]
    public void CreateBearer_WithNullOrEmptyToken_ShouldThrow()
    {
        Assert.Throws<ArgumentException>(() => ClickHouseCredentials.CreateBearer(null));
        Assert.Throws<ArgumentException>(() => ClickHouseCredentials.CreateBearer(string.Empty));
    }

    [Test]
    public void CreateBearer_WithToken_ShouldCarryOnlyToken()
    {
        var credentials = ClickHouseCredentials.CreateBearer("token");
        Assert.That(credentials.BearerToken, Is.EqualTo("token"));
        Assert.That(credentials.Username, Is.Null);
        Assert.That(credentials.Password, Is.Null);
    }
}
