using System;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// Integration tests for JWT/Bearer token authentication.
/// These tests require a ClickHouse Cloud instance configured with JWT authentication.
/// Set the CLICKHOUSE_CLOUD_JWT environment variable to run these tests. In CI, the Cloud workflow
/// signs a new token for each run with <c>.github/scripts/generate_jwt.py</c>.
/// </summary>
[TestFixture]
[Category("Cloud")]
[Category("JWT")]
public class BearerAuthenticationTests
{
    // ClickHouse Cloud runs a JWT-authenticated request as an ephemeral user named JWT::<subject>::<claims_hash>.
    private const string JwtUserNamePattern = "^JWT::.+::.+$";

    private string connectionString;
    private string bearerToken;

    [SetUp]
    public void Setup()
    {
        connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
        bearerToken = Environment.GetEnvironmentVariable("CLICKHOUSE_CLOUD_JWT");

        if (string.IsNullOrEmpty(bearerToken))
        {
            Assert.Ignore("Skipping JWT tests: CLICKHOUSE_CLOUD_JWT environment variable must be set");
        }
    }

    [Test]
    public async Task Connection_WithBearerToken_ShouldExecuteQuery()
    {
        var settings = new ClickHouseClientSettings(connectionString)
        {
            BearerToken = bearerToken,
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync("SELECT 1");

        Assert.That(result, Is.EqualTo(1));
    }

    [Test]
    public async Task Command_WithBearerTokenOverride_ShouldUseCommandToken()
    {
        var settings = new ClickHouseClientSettings(connectionString)
        {
            BearerToken = bearerToken,
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync(); // A bit problematic: this will make a query, so you need to set a valid token at the connection level. Hence the test below.

        // Execute command with the same valid token via command-level override
        var command = connection.CreateCommand();
        command.BearerToken = bearerToken;
        command.CommandText = "SELECT 1";

        var result = await command.ExecuteScalarAsync();

        Assert.That(result, Is.EqualTo(1));
    }

    [Test]
    public async Task Command_WithInvalidBearerToken_ShouldFail()
    {
        // Open connection with valid bearer token
        var settings = new ClickHouseClientSettings(connectionString)
        {
            BearerToken = bearerToken,
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        // Execute command with invalid token - should fail, proving command-level token is used
        var command = connection.CreateCommand();
        command.BearerToken = "invalid_token";
        command.CommandText = "SELECT 1";

        Assert.ThrowsAsync<ClickHouseServerException>(async () => await command.ExecuteScalarAsync());
    }

    [Test]
    public async Task ExecuteScalarAsync_WithClientBearerToken_ShouldRunAsJwtUser()
    {
        var settings = new ClickHouseClientSettings(connectionString)
        {
            BearerToken = bearerToken,
        };
        using var client = new ClickHouseClient(settings);

        var user = await client.ExecuteScalarAsync("SELECT currentUser()");

        Assert.That(user, Does.Match(JwtUserNamePattern));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithBearerTokenInConnectionString_ShouldRunAsJwtUser()
    {
        var builder = new ClickHouseConnectionStringBuilder(connectionString)
        {
            BearerToken = bearerToken,
        };
        using var connection = new ClickHouseConnection(builder.ConnectionString);

        var user = await connection.ExecuteScalarAsync("SELECT currentUser()");

        Assert.That(user, Does.Match(JwtUserNamePattern));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithQueryOptionsBearerToken_ShouldRunOnlyThatQueryAsJwtUser()
    {
        // The client itself authenticates with the Basic credentials of CLICKHOUSE_CONNECTION.
        using var client = new ClickHouseClient(connectionString);

        var tokenUser = await client.ExecuteScalarAsync(
            "SELECT currentUser()", options: new QueryOptions { BearerToken = bearerToken });
        var clientUser = await client.ExecuteScalarAsync("SELECT currentUser()");

        Assert.Multiple(() =>
        {
            Assert.That(tokenUser, Does.Match(JwtUserNamePattern));
            Assert.That(clientUser, Does.Not.Match(JwtUserNamePattern));
        });
    }
}
