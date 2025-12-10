using System;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// Integration tests for JWT/Bearer token authentication.
/// These tests require a ClickHouse Cloud instance configured with JWT authentication.
/// Set the CLICKHOUSE_CLOUD_JWT environment variable to run these tests.
/// </summary>
[TestFixture]
[Category("Cloud")]
[Category("JWT")]
public class BearerAuthenticationTests
{
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
            Username = "invalid_user", // This would fail with basic auth
            Password = "invalid_pass",
            BearerToken = "invalid_token", // But bearer token should be used instead
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.BearerToken = bearerToken;
        command.CommandText = "SELECT 1";

        var result = await command.ExecuteScalarAsync();

        Assert.That(result, Is.EqualTo(1));
    }
}
