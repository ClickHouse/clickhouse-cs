using System;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// Integration tests for <see cref="ClickHouseClientSettings.CredentialsProvider"/> covering
/// password rotation against a live server (issue #395): the provider lets a client pick up a
/// rotated password without being recreated.
/// </summary>
[TestFixture]
public class CredentialsProviderRotationTests
{
    private ClickHouseConnection defaultConnection;
    private string database;
    private string username;

    [OneTimeSetUp]
    public async Task Setup()
    {
        // Requires user management (ALTER USER), which needs access storage enabled.
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
        {
            Assert.Ignore("Skipping credential rotation tests (requires local_single_node environment with access storage)");
        }

        defaultConnection = TestUtilities.GetTestClickHouseConnection();

        database = defaultConnection.Database;
        if (string.IsNullOrEmpty(database))
        {
            database = (string)await defaultConnection.ExecuteScalarAsync("SELECT currentDatabase()");
        }

        username = $"clickhousecs__cred_rotation_{Guid.NewGuid():N}";
    }

    [OneTimeTearDown]
    public async Task Cleanup()
    {
        try
        {
            if (defaultConnection != null)
            {
                await defaultConnection.ExecuteStatementAsync($"DROP USER IF EXISTS {username}");
            }
        }
        finally
        {
            defaultConnection?.Dispose();
        }
    }

    [SetUp]
    public async Task ResetUser()
    {
        // Each test starts from a clean user with a known password.
        await defaultConnection.ExecuteStatementAsync($"DROP USER IF EXISTS {username}");
        await defaultConnection.ExecuteStatementAsync(
            $"CREATE USER {username} IDENTIFIED WITH sha256_password BY 'PasswordA_1!' DEFAULT DATABASE {database}");
        await defaultConnection.ExecuteStatementAsync($"GRANT SELECT ON {database}.* TO {username}");
    }

    private ClickHouseClientSettings CreateSettings(Func<ClickHouseCredentials> credentialsProvider = null, string staticPassword = null)
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.Username = username;
        builder.Password = staticPassword ?? string.Empty;
        builder.Database = database;
        return new ClickHouseClientSettings(builder.ToSettings())
        {
            CredentialsProvider = credentialsProvider,
        };
    }

    private Task RotatePasswordAsync(string newPassword) =>
        defaultConnection.ExecuteStatementAsync(
            $"ALTER USER {username} IDENTIFIED WITH sha256_password BY '{newPassword}'");

    [Test]
    public async Task Query_AfterPasswordRotation_ProviderConnectionSucceedsWithoutRecreating()
    {
        var currentPassword = "PasswordA_1!";
        var settings = CreateSettings(() => ClickHouseCredentials.CreateBasic(username, currentPassword));

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        await RotatePasswordAsync("PasswordB_2!");
        currentPassword = "PasswordB_2!";

        // Same open connection, no recreation - the provider supplies the rotated password.
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    public async Task Query_AfterPasswordRotation_StaticCredentialConnectionFails()
    {
        var settings = CreateSettings(staticPassword: "PasswordA_1!");

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        await RotatePasswordAsync("PasswordB_2!");

        var exception = Assert.ThrowsAsync<ClickHouseServerException>(() => connection.ExecuteScalarAsync("SELECT 1"));
        Assert.That(exception.ErrorCode, Is.EqualTo(516)); // AUTHENTICATION_FAILED
    }

    [Test]
    public async Task Query_ProviderStillReturningOldPassword_ShouldFail()
    {
        var settings = CreateSettings(() => ClickHouseCredentials.CreateBasic(username, "PasswordA_1!"));

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        await RotatePasswordAsync("PasswordB_2!");

        var exception = Assert.ThrowsAsync<ClickHouseServerException>(() => connection.ExecuteScalarAsync("SELECT 1"));
        Assert.That(exception.ErrorCode, Is.EqualTo(516)); // AUTHENTICATION_FAILED
    }

    [Test]
    public async Task Query_AfterPasswordRotation_ProviderClientSucceedsWithoutRecreating()
    {
        var currentPassword = "PasswordA_1!";
        var settings = CreateSettings(() => ClickHouseCredentials.CreateBasic(username, currentPassword));

        // Non-ADO path: ClickHouseClient directly.
        using var client = new ClickHouseClient(settings);
        Assert.That(await client.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        await RotatePasswordAsync("PasswordB_2!");
        currentPassword = "PasswordB_2!";

        Assert.That(await client.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }
}
