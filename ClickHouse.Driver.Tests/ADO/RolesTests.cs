using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.ADO;

[TestFixture]
public class RolesTests
{
    private readonly ConcurrentQueue<string> createdTables = new();

    private ClickHouseConnection defaultConnection;
    private string database;
    private string username;
    private string password;
    private string roleName1;
    private string roleName2;

    [OneTimeSetUp]
    public async Task Setup()
    {
        // These tests require a full ClickHouse installation with access storage enabled
        // (for user/role management). They can't run on cloud or quick-setup environments.
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
        {
            Assert.Ignore("Skipping roles integration tests (requires local_single_node environment with access storage)");
        }

        defaultConnection = TestUtilities.GetTestClickHouseConnection();

        // Get database from connection settings, or query the current database if not set
        database = defaultConnection.Database;
        if (string.IsNullOrEmpty(database))
        {
            database = (string)await defaultConnection.ExecuteScalarAsync("SELECT currentDatabase()");
        }

        // Generate unique names to avoid conflicts
        var guid = Guid.NewGuid().ToString("N");
        username = $"clickhousecs__user_with_roles_{guid}";
        password = $"CHCS_{guid}";
        roleName1 = $"TEST_ROLE_1_{guid}";
        roleName2 = $"TEST_ROLE_2_{guid}";

        // Create user
        await defaultConnection.ExecuteStatementAsync(
            $"CREATE USER IF NOT EXISTS {username} IDENTIFIED WITH sha256_password BY '{password}' DEFAULT DATABASE {database}");

        // Create roles
        await defaultConnection.ExecuteStatementAsync($"CREATE ROLE IF NOT EXISTS {roleName1}");
        await defaultConnection.ExecuteStatementAsync($"CREATE ROLE IF NOT EXISTS {roleName2}");

        // Grant roles to user
        await defaultConnection.ExecuteStatementAsync($"GRANT {roleName1}, {roleName2} TO {username}");

        // Grant permissions to role1 (INSERT and CREATE TABLE)
        await defaultConnection.ExecuteStatementAsync($"GRANT INSERT ON {database}.* TO {roleName1}");
        await defaultConnection.ExecuteStatementAsync($"GRANT CREATE TABLE ON {database}.* TO {roleName1}");

        // role2 has no special permissions - just SELECT
        await defaultConnection.ExecuteStatementAsync($"GRANT SELECT ON {database}.* TO {roleName2}");
    }

    [OneTimeTearDown]
    public async Task Cleanup()
    {
        try
        {
            // Best-effort: a table that cannot be dropped must not fail an otherwise passing fixture
            while (createdTables.TryDequeue(out var tableName))
            {
                try
                {
                    await defaultConnection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {database}.{tableName}");
                }
                catch (Exception e)
                {
                    TestContext.Progress.WriteLine($"Failed to drop test table {tableName}: {e.Message}");
                }
            }

            // Drop user and roles
            await defaultConnection.ExecuteStatementAsync($"DROP USER IF EXISTS {username}");
            await defaultConnection.ExecuteStatementAsync($"DROP ROLE IF EXISTS {roleName1}");
            await defaultConnection.ExecuteStatementAsync($"DROP ROLE IF EXISTS {roleName2}");
        }
        finally
        {
            defaultConnection?.Dispose();
        }
    }

    /// <summary>
    /// Builds a unique table name and registers it to be dropped when the fixture tears down.
    /// Deliberately unqualified: these tests run against the connection's own database, which is
    /// also the database the roles are granted privileges on.
    /// </summary>
    private string CreateTableName([CallerMemberName] string testName = null)
    {
        var tableName = TestUtilities.CreateTableName(database: null, testName: testName);
        createdTables.Enqueue(tableName);
        return tableName;
    }

    private ClickHouseConnection CreateClientWithRoles(params string[] roles)
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.Username = username;
        builder.Password = password;
        builder.Database = database;
        builder.Roles = roles;
        var connection = new ClickHouseConnection(builder.ConnectionString);
        connection.Open();
        return connection;
    }

    private async Task<string[]> QueryCurrentRoles(ClickHouseConnection connection, string[] commandRoles = null)
    {
        using var command = connection.CreateCommand("SELECT currentRoles() as roles");
        if (commandRoles != null)
        {
            foreach (var role in commandRoles)
            {
                command.Roles.Add(role);
            }
        }

        using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();
        return reader.GetFieldValue<string[]>(0);
    }

    [Test]
    public async Task Query_SingleRoleFromConnectionSettings_ShouldUseRole()
    {
        using var client = CreateClientWithRoles(roleName1);

        var actualRoles = await QueryCurrentRoles(client);

        Assert.That(actualRoles, Has.Length.EqualTo(1));
        Assert.That(actualRoles, Contains.Item(roleName1));
    }

    [Test]
    public async Task Query_MultipleRolesFromConnectionSettings_ShouldUseAllRoles()
    {
        using var client = CreateClientWithRoles(roleName1, roleName2);

        var actualRoles = await QueryCurrentRoles(client);

        Assert.That(actualRoles, Has.Length.EqualTo(2));
        Assert.That(actualRoles, Contains.Item(roleName1));
        Assert.That(actualRoles, Contains.Item(roleName2));
    }

    [Test]
    public async Task Query_SingleRoleFromCommandOverride_ShouldUseCommandRole()
    {
        using var client = CreateClientWithRoles(roleName1, roleName2);

        var actualRoles = await QueryCurrentRoles(client, new[] { roleName2 });

        Assert.That(actualRoles, Has.Length.EqualTo(1));
        Assert.That(actualRoles, Contains.Item(roleName2));
    }

    [Test]
    public async Task Query_MultipleRolesFromCommandOverride_ShouldUseCommandRoles()
    {
        using var client = CreateClientWithRoles(roleName1);

        var actualRoles = await QueryCurrentRoles(client, new[] { roleName1, roleName2 });

        Assert.That(actualRoles, Has.Length.EqualTo(2));
        Assert.That(actualRoles, Contains.Item(roleName1));
        Assert.That(actualRoles, Contains.Item(roleName2));
    }

    [Test]
    public async Task Insert_WithRoleThatCanInsert_ShouldSucceed()
    {
        var tableName = CreateTableName();
        using var client = CreateClientWithRoles(roleName1);

        // Create table and insert using role1 (has INSERT permission)
        await defaultConnection.ExecuteStatementAsync(
            $"CREATE TABLE {tableName} (id Int32) ENGINE = MergeTree() ORDER BY id");

        await client.ExecuteStatementAsync($"INSERT INTO {tableName} VALUES (1)");

        // Verify insert worked
        var count = await defaultConnection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Assert.That(count, Is.EqualTo(1UL));
    }

    [Test]
    public async Task Insert_WithRoleThatCannotInsert_ShouldFail()
    {
        var tableName = CreateTableName();
        using var client = CreateClientWithRoles(roleName2); // role2 has no INSERT permission

        await defaultConnection.ExecuteStatementAsync(
            $"CREATE TABLE {tableName} (id Int32) ENGINE = MergeTree() ORDER BY id");

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            async () => await client.ExecuteStatementAsync($"INSERT INTO {tableName} VALUES (1)"));

        Assert.That(ex.Message, Does.Contain("Not enough privileges").IgnoreCase
            .Or.Contain("ACCESS_DENIED").IgnoreCase);
    }

    [Test]
    public async Task Insert_CommandRoleOverrideAllowsInsert_ShouldSucceed()
    {
        var tableName = CreateTableName();
        using var client = CreateClientWithRoles(roleName2); // Connection has role2 (no INSERT)

        await defaultConnection.ExecuteStatementAsync(
            $"CREATE TABLE {tableName} (id Int32) ENGINE = MergeTree() ORDER BY id");

        // Override with role1 (has INSERT permission) at command level
        using var command = client.CreateCommand($"INSERT INTO {tableName} VALUES (1)");
        command.Roles.Add(roleName1);
        await command.ExecuteNonQueryAsync();

        // Verify insert worked
        var count = await defaultConnection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Assert.That(count, Is.EqualTo(1UL));
    }

    [Test]
    public async Task CreateTable_WithRoleThatCanCreate_ShouldSucceed()
    {
        var tableName = CreateTableName();
        using var client = CreateClientWithRoles(roleName1); // role1 has CREATE TABLE permission

        await client.ExecuteStatementAsync(
            $"CREATE TABLE {tableName} (id Int32) ENGINE = MergeTree() ORDER BY id");

        // Verify table was created
        var exists = await defaultConnection.ExecuteScalarAsync(
            $"SELECT count() FROM system.tables WHERE database = '{database}' AND name = '{tableName}'");
        Assert.That(exists, Is.EqualTo(1UL));
    }

    [Test]
    public void CreateTable_WithRoleThatCannotCreate_ShouldFail()
    {
        var tableName = CreateTableName();
        using var client = CreateClientWithRoles(roleName2); // role2 has no CREATE TABLE permission

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            async () => await client.ExecuteStatementAsync(
                $"CREATE TABLE {tableName} (id Int32) ENGINE = MergeTree() ORDER BY id"));

        Assert.That(ex.Message, Does.Contain("Not enough privileges").IgnoreCase
            .Or.Contain("ACCESS_DENIED").IgnoreCase);
    }

    [Test]
    public async Task CreateTable_CommandRoleOverrideAllowsCreate_ShouldSucceed()
    {
        var tableName = CreateTableName();
        using var client = CreateClientWithRoles(roleName2); // Connection has role2 (no CREATE TABLE)

        // Override with role1 (has CREATE TABLE permission) at command level
        using var command = client.CreateCommand(
            $"CREATE TABLE {tableName} (id Int32) ENGINE = MergeTree() ORDER BY id");
        command.Roles.Add(roleName1);
        await command.ExecuteNonQueryAsync();

        // Verify table was created
        var exists = await defaultConnection.ExecuteScalarAsync(
            $"SELECT count() FROM system.tables WHERE database = '{database}' AND name = '{tableName}'");
        Assert.That(exists, Is.EqualTo(1UL));
    }
}
