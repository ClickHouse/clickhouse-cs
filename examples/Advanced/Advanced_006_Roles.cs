using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to use ClickHouse roles to control permissions.
/// See https://clickhouse.com/docs/en/interfaces/http#setting-role-with-query-parameters
///
/// This example:
/// 1. Creates a user and two tables with corresponding roles
/// 2. Shows how connection-level roles restrict access
/// 3. Shows how command-level roles can override connection roles
/// 4. Demonstrates using multiple roles simultaneously
/// </summary>
public static class Roles
{
    private static readonly string Username = "clickhouse_cs_role_user";
    private static readonly string Password = "role_user_password";
    private static readonly string Table1 = "clickhouse_cs_role_table_1";
    private static readonly string Table2 = "clickhouse_cs_role_table_2";

    public static async Task Run()
    {
        Console.WriteLine("Roles Examples\n");
        Console.WriteLine("This example demonstrates role-based access control.\n");

        // Setup: Create tables, roles, and user using default connection
        using var defaultClient = new ClickHouseConnection("Host=localhost");
        await defaultClient.OpenAsync();

        await CreateOrReplaceUser(defaultClient, Username, Password);
        var table1Role = await CreateTableAndGrantAccess(defaultClient, Table1, Username);
        var table2Role = await CreateTableAndGrantAccess(defaultClient, Table2, Username);

        Console.WriteLine($"Created user: {Username}");
        Console.WriteLine($"Created table: {Table1} with role: {table1Role}");
        Console.WriteLine($"Created table: {Table2} with role: {table2Role}");

        try
        {
            // Create a client using a role that only has permission to query table1
            var settings = new ClickHouseClientSettings("Host=localhost")
            {
                Username = Username,
                Password = Password,
                // This role will be applied to all queries by default,
                // unless overridden in a specific command
                Roles = new[] { table1Role }
            };

            using var client = new ClickHouseConnection(settings);
            await client.OpenAsync();

            // 1. Selecting from table1 is allowed using table1Role
            Console.WriteLine($"\n1. Query {Table1} using {table1Role} (connection role):");
            var count1 = await client.ExecuteScalarAsync($"SELECT count(*) FROM {Table1}");
            Console.WriteLine($"   Success! Count: {count1}");

            // 2. Selecting from table2 is NOT allowed using table1Role
            Console.WriteLine($"\n2. Query {Table2} using {table1Role} (should fail):");
            try
            {
                await client.ExecuteScalarAsync($"SELECT count(*) FROM {Table2}");
                Console.WriteLine("   Unexpected success!");
            }
            catch (ClickHouseServerException ex)
            {
                Console.WriteLine($"   Expected failure: {ex.Message[..Math.Min(80, ex.Message.Length)]}...");
            }

            // 3. Override the connection's role with table2Role at the command level -- this REPLACES the connection-level roles
            Console.WriteLine($"\n3. Query {Table2} using {table2Role} (command override):");
            using (var command = client.CreateCommand($"SELECT count(*) FROM {Table2}"))
            {
                command.Roles.Add(table2Role);
                var count2 = await command.ExecuteScalarAsync();
                Console.WriteLine($"   Success! Count: {count2}");
            }

            // 4. Now table1 is NOT accessible with table2Role
            Console.WriteLine($"\n4. Query {Table1} using {table2Role} (should fail):");
            try
            {
                using var command = client.CreateCommand($"SELECT count(*) FROM {Table1}");
                command.Roles.Add(table2Role);
                await command.ExecuteScalarAsync();
                Console.WriteLine("   Unexpected success!");
            }
            catch (ClickHouseServerException ex)
            {
                Console.WriteLine($"   Expected failure: {ex.Message[..Math.Min(80, ex.Message.Length)]}...");
            }

            // 5. Multiple roles allow querying from either table
            Console.WriteLine($"\n5. Query both tables using multiple roles [{table1Role}, {table2Role}]:");
            using (var command = client.CreateCommand($"SELECT count(*) FROM {Table1}"))
            {
                command.Roles.Add(table1Role);
                command.Roles.Add(table2Role);
                var count = await command.ExecuteScalarAsync();
                Console.WriteLine($"   {Table1}: Success! Count: {count}");
            }

            using (var command = client.CreateCommand($"SELECT count(*) FROM {Table2}"))
            {
                command.Roles.Add(table1Role);
                command.Roles.Add(table2Role);
                var count = await command.ExecuteScalarAsync();
                Console.WriteLine($"   {Table2}: Success! Count: {count}");
            }
        }
        finally
        {
            // Cleanup
            Console.WriteLine("\nCleaning up...");
            await defaultClient.ExecuteStatementAsync($"DROP TABLE IF EXISTS {Table1}");
            await defaultClient.ExecuteStatementAsync($"DROP TABLE IF EXISTS {Table2}");
            await defaultClient.ExecuteStatementAsync($"DROP ROLE IF EXISTS {Table1}_role");
            await defaultClient.ExecuteStatementAsync($"DROP ROLE IF EXISTS {Table2}_role");
            await defaultClient.ExecuteStatementAsync($"DROP USER IF EXISTS {Username}");
            Console.WriteLine("Cleanup complete.");
        }

        Console.WriteLine("\nAll roles examples completed!");
    }

    private static async Task CreateOrReplaceUser(ClickHouseConnection client, string username, string password)
    {
        await client.ExecuteStatementAsync(
            $"CREATE USER OR REPLACE {username} IDENTIFIED WITH plaintext_password BY '{password}'");
    }

    private static async Task<string> CreateTableAndGrantAccess(
        ClickHouseConnection client,
        string tableName,
        string username)
    {
        var role = $"{tableName}_role";

        await client.ExecuteStatementAsync($@"
            CREATE OR REPLACE TABLE {tableName}
            (id UInt32, name String, sku Array(UInt32))
            ENGINE MergeTree()
            ORDER BY (id)");

        await client.ExecuteStatementAsync($"CREATE ROLE OR REPLACE {role}");
        await client.ExecuteStatementAsync($"GRANT SELECT ON {tableName} TO {role}");
        await client.ExecuteStatementAsync($"GRANT {role} TO {username}");

        return role;
    }
}
