using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates limitations and behavior when working with users created in READONLY = 1 mode.
/// Read-only users have significant restrictions that affect how the client can be configured.
///
/// Key limitations of READONLY = 1 users:
/// - Cannot INSERT, UPDATE, DELETE, or ALTER data
/// - Cannot modify most ClickHouse settings (including send_progress_in_http_headers)
/// - Can only query tables they have been explicitly granted access to
/// </summary>
public static class ReadOnlyUsers
{
    private static readonly string TestTableName = "clickhouse_cs_readonly_test_data";

    public static async Task Run()
    {
        Console.WriteLine("Read-Only User Examples\n");
        Console.WriteLine("This example demonstrates the limitations of READONLY = 1 users.\n");

        // Setup using the default (non-read-only) user
        using var defaultClient = new ClickHouseConnection("Host=localhost");
        await defaultClient.OpenAsync();

        // Create a unique read-only user for this example
        var guid = Guid.NewGuid().ToString("N");
        var readOnlyUsername = $"clickhouse_cs_readonly_user_{guid}";
        var readOnlyPassword = $"{guid}_pwd";

        await SetupReadOnlyUser(defaultClient, readOnlyUsername, readOnlyPassword);
        await SetupTestTable(defaultClient);

        Console.WriteLine($"Created read-only user: {readOnlyUsername}");
        Console.WriteLine($"Created test table: {TestTableName}");
        PrintSeparator();

        try
        {
            // 1. Read-only user CAN query tables they have access to
            await DemonstrateAllowedSelect(readOnlyUsername, readOnlyPassword);

            // 2. Read-only user CANNOT insert data
            await DemonstrateInsertBlocked(readOnlyUsername, readOnlyPassword);

            // 3. Read-only user CANNOT query tables they don't have access to
            await DemonstrateUnauthorizedTableBlocked(readOnlyUsername, readOnlyPassword);

            // 4. Read-only user CANNOT use most ClickHouse settings
            await DemonstrateSettingsBlocked(readOnlyUsername, readOnlyPassword);

            Console.WriteLine("All read-only user examples completed!");
        }
        finally
        {
            // Cleanup
            Console.WriteLine("\nCleaning up...");
            await defaultClient.ExecuteStatementAsync($"DROP TABLE IF EXISTS {TestTableName}");
            await defaultClient.ExecuteStatementAsync($"DROP USER IF EXISTS {readOnlyUsername}");
            Console.WriteLine("Cleanup complete.");
        }
    }

    private static async Task SetupReadOnlyUser(ClickHouseConnection client, string username, string password)
    {
        // Create a read-only user with READONLY = 1
        await client.ExecuteStatementAsync($@"
            CREATE USER {username}
            IDENTIFIED WITH sha256_password BY '{password}'
            DEFAULT DATABASE default
            SETTINGS readonly = 1
        ");

        // Grant access only to SHOW TABLES and SELECT on the test table
        await client.ExecuteStatementAsync($@"
            GRANT SHOW TABLES, SELECT
            ON {TestTableName}
            TO {username}
        ");
    }

    private static async Task SetupTestTable(ClickHouseConnection client)
    {
        await client.ExecuteStatementAsync($@"
            CREATE OR REPLACE TABLE {TestTableName}
            (id UInt64, name String)
            ENGINE MergeTree()
            ORDER BY (id)
        ");

        await client.ExecuteStatementAsync($@"
            INSERT INTO {TestTableName} VALUES
            (12, 'foo'),
            (42, 'bar')
        ");
    }

    /// <summary>
    /// Read-only users CAN select from tables they have been granted access to.
    /// </summary>
    private static async Task DemonstrateAllowedSelect(string username, string password)
    {
        Console.WriteLine("1. Read-only user CAN query granted tables:");

        using var client = new ClickHouseConnection($"Host=localhost;Username={username};Password={password}");
        await client.OpenAsync();

        using var reader = await client.ExecuteReaderAsync($"SELECT * FROM {TestTableName}");
        Console.WriteLine("   Query result:");
        while (reader.Read())
        {
            Console.WriteLine($"   - id: {reader.GetFieldValue<ulong>(0)}, name: {reader.GetString(1)}");
        }

        PrintSeparator();
    }

    /// <summary>
    /// Read-only users CANNOT insert data into any table.
    /// </summary>
    private static async Task DemonstrateInsertBlocked(string username, string password)
    {
        Console.WriteLine("2. Read-only user CANNOT insert data:");

        using var client = new ClickHouseConnection($"Host=localhost;Username={username};Password={password}");
        await client.OpenAsync();

        try
        {
            await client.ExecuteStatementAsync($"INSERT INTO {TestTableName} VALUES (100, 'blocked')");
            Console.WriteLine("   Unexpected success!");
        }
        catch (ClickHouseServerException ex)
        {
            Console.WriteLine($"   [Expected error] {TruncateMessage(ex.Message)}");
        }

        PrintSeparator();
    }

    /// <summary>
    /// Read-only users CANNOT query tables they don't have access to.
    /// </summary>
    private static async Task DemonstrateUnauthorizedTableBlocked(string username, string password)
    {
        Console.WriteLine("3. Read-only user CANNOT query non-granted tables (e.g., system.users):");

        using var client = new ClickHouseConnection($"Host=localhost;Username={username};Password={password}");
        await client.OpenAsync();

        try
        {
            await client.ExecuteReaderAsync("SELECT * FROM system.users LIMIT 5");
            Console.WriteLine("   Unexpected success!");
        }
        catch (ClickHouseServerException ex)
        {
            Console.WriteLine($"   [Expected error] {TruncateMessage(ex.Message)}");
        }

        PrintSeparator();
    }

    /// <summary>
    /// Read-only users CANNOT modify ClickHouse settings like send_progress_in_http_headers.
    /// </summary>
    private static async Task DemonstrateSettingsBlocked(string username, string password)
    {
        Console.WriteLine("4. Read-only user CANNOT use custom ClickHouse settings:");

        using var client = new ClickHouseConnection($"Host=localhost;Username={username};Password={password}");
        await client.OpenAsync();

        using var command = client.CreateCommand();
        command.CommandText = $"SELECT * FROM {TestTableName}";
        command.CustomSettings.Add("send_progress_in_http_headers", 1);

        try
        {
            using var reader = await command.ExecuteReaderAsync();
            Console.WriteLine("   Unexpected success!");
        }
        catch (ClickHouseServerException ex)
        {
            Console.WriteLine($"   [Expected error] {TruncateMessage(ex.Message)}");
        }

        PrintSeparator();
    }

    private static void PrintSeparator()
    {
        Console.WriteLine(new string('-', 72));
    }

    private static string TruncateMessage(string message)
    {
        var firstLine = message.Split('\n')[0];
        return firstLine.Length > 100 ? firstLine[..100] + "..." : firstLine;
    }
}
