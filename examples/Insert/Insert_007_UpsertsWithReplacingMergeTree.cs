using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates upsert patterns using ReplacingMergeTree engine.
///
/// ReplacingMergeTree automatically deduplicates rows with the same sorting key (ORDER BY),
/// keeping only the row with the highest version. This enables performant "upsert" behavior.
///
/// Note that FINAL incurs a performance penalty, see the blog here for tips on managing that: https://clickhouse.com/blog/clickhouse-postgresql-change-data-capture-cdc-part-1#final-performance
///
/// Other specialized MergeTree variants exist as well, see https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/
///
/// Key concepts:
/// - Version column: determines which duplicate to keep (highest wins)
/// - Deleted column: enables soft deletes (row with deleted=1 is removed during merge)
/// - FINAL modifier: forces deduplication at query time
/// - Background merges: deduplication happens asynchronously
/// </summary>
public static class UpsertsWithReplacingMergeTree
{
    private const string TableName = "example_upserts";

    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        await SetupTable(client);

        await InsertInitialData(client);

        await PerformUpserts(client);

        await DemonstrateSoftDeletes(client);

        await ShowQueryingStrategies(client);

        await ForceMergeAndVerify(client);

        await Cleanup(client);
    }

    /// <summary>
    /// Creates a ReplacingMergeTree table with version and deleted columns.
    /// </summary>
    private static async Task SetupTable(ClickHouseClient client)
    {
        Console.WriteLine("1. Creating ReplacingMergeTree table with version and deleted columns:");

        await client.ExecuteNonQueryAsync($@"DROP TABLE IF EXISTS {TableName}");
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {TableName} (
                user_id UInt64,
                email String,
                name String,
                status String,
                version UInt64,
                deleted UInt8 DEFAULT 0,
                updated_at DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(version, deleted)
            ORDER BY (user_id)
        ");

        Console.WriteLine($"   Table '{TableName}' created");
        Console.WriteLine("   - ORDER BY (user_id): rows with same user_id are considered duplicates");
        Console.WriteLine("   - version column: highest version wins during merge");
        Console.WriteLine("   - deleted column: rows with deleted=1 are removed during merge\n");
    }

    /// <summary>
    /// Inserts initial user records.
    /// </summary>
    private static async Task InsertInitialData(ClickHouseClient client)
    {
        Console.WriteLine("2. Inserting initial user records:");

        var rows = new List<object[]>
        {
            new object[] { 1UL, "alice@example.com", "Alice", "active", 1UL },
            new object[] { 2UL, "bob@example.com", "Bob", "active", 1UL },
            new object[] { 3UL, "carol@example.com", "Carol", "pending", 1UL }
        };

        var columns = new[] { "user_id", "email", "name", "status", "version" };
        await client.InsertBinaryAsync(TableName, columns, rows);

        Console.WriteLine("   Inserted 3 users (Alice, Bob, Carol) with version=1\n");
    }

    /// <summary>
    /// Demonstrates the upsert pattern: insert a new row with higher version to "update".
    /// </summary>
    private static async Task PerformUpserts(ClickHouseClient client)
    {
        Console.WriteLine("3. Performing upserts (updates via insert with higher version):");

        // Update Alice's email by inserting a new row with version=2
        Console.WriteLine("   Updating Alice's email (user_id=1) by inserting version=2...");

        var rows = new List<object[]>
        {
            new object[] { 1UL, "alice.smith@example.com", "Alice Smith", "active", 2UL }
        };
        var columns = new[] { "user_id", "email", "name", "status", "version" };
        await client.InsertBinaryAsync(TableName, columns, rows);

        // Show that both versions exist before merge
        var rowCount = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
        Console.WriteLine($"\n   Total rows in table (before merge): {rowCount}");
        Console.WriteLine("   Note: Both old and new versions coexist until background merge runs\n");
    }

    /// <summary>
    /// Demonstrates soft deletes using the deleted column.
    /// </summary>
    private static async Task DemonstrateSoftDeletes(ClickHouseClient client)
    {
        Console.WriteLine("4. Performing soft delete:");

        // Delete Carol by inserting a row with deleted=1
        Console.WriteLine("   Deleting Carol (user_id=3) by inserting version=2 with deleted=1...");

        var rows = new List<object[]>
        {
            new object[] { 3UL, "carol@example.com", "Carol", "pending", 2UL, (byte)1 }
        };
        var columns = new[] { "user_id", "email", "name", "status", "version", "deleted" };
        await client.InsertBinaryAsync(TableName, columns, rows);

        Console.WriteLine("   Soft delete inserted. Row will be removed during next merge.\n");
    }

    /// <summary>
    /// Shows different querying strategies with and without FINAL.
    /// </summary>
    private static async Task ShowQueryingStrategies(ClickHouseClient client)
    {
        Console.WriteLine("5. Querying strategies:");

        // Query WITHOUT FINAL - shows all versions
        Console.WriteLine("\n   a) Query WITHOUT FINAL (shows all row versions):");
        using (var reader = await client.ExecuteReaderAsync(
            $"SELECT user_id, email, name, status, version, deleted FROM {TableName} ORDER BY user_id, version"))
        {
            Console.WriteLine("      user_id | email                      | name        | status  | ver | del");
            Console.WriteLine("      --------|----------------------------|-------------|---------|-----|----");
            while (reader.Read())
            {
                var userId = reader.GetFieldValue<ulong>(0);
                var email = reader.GetString(1);
                var name = reader.GetString(2);
                var status = reader.GetString(3);
                var version = reader.GetFieldValue<ulong>(4);
                var deleted = reader.GetByte(5);
                Console.WriteLine($"      {userId,-7} | {email,-26} | {name,-11} | {status,-7} | {version,-3} | {deleted}");
            }
        }

        // Query WITH FINAL - shows deduplicated view
        Console.WriteLine("\n   b) Query WITH FINAL (deduplicated, deleted rows removed):");
        using (var reader = await client.ExecuteReaderAsync(
            $"SELECT user_id, email, name, status, version FROM {TableName} FINAL ORDER BY user_id"))
        {
            Console.WriteLine("      user_id | email                      | name        | status  | ver");
            Console.WriteLine("      --------|----------------------------|-------------|---------|----");
            while (reader.Read())
            {
                var userId = reader.GetFieldValue<ulong>(0);
                var email = reader.GetString(1);
                var name = reader.GetString(2);
                var status = reader.GetString(3);
                var version = reader.GetFieldValue<ulong>(4);
                Console.WriteLine($"      {userId,-7} | {email,-26} | {name,-11} | {status,-7} | {version}");
            }
        }

        Console.WriteLine("\n   Note: FINAL has a small performance overhead but guarantees consistent results.\n");
    }

    /// <summary>
    /// Forces a merge to physically collapse duplicates.
    /// </summary>
    private static async Task ForceMergeAndVerify(ClickHouseClient client)
    {
        Console.WriteLine("6. Forcing merge to physically deduplicate:");

        var beforeCount = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
        Console.WriteLine($"   Rows before OPTIMIZE: {beforeCount}");

        await client.ExecuteNonQueryAsync($"OPTIMIZE TABLE {TableName} FINAL");
        Console.WriteLine("   Executed: OPTIMIZE TABLE ... FINAL");

        var afterCount = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
        Console.WriteLine($"   Rows after OPTIMIZE: {afterCount}");

        Console.WriteLine("\n   After merge, the table physically contains only the latest versions.");
        Console.WriteLine("   Carol (deleted=1) has been removed entirely.\n");
    }


    private static async Task Cleanup(ClickHouseClient client)
    {
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {TableName}");
        Console.WriteLine($"Table '{TableName}' dropped");
    }
}
