using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates upsert patterns using ReplacingMergeTree engine.
///
/// ReplacingMergeTree automatically deduplicates rows with the same sorting key (ORDER BY),
/// keeping only the row with the highest version. This enables performant "upsert" behavior.
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
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        await SetupTable(connection);

        await InsertInitialData(connection);

        await PerformUpserts(connection);

        await DemonstrateSoftDeletes(connection);

        await ShowQueryingStrategies(connection);

        await ForceMergeAndVerify(connection);

        await Cleanup(connection);
    }

    /// <summary>
    /// Creates a ReplacingMergeTree table with version and deleted columns.
    /// </summary>
    private static async Task SetupTable(ClickHouseConnection connection)
    {
        Console.WriteLine("1. Creating ReplacingMergeTree table with version and deleted columns:");

        await connection.ExecuteStatementAsync($@"DROP TABLE IF EXISTS {TableName}");
        await connection.ExecuteStatementAsync($@"
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
    private static async Task InsertInitialData(ClickHouseConnection connection)
    {
        Console.WriteLine("2. Inserting initial user records:");

        using var command = connection.CreateCommand();
        command.CommandText = $@"
            INSERT INTO {TableName} (user_id, email, name, status, version)
            VALUES
                ({{id1:UInt64}}, {{email1:String}}, {{name1:String}}, {{status1:String}}, {{ver1:UInt64}}),
                ({{id2:UInt64}}, {{email2:String}}, {{name2:String}}, {{status2:String}}, {{ver2:UInt64}}),
                ({{id3:UInt64}}, {{email3:String}}, {{name3:String}}, {{status3:String}}, {{ver3:UInt64}})
        ";

        command.AddParameter("id1", 1UL);
        command.AddParameter("email1", "alice@example.com");
        command.AddParameter("name1", "Alice");
        command.AddParameter("status1", "active");
        command.AddParameter("ver1", 1UL);

        command.AddParameter("id2", 2UL);
        command.AddParameter("email2", "bob@example.com");
        command.AddParameter("name2", "Bob");
        command.AddParameter("status2", "active");
        command.AddParameter("ver2", 1UL);

        command.AddParameter("id3", 3UL);
        command.AddParameter("email3", "carol@example.com");
        command.AddParameter("name3", "Carol");
        command.AddParameter("status3", "pending");
        command.AddParameter("ver3", 1UL);

        await command.ExecuteNonQueryAsync();
        Console.WriteLine("   Inserted 3 users (Alice, Bob, Carol) with version=1\n");
    }

    /// <summary>
    /// Demonstrates the upsert pattern: insert a new row with higher version to "update".
    /// </summary>
    private static async Task PerformUpserts(ClickHouseConnection connection)
    {
        Console.WriteLine("3. Performing upserts (updates via insert with higher version):");

        // Update Alice's email by inserting a new row with version=2
        Console.WriteLine("   Updating Alice's email (user_id=1) by inserting version=2...");
        await connection.ExecuteStatementAsync($@"
            INSERT INTO {TableName} (user_id, email, name, status, version)
            VALUES (1, 'alice.smith@example.com', 'Alice Smith', 'active', 2)
        ");

        // Show that both versions exist before merge
        var rowCount = await connection.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
        Console.WriteLine($"\n   Total rows in table (before merge): {rowCount}");
        Console.WriteLine("   Note: Both old and new versions coexist until background merge runs\n");
    }

    /// <summary>
    /// Demonstrates soft deletes using the deleted column.
    /// </summary>
    private static async Task DemonstrateSoftDeletes(ClickHouseConnection connection)
    {
        Console.WriteLine("4. Performing soft delete:");

        // Delete Carol by inserting a row with deleted=1
        Console.WriteLine("   Deleting Carol (user_id=3) by inserting version=2 with deleted=1...");
        await connection.ExecuteStatementAsync($@"
            INSERT INTO {TableName} (user_id, email, name, status, version, deleted)
            VALUES (3, 'carol@example.com', 'Carol', 'pending', 2, 1)
        ");

        Console.WriteLine("   Soft delete inserted. Row will be removed during next merge.\n");
    }

    /// <summary>
    /// Shows different querying strategies with and without FINAL.
    /// </summary>
    private static async Task ShowQueryingStrategies(ClickHouseConnection connection)
    {
        Console.WriteLine("5. Querying strategies:");

        // Query WITHOUT FINAL - shows all versions
        Console.WriteLine("\n   a) Query WITHOUT FINAL (shows all row versions):");
        using (var reader = await connection.ExecuteReaderAsync(
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
        using (var reader = await connection.ExecuteReaderAsync(
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
    private static async Task ForceMergeAndVerify(ClickHouseConnection connection)
    {
        Console.WriteLine("6. Forcing merge to physically deduplicate:");

        var beforeCount = await connection.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
        Console.WriteLine($"   Rows before OPTIMIZE: {beforeCount}");

        await connection.ExecuteStatementAsync($"OPTIMIZE TABLE {TableName} FINAL");
        Console.WriteLine("   Executed: OPTIMIZE TABLE ... FINAL");

        var afterCount = await connection.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
        Console.WriteLine($"   Rows after OPTIMIZE: {afterCount}");

        Console.WriteLine("\n   After merge, the table physically contains only the latest versions.");
        Console.WriteLine("   Carol (deleted=1) has been removed entirely.\n");
    }


    private static async Task Cleanup(ClickHouseConnection connection)
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {TableName}");
        Console.WriteLine($"Table '{TableName}' dropped");
    }
}
