using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates inserting data using EPHEMERAL columns.
///
/// A ClickHouse EPHEMERAL column is a special type of column that accepts data during an INSERT
/// operation but is not stored in the table and cannot be queried directly. Its sole purpose is
/// to provide intermediate input for the default value expressions of other, persistent columns.
///
/// This is useful for:
/// - Transforming input data before storage
/// - Providing default values based on input that shouldn't be persisted
/// - Deriving multiple columns from a single input value
///
/// See https://clickhouse.com/docs/sql-reference/statements/create/table#ephemeral
/// </summary>
public static class EphemeralColumns
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        Console.WriteLine("Ephemeral Columns Examples\n");

        Console.WriteLine("1. Multiple derived columns from ephemeral input:");

        var tableName = "example_ephemeral_derived";

        // Create table where multiple columns are derived from ephemeral inputs
        await client.ExecuteNonQueryAsync($@"
            CREATE OR REPLACE TABLE {tableName}
            (
                id              UInt64,
                full_name       String DEFAULT concat(first_name, ' ', last_name),
                name_length     UInt32 DEFAULT length(concat(first_name, last_name)),
                first_name      String EPHEMERAL,
                last_name       String EPHEMERAL
            )
            ENGINE MergeTree()
            ORDER BY (id)
        ");

        Console.WriteLine("   Created table with schema:");
        Console.WriteLine("   - id: UInt64");
        Console.WriteLine("   - full_name: String DEFAULT concat(first_name, ' ', last_name)");
        Console.WriteLine("   - name_length: UInt32 DEFAULT length(concat(first_name, last_name))");
        Console.WriteLine("   - first_name: String EPHEMERAL");
        Console.WriteLine("   - last_name: String EPHEMERAL\n");

        // Insert with ephemeral columns using InsertBinaryAsync
        // Note: Must specify the ephemeral columns explicitly
        Console.WriteLine("   Inserting data with first_name and last_name:");

        var rows = new List<object[]>
        {
            new object[] { 1UL, "Alice", "Smith" },
            new object[] { 2UL, "Bob", "Johnson" },
            new object[] { 3UL, "Carol", "Williams" }
        };

        var columns = new[] { "id", "first_name", "last_name" };
        await client.InsertBinaryAsync(tableName, columns, rows);

        Console.WriteLine("   - Inserted 3 rows with ephemeral columns\n");

        // Query results - derived columns are stored, ephemeral columns are not
        Console.WriteLine("   Query results (derived columns are stored, ephemeral are not):");
        using (var reader = await client.ExecuteReaderAsync($"SELECT * FROM {tableName} ORDER BY id"))
        {
            Console.WriteLine("   ID\tFull Name\t\tName Length");
            Console.WriteLine("   --\t---------\t\t-----------");
            while (reader.Read())
            {
                Console.WriteLine($"   {reader.GetFieldValue<ulong>(0)}\t{reader.GetString(1),-20}\t{reader.GetFieldValue<uint>(2)}");
            }
        }

        // Cleanup
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\n   Table dropped\n");

        Console.WriteLine("All ephemeral column examples completed!");
    }

}
