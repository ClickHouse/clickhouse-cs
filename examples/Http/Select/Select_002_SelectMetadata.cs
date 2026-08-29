using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates different output formats available when querying data from ClickHouse.
/// </summary>
public static class SelectMetadata
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        var tableName = "example_formats";

        // Create and populate a test table
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                name String,
                values Array(UInt32)
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");

        Console.WriteLine($"Created table '{tableName}'\n");

        // Insert sample data
        var rows = new List<object[]>
        {
            new object[] { 1u, "Example", new uint[] { 10, 20, 30 } }
        };
        var columns = new[] { "id", "name", "values" };
        await client.InsertBinaryAsync(tableName, columns, rows);

        Console.WriteLine("Inserted sample data\n");

        // Example 1: Reading data field by field
        Console.WriteLine("\n1. Column metadata:");
        using (var reader = await client.ExecuteReaderAsync($"SELECT * FROM {tableName} LIMIT 1"))
        {
            if (reader.Read())
            {
                Console.WriteLine($"   Field count: {reader.FieldCount}");
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var fieldName = reader.GetName(i);
                    var fieldType = reader.GetFieldType(i);
                    var fieldValue = reader.GetValue(i);
                    Console.WriteLine($"   Field {i}: {fieldName} (Type: {fieldType.Name}) = {fieldValue}");
                }
            }
        }

        // Clean up
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\nTable '{tableName}' dropped");
    }
}
