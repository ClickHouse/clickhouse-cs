namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates query optimization options for InsertBinaryAsync.
/// By default, InsertBinaryAsync queries the table schema before every insert.
/// These options eliminate that overhead for repeated inserts.
/// </summary>
public static class SchemaOptimization
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        var tableName = "example_schema_optimization";

        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                name String,
                score Float32
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");

        Console.WriteLine($"Table '{tableName}' created\n");

        // ---------------------------------------------------------------
        // Option 1: Provide column types explicitly (no schema query at all)
        // ---------------------------------------------------------------
        Console.WriteLine("1. Insert with explicit ColumnTypes (zero schema queries):");

        var options = new InsertOptions
        {
            ColumnTypes = new Dictionary<string, string>
            {
                ["id"] = "UInt64",
                ["name"] = "String",
                ["score"] = "Float32",
            },
        };

        var rows = GenerateSampleData(1000, startId: 1);
        // Note: if ColumnTypes is set in InsertOptions, you must provide a list of columns in InsertBinaryAsync().
        var inserted = await client.InsertBinaryAsync(tableName, ["id", "name", "score"], rows, options);
        Console.WriteLine($"   Inserted {inserted} rows\n");

        // ---------------------------------------------------------------
        // Option 2: Cache the schema (one query, reused across inserts)
        // ---------------------------------------------------------------
        Console.WriteLine("2. Insert with UseSchemaCache (schema queried once, then cached):");

        var cachedOptions = new InsertOptions { UseSchemaCache = true };

        // First insert queries the schema
        var batch1 = GenerateSampleData(500, startId: 1001);
        inserted = await client.InsertBinaryAsync(tableName, ["id", "name", "score"], batch1, cachedOptions);
        Console.WriteLine($"   Batch 1: inserted {inserted} rows (schema was fetched)");

        // Second insert reuses the cached schema — no extra round-trip
        var batch2 = GenerateSampleData(500, startId: 1501);
        inserted = await client.InsertBinaryAsync(tableName, ["id", "name", "score"], batch2, cachedOptions);
        Console.WriteLine($"   Batch 2: inserted {inserted} rows (schema reused from cache)\n");

        // Verify
        var count = await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Console.WriteLine($"Total rows in {tableName}: {count}");

        // Clean up
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine("Table dropped");
    }

    private static IEnumerable<object[]> GenerateSampleData(int count, ulong startId = 1)
    {
        var random = new Random(42);

        for (ulong i = 0; i < (ulong)count; i++)
        {
            var id = startId + i;
            yield return new object[] { id, $"Item_{id}", (float)(random.NextDouble() * 100) };
        }
    }
}
