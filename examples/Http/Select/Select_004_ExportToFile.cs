using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates exporting query results to different formats using ExecuteRawResultAsync.
/// This is useful for exporting data to files (CSV, Parquet, JSON, etc.) or processing raw output.
/// See the format settings in the docs: https://clickhouse.com/docs/interfaces/formats
/// </summary>
public static class ExportToFile
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        var tableName = "example_export";

        // Create and populate a test table
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                name String,
                department String,
                salary Float32
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");

        var rows = new List<object[]>
        {
            new object[] { 1UL, "Alice Johnson", "Engineering", 95000f },
            new object[] { 2UL, "Bob Smith", "Sales", 75000f },
            new object[] { 3UL, "Carol White", "Engineering", 105000f },
            new object[] { 4UL, "David Brown", "Marketing", 68000f },
            new object[] { 5UL, "Eve Davis", "Engineering", 88000f }
        };
        var columns = new[] { "id", "name", "department", "salary" };
        await client.InsertBinaryAsync(tableName, columns, rows);

        Console.WriteLine($"Created and populated table '{tableName}'\n");

        await ExportToJsonEachRow(client, tableName);
        await ExportToParquetFile(client, tableName);

        // Clean up
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\nTable '{tableName}' dropped");
    }

    /// <summary>
    /// Demonstrates exporting query results as JSONEachRow format to memory.
    /// </summary>
    private static async Task ExportToJsonEachRow(ClickHouseClient client, string tableName)
    {
        Console.WriteLine("1. Export to JSONEachRow (in memory):");

        using var result = await client.ExecuteRawResultAsync($"SELECT * FROM {tableName} FORMAT JSONEachRow");

        // Read the entire response as a string
        var json = await result.ReadAsStringAsync();

        Console.WriteLine("   Raw JSON output:");
        foreach (var line in json.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            Console.WriteLine($"   {line}");
        }
        Console.WriteLine();
    }

    /// <summary>
    /// Demonstrates exporting query results as Parquet format to a file.
    /// </summary>
    private static async Task ExportToParquetFile(ClickHouseClient client, string tableName)
    {
        Console.WriteLine("2. Export to Parquet file:");

        var parquetFile = Path.Combine(Path.GetTempPath(), $"clickhouse_export_{Guid.NewGuid()}.parquet");

        try
        {
            using var result = await client.ExecuteRawResultAsync($"SELECT * FROM {tableName} FORMAT Parquet");

            // Stream directly to file
            await using (var fileStream = File.Create(parquetFile))
            {
                await result.CopyToAsync(fileStream);
            }

            var fileInfo = new FileInfo(parquetFile);
            Console.WriteLine($"   Exported to: {parquetFile}");
            Console.WriteLine($"   File size: {fileInfo.Length} bytes");
        }
        finally
        {
            // Clean up the temporary file
            if (File.Exists(parquetFile))
            {
                File.Delete(parquetFile);
                Console.WriteLine($"\n   Cleaned up temporary file: {parquetFile}");
            }
        }
    }
}
