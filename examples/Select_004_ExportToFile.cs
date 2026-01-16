using ClickHouse.Driver.ADO;
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
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        var tableName = "example_export";

        // Create and populate a test table
        await connection.ExecuteStatementAsync($@"
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

        await connection.ExecuteStatementAsync($@"
            INSERT INTO {tableName} VALUES
            (1, 'Alice Johnson', 'Engineering', 95000),
            (2, 'Bob Smith', 'Sales', 75000),
            (3, 'Carol White', 'Engineering', 105000),
            (4, 'David Brown', 'Marketing', 68000),
            (5, 'Eve Davis', 'Engineering', 88000)
        ");

        Console.WriteLine($"Created and populated table '{tableName}'\n");

        await ExportToJsonEachRow(connection, tableName);
        await ExportToParquetFile(connection, tableName);

        // Clean up
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\nTable '{tableName}' dropped");
    }

    /// <summary>
    /// Demonstrates exporting query results as JSONEachRow format to memory.
    /// </summary>
    private static async Task ExportToJsonEachRow(ClickHouseConnection connection, string tableName)
    {
        Console.WriteLine("1. Export to JSONEachRow (in memory):");

        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM {tableName} FORMAT JSONEachRow";

        using var result = await command.ExecuteRawResultAsync(CancellationToken.None);

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
    private static async Task ExportToParquetFile(ClickHouseConnection connection, string tableName)
    {
        Console.WriteLine("2. Export to Parquet file:");

        var parquetFile = Path.Combine(Path.GetTempPath(), $"clickhouse_export_{Guid.NewGuid()}.parquet");

        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT * FROM {tableName} FORMAT Parquet";

            using var result = await command.ExecuteRawResultAsync(CancellationToken.None);

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
