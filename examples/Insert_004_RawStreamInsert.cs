using System.Text;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates inserting raw data streams using InsertRawStreamAsync.
/// This is useful for loading data from files or in-memory sources in formats like CSV, JSON, etc.
/// See the format settings in the docs for ways to control the ingestion: https://clickhouse.com/docs/operations/settings/formats
/// </summary>
public static class RawStreamInsert
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        await InsertFromFile(connection);
        await InsertFromMemory(connection);
    }

    /// <summary>
    /// Demonstrates inserting data from a CSV file stream.
    /// </summary>
    private static async Task InsertFromFile(ClickHouseConnection connection)
    {
        var tableName = "example_raw_stream_file";

        // Create a test table matching the CSV structure
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                Id UInt64,
                Name String,
                Value Float32
            )
            ENGINE = MergeTree()
            ORDER BY (Id)
        ");

        Console.WriteLine($"Table '{tableName}' created\n");
        
        var csvFile = Path.Combine(AppContext.BaseDirectory, "resources", "people.csv");

        Console.WriteLine("1. Inserting from CSV file stream:");
        Console.WriteLine($"   File: {csvFile}");
        Console.WriteLine($"   Content:\n{await File.ReadAllTextAsync(csvFile)}");

        // Open a file stream and insert directly
        // ClickHouse 23.1+ automatically detects CSV headers, so plain "CSV" format works
        // For older versions, use "CSVWithNames" or use the setting input_format_csv_skip_first_lines
        await using (var fileStream = File.OpenRead(csvFile))
        {
            using var response = await connection.InsertRawStreamAsync(
                table: tableName,
                stream: fileStream,
                format: "CSV");

            Console.WriteLine($"   Insert completed with status: {response.StatusCode}\n");
        }

        // Query and display results
        Console.WriteLine("   Querying inserted data:");
        using (var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {tableName} ORDER BY Id"))
        {
            Console.WriteLine("   ID\tName\t\tValue");
            Console.WriteLine("   --\t----\t\t-----");

            while (reader.Read())
            {
                var id = reader.GetFieldValue<ulong>(0);
                var name = reader.GetString(1);
                var value = reader.GetFloat(2);

                Console.WriteLine($"   {id}\t{name,-12}\t{value:F1}");
            }
        }

        var totalCount = await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Console.WriteLine($"\n   Total rows: {totalCount}");

        // Clean up table
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\n   Table '{tableName}' dropped\n");
    }

    /// <summary>
    /// Demonstrates inserting data from in-memory streams in various formats.
    /// </summary>
    private static async Task InsertFromMemory(ClickHouseConnection connection)
    {
        var tableName = "example_raw_stream_memory";

        // Create a test table
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                product String,
                price Float32,
                in_stock UInt8
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");

        Console.WriteLine($"Table '{tableName}' created\n");

        // Insert from JSONEachRow format in memory
        Console.WriteLine("2. Inserting from JSONEachRow string in memory:");
        var jsonData = """
            {"id": 1, "product": "Contraption", "price": 49.99, "in_stock": 1}
            {"id": 2, "product": "Apparatus", "price": 59.99, "in_stock": 0}
            """;

        Console.WriteLine($"   JSON data:\n{jsonData}\n");

        using (var jsonStream = new MemoryStream(Encoding.UTF8.GetBytes(jsonData)))
        {
            using var response = await connection.InsertRawStreamAsync(
                table: tableName,
                stream: jsonStream,
                format: "JSONEachRow",
                columns: ["id", "product", "price", "in_stock"]);

            Console.WriteLine($"   JSON insert completed with status: {response.StatusCode}\n");
        }

        // Query and display all results
        Console.WriteLine("   Querying all inserted data:");
        using (var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {tableName} ORDER BY id"))
        {
            Console.WriteLine("   ID\tProduct\t\t\tPrice\t\tIn Stock");
            Console.WriteLine("   --\t-------\t\t\t-----\t\t--------");

            while (reader.Read())
            {
                var id = reader.GetFieldValue<ulong>(0);
                var product = reader.GetString(1);
                var price = reader.GetFloat(2);
                var inStock = reader.GetFieldValue<byte>(3) == 1 ? "Yes" : "No";

                Console.WriteLine($"   {id}\t{product,-16}\t${price,-10:F2}\t{inStock}");
            }
        }

        var totalCount = await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Console.WriteLine($"\n   Total rows: {totalCount}");

        // Clean up table
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\n   Table '{tableName}' dropped");
    }
}
