using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates high-performance bulk data insertion using ClickHouseBulkCopy.
/// This is the recommended approach for inserting large amounts of data efficiently.
/// </summary>
public static class BulkInsert
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        var tableName = "example_bulk_insert";

        // Create a test table
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                product_name String,
                category String,
                price Float32,
                quantity UInt32,
                sale_date DateTime
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");

        Console.WriteLine($"Table '{tableName}' created\n");

        // Example 1: Bulk insert for large data sets
        Console.WriteLine("1. Bulk inserting:");
        using (var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
            BatchSize = 1000, // Number of rows per batch. Due to the way the MergeTree table works, it is recommended to insert data in large batches.
            MaxDegreeOfParallelism = 4 // Use parallel processing for better performance
        })
        {
            // Track progress with BatchSent event
            bulkCopy.BatchSent += (sender, e) =>
            {
                Console.WriteLine($"   Batch sent: {e.RowsWritten} rows written");
            };

            await bulkCopy.InitAsync();

            // Generate data
            var largeData = GenerateSampleData(10000, startId: 6);

            await bulkCopy.WriteToServerAsync(largeData);
            Console.WriteLine($"   Total rows inserted: {bulkCopy.RowsWritten}\n");
        }

        // Example 2: Bulk insert with specific columns
        Console.WriteLine("2. Bulk inserting with specific columns:");
        var partialTableName = "example_bulk_partial";
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {partialTableName}
            (
                id UInt64,
                name String,
                value Float32 DEFAULT 0.0,
                created_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");

        using (var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = partialTableName,
            ColumnNames = new[] { "id", "name" } // Only specify these columns, others use defaults
        })
        {
            await bulkCopy.InitAsync();

            var partialData = new List<object[]>
            {
                new object[] { 1UL, "Item 1" },
                new object[] { 2UL, "Item 2" },
                new object[] { 3UL, "Item 3" }
            };

            await bulkCopy.WriteToServerAsync(partialData);
            Console.WriteLine($"   Inserted {bulkCopy.RowsWritten} rows with partial columns\n");
        }

        // Query and display sample results
        Console.WriteLine("Sample data from main table:");
        using (var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {tableName} ORDER BY id LIMIT 5"))
        {
            Console.WriteLine("ID\tProduct Name\t\tCategory\tPrice\t\tQuantity");
            Console.WriteLine("--\t------------\t\t--------\t-----\t\t--------");

            while (reader.Read())
            {
                var id = reader.GetFieldValue<UInt64>(0);
                var productName = reader.GetString(1);
                var category = reader.GetString(2);
                var price = reader.GetFloat(3);
                var quantity = reader.GetFieldValue<UInt32>(4);

                Console.WriteLine($"{id}\t{productName,-20}\t{category,-15}\t${price,-10:F2}\t{quantity}");
            }
        }

        // Get total row counts
        var totalCount = await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Console.WriteLine($"\nTotal rows in {tableName}: {totalCount}");

        var partialCount = await connection.ExecuteScalarAsync($"SELECT count() FROM {partialTableName}");
        Console.WriteLine($"Total rows in {partialTableName}: {partialCount}");

        // Clean up
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {partialTableName}");
        Console.WriteLine($"\nTables dropped");
    }

    private static IEnumerable<object[]> GenerateSampleData(int count, ulong startId = 1)
    {
        var random = new Random(42);
        var categories = new[] { "Electronics", "Furniture", "Clothing", "Books", "Toys" };
        var productPrefixes = new[] { "Premium", "Deluxe", "Standard", "Economy", "Budget" };
        var productTypes = new[] { "Widget", "Gadget", "Device", "Tool", "Item" };

        for (ulong i = 0; i < (ulong)count; i++)
        {
            var id = startId + i;
            var prefix = productPrefixes[random.Next(productPrefixes.Length)];
            var type = productTypes[random.Next(productTypes.Length)];
            var productName = $"{prefix} {type} #{id}";
            var category = categories[random.Next(categories.Length)];
            var price = (float)(random.NextDouble() * 1000 + 10);
            var quantity = (uint)random.Next(1, 100);
            var saleDate = DateTime.UtcNow.AddDays(-random.Next(0, 365));

            yield return new object[] { id, productName, category, price, quantity, saleDate };
        }
    }
}
