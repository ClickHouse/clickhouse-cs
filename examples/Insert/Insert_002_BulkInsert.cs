using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates high-performance bulk data insertion using InsertBinaryAsync.
/// This is the recommended approach for inserting large amounts of data efficiently.
/// </summary>
public static class BulkInsert
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        var tableName = "example_bulk_insert";

        // Create a test table
        await client.ExecuteNonQueryAsync($@"
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

        // Example 1: Bulk insert with default options
        Console.WriteLine("1. Bulk inserting with InsertBinaryAsync:");
        var columns = new[] { "id", "product_name", "category", "price", "quantity", "sale_date" };
        var data = GenerateSampleData(10000, startId: 1);

        var rowsInserted = await client.InsertBinaryAsync(tableName, columns, data);
        Console.WriteLine($"   Inserted {rowsInserted} rows\n");

        // Example 2: Bulk insert with custom options (batch size, parallelism)
        Console.WriteLine("2. Bulk inserting with custom InsertOptions:");
        var options = new InsertOptions
        {
            BatchSize = 5000,              // Rows per batch
            MaxDegreeOfParallelism = 4,    // Parallel batch uploads
        };

        var moreData = GenerateSampleData(10000, startId: 10001);
        rowsInserted = await client.InsertBinaryAsync(tableName, columns, moreData, options);
        Console.WriteLine($"   Inserted {rowsInserted} rows with custom options\n");

        // Example 3: Bulk insert with specific columns (others use defaults)
        Console.WriteLine("3. Bulk inserting with specific columns:");
        var partialTableName = "example_bulk_partial";
        await client.ExecuteNonQueryAsync($@"
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

        var partialColumns = new[] { "id", "name" };
        var partialData = new List<object[]>
        {
            new object[] { 1UL, "Item 1" },
            new object[] { 2UL, "Item 2" },
            new object[] { 3UL, "Item 3" },
        };

        rowsInserted = await client.InsertBinaryAsync(partialTableName, partialColumns, partialData);
        Console.WriteLine($"   Inserted {rowsInserted} rows with partial columns\n");

        // Query and display sample results
        Console.WriteLine("Sample data from main table:");
        using (var reader = await client.ExecuteReaderAsync($"SELECT * FROM {tableName} ORDER BY id LIMIT 5"))
        {
            Console.WriteLine("ID\tProduct Name\t\tCategory\tPrice\t\tQuantity");
            Console.WriteLine("--\t------------\t\t--------\t-----\t\t--------");

            while (reader.Read())
            {
                var id = reader.GetFieldValue<ulong>(0);
                var productName = reader.GetString(1);
                var category = reader.GetString(2);
                var price = reader.GetFloat(3);
                var quantity = reader.GetFieldValue<uint>(4);

                Console.WriteLine($"{id}\t{productName,-20}\t{category,-15}\t${price,-10:F2}\t{quantity}");
            }
        }

        // Get total row counts
        var totalCount = await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Console.WriteLine($"\nTotal rows in {tableName}: {totalCount}");

        var partialCount = await client.ExecuteScalarAsync($"SELECT count() FROM {partialTableName}");
        Console.WriteLine($"Total rows in {partialTableName}: {partialCount}");

        // Clean up
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {partialTableName}");
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
