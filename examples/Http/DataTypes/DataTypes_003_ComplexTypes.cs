using System.Net;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates working with complex ClickHouse data types.
/// Covers:
/// - Arrays (Array(T))
/// - Maps (Map(K, V))
/// - Tuples (Tuple(T1, T2, ...))
/// - UUIDs
/// - IP Addresses (IPv4, IPv6)
/// - Nested structures
/// </summary>
public static class ComplexTypes
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        Console.WriteLine("Complex Data Types Examples\n");

        // Example 1: Arrays
        Console.WriteLine("1. Working with Arrays:");
        await Example1_Arrays(client);

        // Example 2: Maps
        Console.WriteLine("\n2. Working with Maps:");
        await Example2_Maps(client);

        // Example 3: Tuples
        Console.WriteLine("\n3. Working with Tuples:");
        await Example3_Tuples(client);

        // Example 4: IP Addresses
        Console.WriteLine("\n4. Working with IP Addresses:");
        await Example4_IPAddresses(client);

        // Example 5: Nested structures
        Console.WriteLine("\n5. Working with Nested structures:");
        await Example5_Nested(client);

        Console.WriteLine("\nAll complex data types examples completed!");
    }

    private static async Task Example1_Arrays(ClickHouseClient client)
    {
        var tableName = "example_arrays";

        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                tags Array(String),
                numbers Array(Int32),
                scores Array(Float64)
            )
            ENGINE = MergeTree()
            ORDER BY id
        ");

        Console.WriteLine($"   Created table '{tableName}' with Array columns");

        // Insert data with arrays using InsertBinaryAsync
        var rows = new List<object[]>
        {
            new object[] { 1u, new[] { "important", "urgent", "review" }, new[] { 10, 20, 30, 40 }, new[] { 95.5, 87.3, 92.1 } }
        };
        var columns = new[] { "id", "tags", "numbers", "scores" };
        await client.InsertBinaryAsync(tableName, columns, rows);

        Console.WriteLine("   Inserted rows with array data");

        // Query and read arrays
        using (var reader = await client.ExecuteReaderAsync($"SELECT id, tags, numbers, scores FROM {tableName} ORDER BY id"))
        {
            Console.WriteLine("\n   Reading array data:");
            while (reader.Read())
            {
                var id = reader.GetFieldValue<uint>(0);
                var tags = reader.GetFieldValue<string[]>(1);
                var numbers = reader.GetFieldValue<int[]>(2);
                var scores = reader.GetFieldValue<double[]>(3);

                Console.WriteLine($"\n   ID: {id}");
                Console.WriteLine($"     Tags: [{string.Join(", ", tags)}]");
                Console.WriteLine($"     Numbers: [{string.Join(", ", numbers)}]");
                Console.WriteLine($"     Scores: [{string.Join(", ", scores.Select(s => s.ToString("F1")))}]");
            }
        }

        // Array functions
        Console.WriteLine("\n   Using array functions:");
        var arrayLength = await client.ExecuteScalarAsync($"SELECT length(tags) FROM {tableName} WHERE id = 1");
        Console.WriteLine($"     Length of tags array for ID=1: {arrayLength}");

        var hasElement = await client.ExecuteScalarAsync($"SELECT has(tags, 'urgent') FROM {tableName} WHERE id = 1");
        Console.WriteLine($"     Does ID=1 have 'urgent' tag? {hasElement}");

        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    private static async Task Example2_Maps(ClickHouseClient client)
    {
        var tableName = "example_maps";

        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                user_id UInt32,
                preferences Map(String, String),
            )
            ENGINE = MergeTree()
            ORDER BY user_id
        ");

        Console.WriteLine($"   Created table '{tableName}' with Map columns");

        // Insert data with maps using InsertBinaryAsync
        var rows = new List<object[]>
        {
            new object[] { 1u, new Dictionary<string, string>
            {
                { "theme", "dark" },
                { "language", "en" },
                { "timezone", "UTC" },
            }}
        };
        var columns = new[] { "user_id", "preferences" };
        await client.InsertBinaryAsync(tableName, columns, rows);

        Console.WriteLine("   Inserted row with map data");

        // Query and read maps
        using (var reader = await client.ExecuteReaderAsync($"SELECT user_id, preferences FROM {tableName}"))
        {
            Console.WriteLine("\n   Reading map data:");
            while (reader.Read())
            {
                var userId = reader.GetFieldValue<uint>(0);
                var preferences = reader.GetFieldValue<Dictionary<string, string>>(1);

                Console.WriteLine($"\n   User ID: {userId}");
                Console.WriteLine("     Preferences:");
                foreach (var (key, value) in preferences)
                {
                    Console.WriteLine($"       {key}: {value}");
                }
            }
        }

        // Map functions
        Console.WriteLine("\n   Using map functions:");
        var prefValue = await client.ExecuteScalarAsync($"SELECT preferences['theme'] FROM {tableName} WHERE user_id = 1");
        Console.WriteLine($"     Theme preference: {prefValue}");

        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    private static async Task Example3_Tuples(ClickHouseClient client)
    {
        var tableName = "example_tuples";

        // Tuples can be named, or not
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                person Tuple(name String, age UInt8, email String),
                coordinates Tuple(x Float64, y Float64, z Float64),
                point Tuple(Int32, Int32)
            )
            ENGINE = MergeTree()
            ORDER BY id
        ");

        Console.WriteLine($"   Created table '{tableName}' with Tuple columns");

        // Insert data with tuples using InsertBinaryAsync
        var rows = new List<object[]>
        {
            new object[] {
                1u,
                Tuple.Create("Alice Johnson", (byte)30, "alice@example.com"),
                Tuple.Create(12.5, 34.7, 56.9),
                Tuple.Create(1, 2)
            }
        };
        var columns = new[] { "id", "person", "coordinates", "point" };
        await client.InsertBinaryAsync(tableName, columns, rows);

        Console.WriteLine("   Inserted row with tuple data");

        // Query and read tuples
        using (var reader = await client.ExecuteReaderAsync($"SELECT id, person, coordinates, point FROM {tableName}"))
        {
            Console.WriteLine("\n   Reading tuple data:");
            while (reader.Read())
            {
                var id = reader.GetFieldValue<uint>(0);
                var person = reader.GetFieldValue<Tuple<string, byte, string>>(1);
                var coordinates = reader.GetFieldValue<Tuple<double, double, double>>(2);
                var point = reader.GetFieldValue<Tuple<int, int>>(3);

                Console.WriteLine($"\n   ID: {id}");
                Console.WriteLine($"     Person: {person.Item1}, Age {person.Item2}, Email {person.Item3}");
                Console.WriteLine($"     Coordinates: ({coordinates.Item1}, {coordinates.Item2}, {coordinates.Item3})");
                Console.WriteLine($"     Point: ({point.Item1}, {point.Item2})");
            }
        }

        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    private static async Task Example4_IPAddresses(ClickHouseClient client)
    {
        var tableName = "example_ip_addresses";

        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                ipv4_addr IPv4,
                ipv6_addr IPv6,
                request_time DateTime
            )
            ENGINE = MergeTree()
            ORDER BY id
        ");

        Console.WriteLine($"   Created table '{tableName}' with IPv4 and IPv6 columns");

        // Insert data with IP addresses using InsertBinaryAsync
        var rows = new List<object[]>
        {
            new object[] { 1u, IPAddress.Parse("192.168.1.100"), IPAddress.Parse("2001:0db8:85a3:0000:0000:8a2e:0370:7334"), DateTime.UtcNow },
            new object[] { 2u, IPAddress.Parse("10.0.0.1"), IPAddress.Parse("fe80::1"), DateTime.UtcNow }
        };
        var columns = new[] { "id", "ipv4_addr", "ipv6_addr", "request_time" };
        await client.InsertBinaryAsync(tableName, columns, rows);

        Console.WriteLine("   Inserted rows with IP address data");

        // Query and read IP addresses
        using (var reader = await client.ExecuteReaderAsync($"SELECT id, ipv4_addr, ipv6_addr FROM {tableName} ORDER BY id"))
        {
            Console.WriteLine("\n   Reading IP address data:");
            while (reader.Read())
            {
                var id = reader.GetFieldValue<uint>(0);
                var ipv4 = reader.GetFieldValue<IPAddress>(1);
                var ipv6 = reader.GetFieldValue<IPAddress>(2);

                Console.WriteLine($"\n   ID: {id}");
                Console.WriteLine($"     IPv4: {ipv4}");
                Console.WriteLine($"     IPv6: {ipv6}");
            }
        }

        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    private static async Task Example5_Nested(ClickHouseClient client)
    {
        var tableName = "example_nested";

        // Nested is a special type representing an array of tuples
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                order_id UInt32,
                items Nested(
                    name String,
                    quantity UInt32,
                    price Float64
                )
            )
            ENGINE = MergeTree()
            ORDER BY order_id
        ");

        Console.WriteLine($"   Created table '{tableName}' with Nested structure");

        // Insert data - Nested columns are stored as separate arrays
        // Using InsertBinaryAsync with nested arrays
        var rows = new List<object[]>
        {
            new object[] { 1u, new[] { "Widget", "Gadget", "Tool" }, new uint[] { 2, 1, 3 }, new[] { 19.99, 49.99, 9.99 } },
            new object[] { 2u, new[] { "Book", "Pen" }, new uint[] { 5, 10 }, new[] { 12.50, 1.25 } }
        };
        var columns = new[] { "order_id", "items.name", "items.quantity", "items.price" };
        await client.InsertBinaryAsync(tableName, columns, rows);

        Console.WriteLine("   Inserted rows with nested data");

        // Query nested data
        using (var reader = await client.ExecuteReaderAsync($@"
            SELECT
                order_id,
                items.name,
                items.quantity,
                items.price
            FROM {tableName}
            ORDER BY order_id
        "))
        {
            Console.WriteLine("\n   Reading nested data:");
            while (reader.Read())
            {
                var orderId = reader.GetFieldValue<uint>(0);
                var names = reader.GetFieldValue<string[]>(1);
                var quantities = reader.GetFieldValue<uint[]>(2);
                var prices = reader.GetFieldValue<double[]>(3);

                Console.WriteLine($"\n   Order ID: {orderId}");
                Console.WriteLine("     Items:");
                for (int i = 0; i < names.Length; i++)
                {
                    Console.WriteLine($"       - {names[i]}: {quantities[i]} @ ${prices[i]:F2}");
                }
            }
        }

        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
    }
}
