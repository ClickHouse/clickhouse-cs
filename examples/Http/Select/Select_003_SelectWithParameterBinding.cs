using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates parameter binding in SELECT queries to prevent SQL injection,
/// properly handle type conversion, and enable dynamic query construction safely.
/// </summary>
public static class SelectWithParameterBinding
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        var tableName = "example_parameter_binding";

        // Create and populate a test table
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                username String,
                email String,
                age UInt8,
                country String,
                registration_date Date,
                score Float32
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");

        // Insert sample data using InsertBinaryAsync
        var rows = new List<object[]>
        {
            new object[] { 1UL, "alice", "alice@example.com", (byte)28, "USA", new DateOnly(2020, 1, 15), 95.5f },
            new object[] { 2UL, "bob", "bob@example.com", (byte)35, "UK", new DateOnly(2019, 6, 20), 87.3f },
            new object[] { 3UL, "carol", "carol@example.com", (byte)42, "USA", new DateOnly(2018, 3, 10), 92.1f },
            new object[] { 4UL, "david", "david@example.com", (byte)29, "Canada", new DateOnly(2021, 9, 5), 88.9f },
            new object[] { 5UL, "eve", "eve@example.com", (byte)31, "USA", new DateOnly(2020, 11, 12), 91.7f },
            new object[] { 6UL, "frank", "frank@example.com", (byte)45, "UK", new DateOnly(2019, 2, 28), 79.8f },
            new object[] { 7UL, "grace", "grace@example.com", (byte)26, "Canada", new DateOnly(2022, 1, 8), 94.2f }
        };
        var columns = new[] { "id", "username", "email", "age", "country", "registration_date", "score" };
        await client.InsertBinaryAsync(tableName, columns, rows);

        Console.WriteLine($"Created and populated table '{tableName}'\n");

        // Example 1: Using parameters. The ClickHouse type is parsed from the query, and used to serialize the parameter value appropriately
        Console.WriteLine("\n1. Parameters with explicit types:");
        {
            // The ClickHouse parameter format is {parameter_name:clickhouse_type}. Below, "Date" specifies the type of the parameter
            var parameters = new ClickHouseParameterCollection();
            parameters.AddParameter("startDate", new DateOnly(2020, 1, 1));

            using var reader = await client.ExecuteReaderAsync($@"
                SELECT username, registration_date
                FROM {tableName}
                WHERE registration_date >= {{startDate:Date}}
                ORDER BY registration_date", parameters);

            Console.WriteLine("   Users registered since 2020:");
            while (reader.Read())
            {
                Console.WriteLine($"   - {reader.GetString(0)}: {reader.GetDateTime(1):yyyy-MM-dd}");
            }
        }

        // Example 2: Querying with different parameter values
        Console.WriteLine("\n2. Querying with different parameters:");
        {
            var countriesQuery = new[] { "USA", "UK", "Canada" };

            foreach (var country in countriesQuery)
            {
                var parameters = new ClickHouseParameterCollection();
                parameters.AddParameter("country", country);

                var topUser = await client.ExecuteScalarAsync($@"
                    SELECT username
                    FROM {tableName}
                    WHERE country = {{country:String}}
                    ORDER BY score DESC
                    LIMIT 1", parameters);

                Console.WriteLine($"   Top user in {country}: {topUser}");
            }
        }

        // Example 3: Parameter binding with IN clause using arrays
        Console.WriteLine("\n3. Parameter binding with IN clause:");
        {
            var parameters = new ClickHouseParameterCollection();
            parameters.AddParameter("countries", new[] { "USA", "UK" });

            using var reader = await client.ExecuteReaderAsync($@"
                SELECT username, country, age
                FROM {tableName}
                WHERE country IN ({{countries:Array(String)}})
                ORDER BY age DESC", parameters);

            Console.WriteLine("   Users from USA or UK:");
            while (reader.Read())
            {
                Console.WriteLine($"   - {reader.GetString(0)} ({reader.GetString(1)}), Age: {reader.GetByte(2)}");
            }
        }

        // Example 4: Parameter binding for tuple comparison
        Console.WriteLine("\n4. Parameter binding with tuple:");
        {
            var parameters = new ClickHouseParameterCollection();
            parameters.AddParameter("comparison", Tuple.Create((byte)30, 85.0f));

            using var reader = await client.ExecuteReaderAsync($@"
                SELECT username, age, score
                FROM {tableName}
                WHERE (age, score) > {{comparison:Tuple(UInt8, Float32)}}
                ORDER BY age, score
                LIMIT 3", parameters);

            Console.WriteLine("   Users with (age, score) > (30, 85.0):");
            while (reader.Read())
            {
                Console.WriteLine($"   - {reader.GetString(0)}: Age {reader.GetByte(1)}, Score {reader.GetFloat(2):F1}");
            }
        }

        // Clean up
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\nTable '{tableName}' dropped");
    }
}
