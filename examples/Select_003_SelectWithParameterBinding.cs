using ClickHouse.Driver.ADO;
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
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        var tableName = "example_parameter_binding";

        // Create and populate a test table
        await connection.ExecuteStatementAsync($@"
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

        // Insert sample data
        await connection.ExecuteStatementAsync($@"
            INSERT INTO {tableName} VALUES
            (1, 'alice', 'alice@example.com', 28, 'USA', '2020-01-15', 95.5),
            (2, 'bob', 'bob@example.com', 35, 'UK', '2019-06-20', 87.3),
            (3, 'carol', 'carol@example.com', 42, 'USA', '2018-03-10', 92.1),
            (4, 'david', 'david@example.com', 29, 'Canada', '2021-09-05', 88.9),
            (5, 'eve', 'eve@example.com', 31, 'USA', '2020-11-12', 91.7),
            (6, 'frank', 'frank@example.com', 45, 'UK', '2019-02-28', 79.8),
            (7, 'grace', 'grace@example.com', 26, 'Canada', '2022-01-08', 94.2)
        ");

        Console.WriteLine($"Created and populated table '{tableName}'\n");

        // Example 1: Parameters with explicit ClickHouse type specification. This is the recommended approach.
        Console.WriteLine("\n1. Parameters with explicit types:");
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                SELECT username, registration_date
                FROM {tableName}
                WHERE registration_date >= {{startDate:Date}}
                ORDER BY registration_date";

            command.AddParameter("startDate", "Date", new DateTime(2020, 1, 1)); // "Date" here specifies the ClickHouse type

            using var reader = await command.ExecuteReaderAsync();
            Console.WriteLine("   Users registered since 2020:");
            while (reader.Read())
            {
                Console.WriteLine($"   - {reader.GetString(0)}: {reader.GetDateTime(1):yyyy-MM-dd}");
            }
        }

        // Example 2: Simple parameter binding with type inference
        // WARNING: Type inference can result in unexpected issues, especially with numeric types
        // and complex data types. It is strongly recommended to always specify the type explicitly.
        Console.WriteLine("\n2. Simple parameter binding with type inference:");
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                SELECT username, age, score
                FROM {tableName}
                WHERE age >= {{minAge:UInt8}} AND score >= {{minScore:Float32}}
                ORDER BY score DESC";

            // Type will be inferred - may lead to unexpected behavior
            command.AddParameter("minAge", (byte)30);
            command.AddParameter("minScore", 90.0f);

            using var reader = await command.ExecuteReaderAsync();
            Console.WriteLine("   Users aged 30+ with score >= 90:");
            while (reader.Read())
            {
                Console.WriteLine($"   - {reader.GetString(0)}, Age: {reader.GetByte(1)}, Score: {reader.GetFloat(2):F1}");
            }
        }

        // Example 3: Reusing command with different parameter values
        Console.WriteLine("\n3. Reusing command with different parameters:");
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                SELECT username, country, score
                FROM {tableName}
                WHERE country = {{country:String}}
                ORDER BY score DESC
                LIMIT 1";

            var countries = new[] { "USA", "UK", "Canada" };

            foreach (var country in countries)
            {
                command.Parameters.Clear();
                command.AddParameter("country", "String", country);

                var topUser = await command.ExecuteScalarAsync();
                Console.WriteLine($"   Top user in {country}: {topUser}");
            }
        }

        // Example 4: Parameter binding with IN clause using arrays
        Console.WriteLine("\n4. Parameter binding with IN clause:");
        using (var command = connection.CreateCommand())
        {
            // Note: For IN clauses with arrays, we need to format them properly
            command.CommandText = $@"
                SELECT username, country, age
                FROM {tableName}
                WHERE country IN ({{countries:Array(String)}})
                ORDER BY age DESC";

            command.AddParameter("countries", "Array(String)", new[] { "USA", "UK" });

            using var reader = await command.ExecuteReaderAsync();

            Console.WriteLine("   Users from USA or UK:");
            while (reader.Read())
            {
                Console.WriteLine($"   - {reader.GetString(0)} ({reader.GetString(1)}), Age: {reader.GetByte(2)}");
            }
        }

        // Example 5: Parameter binding for tuple comparison
        Console.WriteLine("\n5. Parameter binding with tuple:");
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                SELECT username, age, score
                FROM {tableName}
                WHERE (age, score) > {{comparison:Tuple(UInt8, Float32)}}
                ORDER BY age, score
                LIMIT 3";

            command.AddParameter("comparison", "Tuple(UInt8, Float32)", Tuple.Create((byte)30, 85.0f));

            using var reader = await command.ExecuteReaderAsync();
            Console.WriteLine("   Users with (age, score) > (30, 85.0):");
            while (reader.Read())
            {
                Console.WriteLine($"   - {reader.GetString(0)}: Age {reader.GetByte(1)}, Score {reader.GetFloat(2):F1}");
            }
        }

        // Clean up
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\nTable '{tableName}' dropped");
    }
}
