using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates simple data insertion methods in ClickHouse.
/// Shows parameterized queries and basic INSERT statements.
/// </summary>
public static class SimpleDataInsert
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        var tableName = "example_simple_insert";

        // Create a test table
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                name String,
                email String,
                age UInt8,
                score Float32,
                registered_at DateTime
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");

        Console.WriteLine($"Table '{tableName}' created\n");

        // Method 1: Insert using parameterized query
        Console.WriteLine("1. Inserting data using parameterized query:");
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                INSERT INTO {tableName} (id, name, email, age, score, registered_at)
                VALUES ({{id:UInt64}}, {{name:String}}, {{email:String}}, {{age:UInt8}}, {{score:Float32}}, {{timestamp:DateTime}})";

            command.AddParameter("id", 1);
            command.AddParameter("name", "Alice Smith");
            command.AddParameter("email", "alice@example.com");
            command.AddParameter("age", 28);
            command.AddParameter("score", 95.5f);
            command.AddParameter("timestamp", DateTime.UtcNow);

            await command.ExecuteNonQueryAsync();
            Console.WriteLine("   Inserted 1 row with parameters\n");
        }

        // Method 2: Insert multiple rows with parameter reuse
        Console.WriteLine("2. Inserting multiple rows using parameters:");
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                INSERT INTO {tableName} (id, name, email, age, score, registered_at)
                VALUES ({{id:UInt64}}, {{name:String}}, {{email:String}}, {{age:UInt8}}, {{score:Float32}}, {{timestamp:DateTime}})";

            var users = new[]
            {
                new { Id = 2UL, Name = "Bob Johnson", Email = "bob@example.com", Age = (byte)35, Score = 87.3f },
                new { Id = 3UL, Name = "Carol White", Email = "carol@example.com", Age = (byte)42, Score = 92.1f },
                new { Id = 4UL, Name = "David Brown", Email = "david@example.com", Age = (byte)29, Score = 88.9f },
            };

            foreach (var user in users)
            {
                command.Parameters.Clear();
                command.AddParameter("id", user.Id);
                command.AddParameter("name", user.Name);
                command.AddParameter("email", user.Email);
                command.AddParameter("age", user.Age);
                command.AddParameter("score", user.Score);
                command.AddParameter("timestamp", DateTime.UtcNow);
                await command.ExecuteNonQueryAsync();
            }

            Console.WriteLine($"   Inserted {users.Length} rows\n");
        }

        // Method 3: Insert with ExecuteStatementAsync (for simple cases)
        Console.WriteLine("3. Inserting data using ExecuteStatementAsync:");
        await connection.ExecuteStatementAsync($@"
            INSERT INTO {tableName} (id, name, email, age, score, registered_at)
            VALUES (5, 'Eve Davis', 'eve@example.com', 31, 91.7, now())
        ");
        Console.WriteLine("   Inserted 1 row using ExecuteStatementAsync\n");

        // Query and display the inserted data
        Console.WriteLine("Verifying inserted data:");
        using (var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {tableName} ORDER BY id"))
        {
            Console.WriteLine("ID\tName\t\t\tEmail\t\t\t\tAge\tScore\tRegistered At");
            Console.WriteLine("--\t----\t\t\t-----\t\t\t\t---\t-----\t-------------");

            while (reader.Read())
            {
                var id = reader.GetFieldValue<ulong>(0);
                var name = reader.GetString(1);
                var email = reader.GetString(2);
                var age = reader.GetByte(3);
                var score = reader.GetFloat(4);
                var registeredAt = reader.GetDateTime(5);

                Console.WriteLine($"{id}\t{name,-20}\t{email,-30}\t{age}\t{score:F1}\t{registeredAt:yyyy-MM-dd HH:mm:ss}");
            }
        }

        // Get row count
        var count = await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Console.WriteLine($"\nTotal rows inserted: {count}");

        // Clean up
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\nTable '{tableName}' dropped");
    }
}
