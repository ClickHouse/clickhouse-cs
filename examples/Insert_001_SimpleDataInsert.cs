using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates simple data insertion methods in ClickHouse.
/// Shows parameterized queries and basic INSERT statements.
/// For performant inserts using the binary format, see the BulkCopy examples instead.
/// </summary>
public static class SimpleDataInsert
{
    private const string TableName = "example_simple_insert";
    private const string ExceptTableName = "example_insert_except";

    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        await SetupTable(connection);

        // Insert a single row using {name:Type} parameter syntax
        await InsertUsingParameterizedQuery(connection);

        // Insert multiple rows by reusing the same command
        await InsertMultipleRowsWithCommandReuse(connection);

        // Simple insert without parameters using extension method
        await InsertUsingExecuteStatementAsync(connection);

        await VerifyInsertedData(connection);

        // Use EXCEPT clause to skip columns with DEFAULT values
        await InsertUsingExceptClause(connection);

        await Cleanup(connection);
    }

    private static async Task SetupTable(ClickHouseConnection connection)
    {
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {TableName}
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

        Console.WriteLine($"Table '{TableName}' created\n");
    }

    /// <summary>
    /// Demonstrates inserting a single row using parameterized query.
    /// Parameters are specified using {name:Type} syntax in the query.
    /// </summary>
    private static async Task InsertUsingParameterizedQuery(ClickHouseConnection connection)
    {
        Console.WriteLine("1. Inserting data using parameterized query:");
        using var command = connection.CreateCommand();

        command.CommandText = $@"
            INSERT INTO {TableName} (id, name, email, age, score, registered_at)
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

    /// <summary>
    /// Demonstrates inserting multiple rows by reusing a command with different parameter values.
    /// The Parameters collection is cleared between each insert.
    /// </summary>
    private static async Task InsertMultipleRowsWithCommandReuse(ClickHouseConnection connection)
    {
        Console.WriteLine("2. Inserting multiple rows using parameters:");
        using var command = connection.CreateCommand();

        command.CommandText = $@"
            INSERT INTO {TableName} (id, name, email, age, score, registered_at)
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

    /// <summary>
    /// Demonstrates inserting data using ExecuteStatementAsync extension method.
    /// Suitable for simple cases where parameterization is not needed.
    /// </summary>
    private static async Task InsertUsingExecuteStatementAsync(ClickHouseConnection connection)
    {
        Console.WriteLine("3. Inserting data using ExecuteStatementAsync:");
        await connection.ExecuteStatementAsync($@"
            INSERT INTO {TableName} (id, name, email, age, score, registered_at)
            VALUES (5, 'Eve Davis', 'eve@example.com', 31, 91.7, now())
        ");
        Console.WriteLine("   Inserted 1 row using ExecuteStatementAsync\n");
    }

    /// <summary>
    /// Demonstrates inserting data using the EXCEPT clause to exclude columns with DEFAULT values.
    /// This is useful when you want the server to populate certain columns automatically.
    /// </summary>
    private static async Task InsertUsingExceptClause(ClickHouseConnection connection)
    {
        Console.WriteLine("4. Inserting data using EXCEPT clause:");

        // Create a table with DEFAULT columns
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {ExceptTableName} (
                id Int32,
                name String,
                value Float64,
                created DateTime DEFAULT now(),
                updated DateTime DEFAULT now()
            ) ENGINE = Memory
        ");

        using var command = connection.CreateCommand();

        // Use EXCEPT to exclude columns that have DEFAULT values
        // The server will automatically populate 'created' and 'updated' columns
        command.CommandText = $"INSERT INTO {ExceptTableName} (* EXCEPT (created, updated)) VALUES ({{id:Int32}}, {{name:String}}, {{value:Float64}})";

        command.AddParameter("id", 1);
        command.AddParameter("name", "Test Item");
        command.AddParameter("value", 123.45);

        await command.ExecuteNonQueryAsync();
        Console.WriteLine("   Inserted 1 row using EXCEPT clause (created/updated auto-populated)\n");

        // Verify the data was inserted with DEFAULT values
        using var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {ExceptTableName}");
        Console.WriteLine("   Result:");
        while (reader.Read())
        {
            var id = reader.GetInt32(0);
            var name = reader.GetString(1);
            var value = reader.GetDouble(2);
            var created = reader.GetDateTime(3);
            var updated = reader.GetDateTime(4);
            Console.WriteLine($"   id={id}, name={name}, value={value}, created={created:HH:mm:ss}, updated={updated:HH:mm:ss}\n");
        }

        // Clean up the EXCEPT example table
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {ExceptTableName}");
    }

    private static async Task VerifyInsertedData(ClickHouseConnection connection)
    {
        Console.WriteLine("Verifying inserted data:");
        using var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {TableName} ORDER BY id");

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

        var count = await connection.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
        Console.WriteLine($"\nTotal rows inserted: {count}");
    }

    private static async Task Cleanup(ClickHouseConnection connection)
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {TableName}");
        Console.WriteLine($"\nTable '{TableName}' dropped");
    }
}
