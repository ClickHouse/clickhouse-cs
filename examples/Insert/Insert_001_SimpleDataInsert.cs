using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates three data insertion approaches in ClickHouse:
/// 1. InsertBinaryAsync - High-performance binary format (recommended for bulk data)
/// 2. ExecuteStatementAsync with parameters - SQL with parameterized values
/// 3. ADO.NET Command - Classic ADO.NET pattern with ClickHouseCommand
/// </summary>
public static class SimpleDataInsert
{
    private const string TableName = "example_simple_insert";

    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        await SetupTable(client);

        // Option 1: InsertBinaryAsync (recommended for bulk inserts)
        await InsertUsingBinaryAsync(client);

        // Option 2: ExecuteStatementAsync with parameters
        await InsertUsingParameterizedStatement(client);

        // Option 3: ADO.NET Command pattern
        await InsertUsingAdoCommand();

        // Option 4: EXCEPT clause for DEFAULT columns
        await InsertUsingExceptClause(client);

        await VerifyInsertedData(client);
        await Cleanup(client);
    }

    private static async Task SetupTable(ClickHouseClient client)
    {
        await client.ExecuteNonQueryAsync($@"
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
    /// Option 1: InsertBinaryAsync - High-performance binary format.
    /// Recommended for inserting multiple rows efficiently.
    /// </summary>
    private static async Task InsertUsingBinaryAsync(ClickHouseClient client)
    {
        Console.WriteLine("1. InsertBinaryAsync (recommended for bulk inserts):");

        var rows = new List<object[]>
        {
            new object[] { 1UL, "Alice Smith", "alice@example.com", (byte)28, 95.5f, DateTime.UtcNow },
            new object[] { 2UL, "Bob Johnson", "bob@example.com", (byte)35, 87.3f, DateTime.UtcNow },
            new object[] { 3UL, "Carol White", "carol@example.com", (byte)42, 92.1f, DateTime.UtcNow },
        };

        var columns = new[] { "id", "name", "email", "age", "score", "registered_at" };
        await client.InsertBinaryAsync(TableName, columns, rows);

        Console.WriteLine($"   Inserted {rows.Count} rows using binary format\n");
    }

    /// <summary>
    /// Option 2: ExecuteNonQueryAsync with parameters.
    /// Uses {name:Type} syntax for parameterized values.
    /// </summary>
    private static async Task InsertUsingParameterizedStatement(ClickHouseClient client)
    {
        Console.WriteLine("2. ExecuteNonQueryAsync with parameters:");

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("id", 4UL);
        parameters.AddParameter("name", "David Brown");
        parameters.AddParameter("email", "david@example.com");
        parameters.AddParameter("age", (byte)29);
        parameters.AddParameter("score", 88.9f);
        parameters.AddParameter("timestamp", DateTime.UtcNow);

        await client.ExecuteNonQueryAsync($@"
            INSERT INTO {TableName} (id, name, email, age, score, registered_at)
            VALUES ({{id:UInt64}}, {{name:String}}, {{email:String}}, {{age:UInt8}}, {{score:Float32}}, {{timestamp:DateTime}})",
            parameters);

        Console.WriteLine("   Inserted 1 row using parameterized statement\n");
    }

    /// <summary>
    /// Option 3: ADO.NET Command pattern.
    /// Classic approach using ClickHouseConnection and ClickHouseCommand.
    /// Useful when integrating with ORMs or ADO.NET-based tools.
    /// </summary>
    private static async Task InsertUsingAdoCommand()
    {
        Console.WriteLine("3. ADO.NET Command pattern:");

        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        using var command = connection.CreateCommand();
        command.CommandText = $@"
            INSERT INTO {TableName} (id, name, email, age, score, registered_at)
            VALUES ({{id:UInt64}}, {{name:String}}, {{email:String}}, {{age:UInt8}}, {{score:Float32}}, {{timestamp:DateTime}})";

        command.AddParameter("id", 5UL);
        command.AddParameter("name", "Eve Davis");
        command.AddParameter("email", "eve@example.com");
        command.AddParameter("age", (byte)31);
        command.AddParameter("score", 91.7f);
        command.AddParameter("timestamp", DateTime.UtcNow);

        await command.ExecuteNonQueryAsync();

        Console.WriteLine("   Inserted 1 row using ADO.NET Command\n");
    }

    /// <summary>
    /// Option 4: Using EXCEPT clause to skip columns with DEFAULT values.
    /// The server automatically populates columns excluded via EXCEPT.
    /// </summary>
    private static async Task InsertUsingExceptClause(ClickHouseClient client)
    {
        Console.WriteLine("4. Insert using EXCEPT clause:");

        var exceptTableName = "example_insert_except";

        // Create a table with DEFAULT columns
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {exceptTableName} (
                id Int32,
                name String,
                value Float64,
                created DateTime DEFAULT now(),
                updated DateTime DEFAULT now()
            ) ENGINE = Memory
        ");

        // Use EXCEPT to exclude columns that have DEFAULT values
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("id", 1);
        parameters.AddParameter("name", "Test Item");
        parameters.AddParameter("value", 123.45);

        await client.ExecuteNonQueryAsync(
            $"INSERT INTO {exceptTableName} (* EXCEPT (created, updated)) VALUES ({{id:Int32}}, {{name:String}}, {{value:Float64}})",
            parameters);

        Console.WriteLine("   Inserted 1 row using EXCEPT clause (created/updated auto-populated)");

        // Verify the data was inserted with DEFAULT values
        using var reader = await client.ExecuteReaderAsync($"SELECT * FROM {exceptTableName}");
        while (reader.Read())
        {
            var id = reader.GetInt32(0);
            var name = reader.GetString(1);
            var value = reader.GetDouble(2);
            var created = reader.GetDateTime(3);
            var updated = reader.GetDateTime(4);
            Console.WriteLine($"   Result: id={id}, name={name}, value={value}, created={created:HH:mm:ss}, updated={updated:HH:mm:ss}\n");
        }

        // Clean up
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {exceptTableName}");
    }

    private static async Task VerifyInsertedData(ClickHouseClient client)
    {
        Console.WriteLine("Verifying inserted data:");
        using var reader = await client.ExecuteReaderAsync($"SELECT * FROM {TableName} ORDER BY id");

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

        var count = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
        Console.WriteLine($"\nTotal rows inserted: {count}");
    }

    private static async Task Cleanup(ClickHouseClient client)
    {
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {TableName}");
        Console.WriteLine($"\nTable '{TableName}' dropped");
    }
}
