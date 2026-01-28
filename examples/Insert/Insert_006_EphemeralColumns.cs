using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates inserting data using EPHEMERAL columns.
///
/// A ClickHouse EPHEMERAL column is a special type of column that accepts data during an INSERT
/// operation but is not stored in the table and cannot be queried directly. Its sole purpose is
/// to provide intermediate input for the default value expressions of other, persistent columns.
///
/// This is useful for:
/// - Transforming input data before storage
/// - Providing default values based on input that shouldn't be persisted
/// - Deriving multiple columns from a single input value
///
/// See https://clickhouse.com/docs/sql-reference/statements/create/table#ephemeral
/// </summary>
public static class EphemeralColumns
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("Ephemeral Columns Examples\n");

        Console.WriteLine("1. Multiple derived columns from ephemeral input:");

        var tableName = "example_ephemeral_derived";

        // Create table where multiple columns are derived from ephemeral inputs
        await connection.ExecuteStatementAsync($@"
            CREATE OR REPLACE TABLE {tableName}
            (
                id              UInt64,
                full_name       String DEFAULT concat(first_name, ' ', last_name),
                name_length     UInt32 DEFAULT length(concat(first_name, last_name)),
                first_name      String EPHEMERAL,
                last_name       String EPHEMERAL
            )
            ENGINE MergeTree()
            ORDER BY (id)
        ");

        Console.WriteLine("   Created table with schema:");
        Console.WriteLine("   - id: UInt64");
        Console.WriteLine("   - full_name: String DEFAULT concat(first_name, ' ', last_name)");
        Console.WriteLine("   - name_length: UInt32 DEFAULT length(concat(first_name, last_name))");
        Console.WriteLine("   - first_name: String EPHEMERAL");
        Console.WriteLine("   - last_name: String EPHEMERAL\n");

        // Insert with ephemeral columns using parameterized query
        Console.WriteLine("   Inserting data with first_name and last_name:");

        var people = new[]
        {
            (Id: 1UL, FirstName: "Alice", LastName: "Smith"),
            (Id: 2UL, FirstName: "Bob", LastName: "Johnson"),
            (Id: 3UL, FirstName: "Carol", LastName: "Williams")
        };

        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                INSERT INTO {tableName} (id, first_name, last_name)
                VALUES ({{id:UInt64}}, {{first_name:String}}, {{last_name:String}})";

            foreach (var person in people)
            {
                command.Parameters.Clear();
                command.AddParameter("id", person.Id);
                command.AddParameter("first_name", person.FirstName);
                command.AddParameter("last_name", person.LastName);
                await command.ExecuteNonQueryAsync();
                Console.WriteLine($"   - Inserted id={person.Id}, first_name='{person.FirstName}', last_name='{person.LastName}'");
            }
        }

        Console.WriteLine();

        // Query results - derived columns are stored, ephemeral columns are not
        Console.WriteLine("   Query results (derived columns are stored, ephemeral are not):");
        using (var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {tableName} ORDER BY id"))
        {
            Console.WriteLine("   ID\tFull Name\t\tName Length");
            Console.WriteLine("   --\t---------\t\t-----------");
            while (reader.Read())
            {
                Console.WriteLine($"   {reader.GetFieldValue<ulong>(0)}\t{reader.GetString(1),-20}\t{reader.GetFieldValue<uint>(2)}");
            }
        }

        // Cleanup
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\n   Table dropped\n");

        Console.WriteLine("All ephemeral column examples completed!");
    }

}
