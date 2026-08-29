using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to use Session IDs for maintaining state across multiple queries.
/// Sessions are primarily used for:
/// - Creating and using temporary tables
/// - Maintaining query context across multiple statements
/// </summary>
public static class SessionIdUsage
{
    public static async Task Run()
    {
        Console.WriteLine("Session ID Usage Examples\n");

        // To use temporary tables, you must enable sessions
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            UseSession = true,
            // If you don't set SessionId, a GUID will be automatically generated
        };

        using var client = new ClickHouseClient(settings);

        Console.WriteLine($"   Session ID: {settings.SessionId}");

        // Create a temporary table
        // Temporary tables only exist within the session and are automatically dropped
        await client.ExecuteNonQueryAsync(@"
            CREATE TEMPORARY TABLE temp_users
            (
                id UInt64,
                name String,
                email String
            )
        ");
        Console.WriteLine("   Created temporary table 'temp_users'");

        // Insert data into the temporary table
        var rows = new List<object[]>
        {
            new object[] { 1UL, "Alice", "alice@example.com" },
        };
        await client.InsertBinaryAsync("temp_users", new[] { "id", "name", "email" }, rows);
        Console.WriteLine("   Inserted data into temporary table");

        // Query the temporary table
        using (var reader = await client.ExecuteReaderAsync("SELECT id, name, email FROM temp_users ORDER BY id"))
        {
            Console.WriteLine("\n   Data from temporary table:");
            Console.WriteLine("   ID\tName\tEmail");
            Console.WriteLine("   --\t----\t-----");
            while (reader.Read())
            {
                var id = reader.GetFieldValue<ulong>(0);
                var name = reader.GetString(1);
                var email = reader.GetString(2);
                Console.WriteLine($"   {id}\t{name}\t{email}");
            }
        }

        // Temporary tables are automatically dropped when the session ends
        Console.WriteLine("\n   Temporary table will be dropped when session ends");

        Console.WriteLine("\nAll Session ID examples completed!");
    }
}
