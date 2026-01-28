using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to use Session IDs for maintaining state across multiple queries.
/// Sessions are primarily used for:
/// - Creating and using temporary tables
/// - Maintaining query context across multiple statements
///
/// IMPORTANT LIMITATION: When UseSession is enabled with a SessionId, the driver creates
/// a single-connection HttpClientFactory instead of using a pooled connection. This means
/// all queries in the session will use the same underlying HTTP connection, which is not
/// suitable for high-performance or high-concurrency scenarios.
///
/// Making queries using the same id from multiple connections simultaneously will cause errors.
/// 
/// Consider using regular tables with TTL instead of temporary tables
/// if you need to share data across multiple connections
/// </summary>
public static class SessionIdUsage
{
    public static async Task Run()
    {
        Console.WriteLine("Session ID Usage Examples\n");

        // Example 1: Using sessions for temporary tables
        // To use temporary tables, you must enable sessions
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            UseSession = true,
            // If you don't set SessionId, a GUID will be automatically generated
        };

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        Console.WriteLine($"   Session ID: {settings.SessionId}");

        // Create a temporary table
        // Temporary tables only exist within the session and are automatically dropped
        await connection.ExecuteStatementAsync(@"
            CREATE TEMPORARY TABLE temp_users
            (
                id UInt64,
                name String,
                email String
            )
        ");
        Console.WriteLine("   Created temporary table 'temp_users'");

        // Insert data into the temporary table
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "INSERT INTO temp_users (id, name, email) VALUES ({id:UInt64}, {name:String}, {email:String})";
            command.AddParameter("id", 1UL);
            command.AddParameter("name", "Alice");
            command.AddParameter("email", "alice@example.com");
            await command.ExecuteNonQueryAsync();
        }
        Console.WriteLine("   Inserted data into temporary table");

        // Query the temporary table
        using (var reader = await connection.ExecuteReaderAsync("SELECT id, name, email FROM temp_users ORDER BY id"))
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

        // Temporary tables are automatically dropped when the connection closes
        Console.WriteLine("\n   Temporary table will be dropped when connection closes");

        Console.WriteLine("\nAll Session ID examples completed!");
    }
}
