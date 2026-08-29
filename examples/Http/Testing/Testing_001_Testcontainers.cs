using ClickHouse.Driver.Utility;
using Testcontainers.ClickHouse;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates using Testcontainers for integration testing with ClickHouse.
///
/// Testcontainers provides disposable, lightweight containers for testing.
/// This is ideal for:
/// - Integration tests that need a real ClickHouse instance
/// - CI/CD pipelines where you can't rely on external infrastructure
/// - Local development without installing ClickHouse
///
/// Key patterns shown:
/// - Container lifecycle management (start/stop)
/// - Getting connection string from container
/// - Running tests against ephemeral ClickHouse instance
/// - Best practices for test setup/teardown
///
/// In a real test project, you would:
/// - Use TestFixture or ICollectionFixture (depending on test framework) to start container once per test run
/// - Share the container across multiple test classes
/// - Use the connection string from GetConnectionString()
///
///  * CI/CD CONSIDERATIONS
///    ==================
///
///    * Will not work on macos images.
///
///    * GitHub Actions:
///      - Testcontainers works out of the box with GitHub Actions
///      - No special configuration needed
///
///    * Azure DevOps:
///      - Use Linux agents or Windows with Docker Desktop
///      - May need to set TESTCONTAINERS_RYUK_DISABLED=true in some cases
/// </summary>
public static class Testcontainers
{
    public static async Task Run()
    {
        Console.WriteLine("Testcontainers for ClickHouse Integration Testing\n");
        Console.WriteLine("Note: this example will probably have trouble running on macos.\n");

        // 1. Create and start the container
        Console.WriteLine("1. Starting ClickHouse container:");
        Console.WriteLine("   This may take a moment on first run (downloading image)...\n");

        // Use the alpine image for faster startup
        await using var container = new ClickHouseBuilder()
            .WithImage("clickhouse/clickhouse-server:25.12-alpine")
            .Build();

        await container.StartAsync();

        Console.WriteLine($"   Container started!");
        Console.WriteLine($"   Connection string: {container.GetConnectionString()}\n");

        // 2. Connect and run a simple test scenario
        Console.WriteLine("2. Running simulated test scenario:");

        await RunSimulatedTests(container.GetConnectionString());

        Console.WriteLine("\n3. Container cleanup:");
        Console.WriteLine("   Container will be automatically stopped and removed");
        Console.WriteLine("   when disposed (via 'await using' or explicit DisposeAsync)");

        // Container is automatically stopped and removed when disposed
    }

    /// <summary>
    /// Simulates a typical integration test scenario.
    /// In a real test project, these would be separate test methods.
    /// </summary>
    private static async Task RunSimulatedTests(string connectionString)
    {
        using var client = new ClickHouseClient(connectionString);

        // Test 1: Create table
        Console.WriteLine("   [TEST] CreateTable_ShouldSucceed");
        await client.ExecuteNonQueryAsync(@"
            CREATE TABLE test_users (
                id UInt64,
                name String,
                email String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY id
        ");
        Console.WriteLine("          PASSED - Table created\n");

        // Test 2: Insert data using InsertBinaryAsync
        Console.WriteLine("   [TEST] InsertData_ShouldSucceed");
        var rows = new List<object[]>
        {
            new object[] { 1UL, "Alice", "alice@example.com" }
        };
        var columns = new[] { "id", "name", "email" };
        await client.InsertBinaryAsync("test_users", columns, rows);
        Console.WriteLine("          PASSED - Data inserted\n");

        // Test 3: Query data
        Console.WriteLine("   [TEST] QueryData_ShouldReturnInsertedRow");
        var name = await client.ExecuteScalarAsync("SELECT name FROM test_users WHERE id = 1");
        Console.WriteLine($"Name: {name}");
        if ((string)name! != "Alice")
            throw new Exception($"Expected 'Alice' but got '{name}'");
        Console.WriteLine("          PASSED - Query returned correct data\n");

        // Test 4: Verify count
        Console.WriteLine("   [TEST] Count_ShouldBeOne");
        var count = await client.ExecuteScalarAsync("SELECT count() FROM test_users");
        if ((ulong)count! != 1)
            throw new Exception($"Expected 1 but got {count}");
        Console.WriteLine("          PASSED - Count is correct\n");

        // Test 5: Drop table (cleanup within test)
        Console.WriteLine("   [TEST] DropTable_ShouldSucceed");
        await client.ExecuteNonQueryAsync("DROP TABLE test_users");
        Console.WriteLine("          PASSED - Table dropped");

        Console.WriteLine("\n   All tests passed!");
    }
}
