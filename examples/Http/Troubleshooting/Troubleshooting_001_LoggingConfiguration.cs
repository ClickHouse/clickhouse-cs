using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Example demonstrating how to configure logging.
/// </summary>
public static class LoggingConfiguration
{
    public static async Task Run()
    {
        Console.WriteLine("=== Example: Logging Configuration ===\n");

        // Create a console logger factory. Different providers can be configured here.
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddConsole()
                .SetMinimumLevel(LogLevel.Trace); // Set to Trace to see HttpClient configuration
        });

        Console.WriteLine("Creating client with Trace-level logging enabled...\n");

        // Create client settings with logger factory
        var settings = new ClickHouseClientSettings("Host=localhost;Port=8123;Username=default;Database=default")
        {
            LoggerFactory = loggerFactory,
        };

        using var client = new ClickHouseClient(settings);

        // Perform a simple query
        Console.WriteLine("\n\nPerforming a simple query...");
        var result = await client.ExecuteScalarAsync("SELECT 1");
        Console.WriteLine($"Query result: {result}");
    }
}
