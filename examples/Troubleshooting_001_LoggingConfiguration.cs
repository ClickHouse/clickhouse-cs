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

        Console.WriteLine("Creating connection with Trace-level logging enabled...\n");

        // Create connection settings with logger factory
        var settings = new ClickHouseClientSettings("Host=localhost;Port=8123;Username=default;Database=default")
        {
            LoggerFactory = loggerFactory
        };

        using var connection = new ClickHouseConnection(settings);

        Console.WriteLine("Opening connection (watch for HttpClient configuration logs)...\n");
        await connection.OpenAsync();

        // Perform a simple query
        Console.WriteLine("\n\nPerforming a simple query...");
        var result = await connection.ExecuteScalarAsync("SELECT 1");
        Console.WriteLine($"Query result: {result}");
    }
}
