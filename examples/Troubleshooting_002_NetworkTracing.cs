using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Diagnostic;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Example demonstrating the EnableDebugMode setting for low-level .NET network tracing.
/// This feature is useful for diagnosing network issues, TLS handshake problems, DNS resolution,
/// socket errors, and HTTP protocol details.
///
/// WARNING: This feature has significant performance impact and generates large amounts of log data.
/// Only use for debugging - NOT recommended for production environments.
/// Requires .NET 5 or later.
/// </summary>
public static class NetworkTracing
{
    public static async Task Run()
    {
#if !NET5_0_OR_GREATER
        Console.WriteLine("WARNING: This feature requires .NET 5.0 or later.");
        Console.WriteLine("Current runtime does not support EnableDebugMode.");
        Console.WriteLine("Skipping this example.\n");
        return;
#else
        Console.WriteLine("Debug Mode Network Tracing Example");
        Console.WriteLine("===================================");
        Console.WriteLine("This example demonstrates low-level .NET network tracing.");
        Console.WriteLine("You will see detailed HTTP, Socket, DNS, and TLS events.\n");



        // Step 1: Configure a logger factory with Trace level enabled
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddConsole()
                .SetMinimumLevel(LogLevel.Trace); // Must be Trace level to see network events
        });

        // Step 2: Configure ClickHouse connection with EnableDebugMode
        var settings = new ClickHouseClientSettings("Host=localhost;Port=8123;Username=default;Database=default")
        {
            LoggerFactory = loggerFactory,
            EnableDebugMode = true,  // Enable low-level network tracing
        };

        Console.WriteLine("Connecting to ClickHouse with debug mode enabled...\n");

        try
        {
            using (var connection = new ClickHouseConnection(settings))
            {
                // Open connection - you'll see DNS resolution, socket connection, TLS handshake (if HTTPS)
                await connection.OpenAsync();
                Console.WriteLine("\n[Application] Connection opened successfully\n");

                // Execute a simple query - you'll see HTTP request/response details
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT version(), 'Hello from ClickHouse!' as message";
                using var reader = await command.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    Console.WriteLine($"\n[Application] ClickHouse version: {reader.GetString(0)}");
                    Console.WriteLine($"[Application] Message: {reader.GetString(1)}\n");
                }
            }

        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n[Application] Error: {ex.Message}");
        }
        finally
        {
            TraceHelper.Deactivate();
        }

        Console.WriteLine("\n\nDebug mode example completed!");
        Console.WriteLine("\nRemember: Disable this in production due to performance impact!");
    }
#endif
}
