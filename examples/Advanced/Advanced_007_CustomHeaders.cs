using System.Collections.Generic;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to use custom HTTP headers with ClickHouse connections.
/// Custom headers can be used for proxy authentication, distributed tracing via correlation ids, etc.
///
/// The Authorization, User-Agent, and Connection headers cannot be overridden.
/// </summary>
public static class CustomHeaders
{
    public static async Task Run()
    {
        Console.WriteLine("Custom HTTP Headers Example\n");

        // Add custom headers for proxy authentication
        // Useful when connecting through a proxy that requires specific headers
        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            CustomHeaders = new Dictionary<string, string>
            {
                ["X-Proxy-Authorization"] = "Bearer proxy-token-xyz",
                ["X-Forwarded-For"] = "192.168.1.100",
                ["X-Real-IP"] = "192.168.1.100"
            }
        };

        Console.WriteLine("   Settings created with proxy headers:");
        Console.WriteLine("   - X-Proxy-Authorization: Bearer proxy-token-xyz");
        Console.WriteLine("   - X-Forwarded-For: 192.168.1.100");
        Console.WriteLine("   - X-Real-IP: 192.168.1.100");

        try
        {
            using var connection = new ClickHouseConnection(settings);
            await connection.OpenAsync();

            var result = await connection.ExecuteScalarAsync("SELECT 42");
            Console.WriteLine($"   Query executed successfully: {result}");
        }
        catch
        {
            Console.WriteLine("   Query could not be executed");
        }

        Console.WriteLine("\nAll custom headers examples completed!");
    }
}
