using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Examples of using JWT authentication with ClickHouse.
/// </summary>
public static class JwtAuthentication
{
    public static async Task Run()
    {
        // Get JWT from environment variable (in production, you'd get this from your IdP)
        var jwt = Environment.GetEnvironmentVariable("CLICKHOUSE_JWT_TOKEN");
        var host = Environment.GetEnvironmentVariable("CLICKHOUSE_HOST") ?? "localhost";

        if (string.IsNullOrEmpty(jwt))
        {
            Console.WriteLine("Skipping JWT example: Set CLICKHOUSE_JWT_TOKEN environment variable to run this example");
            return;
        }

        // 1. Basic JWT authentication at connection level
        Console.WriteLine("1. JWT authentication at connection level:");
        var settings = new ClickHouseClientSettings
        {
            Host = host,
            Port = 8443,
            Protocol = "https",
            BearerToken = jwt,
        };

        using (var connection = new ClickHouseConnection(settings))
        {
            await connection.OpenAsync();
            var version = await connection.ExecuteScalarAsync("SELECT version()");
            Console.WriteLine($"   Connected to ClickHouse version: {version}");
        }

        // 2. JWT authentication can coexist with Username/Password
        // When BearerToken is set, it takes precedence over basic auth
        Console.WriteLine("\n2. BearerToken takes precedence over Username/Password:");
        var settingsWithBoth = new ClickHouseClientSettings
        {
            Host = host,
            Port = 8443,
            Protocol = "https",
            Username = "ignored_user",    // These are ignored when BearerToken is set
            Password = "ignored_password",
            BearerToken = jwt,        // Bearer token is used instead
        };

        using (var connection = new ClickHouseConnection(settingsWithBoth))
        {
            await connection.OpenAsync();
            Console.WriteLine("   Connected successfully using Bearer token (Username/Password ignored)");
        }

        // 3. Command-level token override
        // Useful when you need different tokens for different operations
        Console.WriteLine("\n3. Command-level token override:");
        using (var connection = new ClickHouseConnection(settings))
        {
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.BearerToken = jwt; // Override connection-level token
            command.CommandText = "SELECT currentUser()";

            var user = await command.ExecuteScalarAsync();
            Console.WriteLine($"   Current user: {user}");
        }

        // 4. Token refresh pattern (conceptual)
        // Not implemented yet

        Console.WriteLine("\nJWT authentication examples completed successfully!");
    }
}
