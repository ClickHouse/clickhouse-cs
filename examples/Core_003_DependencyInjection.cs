#if NET7_0_OR_GREATER
using System.Net;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to use ClickHouse with dependency injection in .NET 7+ applications.
/// Shows proper integration with IServiceCollection, ClickHouseDataSource, and IHttpClientFactory.
/// Also, loading options using the ConfigurationBuilder.
/// </summary>
public static class DependencyInjection
{
    public static async Task Run()
    {
        Console.WriteLine("ClickHouse Dependency Injection Examples (.NET 7+)\n");

        // Example 1: Basic registration with connection string
        Console.WriteLine("1. Basic registration with connection string:");
        await Example1_BasicRegistration();

        Console.WriteLine("\n2. Registration with ClickHouseClientSettings:");
        await Example2_SettingsRegistration();

        Console.WriteLine("\n3. Registration with IHttpClientFactory:");
        await Example3_HttpClientFactoryRegistration();

        Console.WriteLine("\nAll dependency injection examples completed!");
    }

    private static async Task Example1_BasicRegistration()
    {
        // Create a service collection (this would normally be done by the framework)
        var services = new ServiceCollection();

        // Add logging (optional but recommended)
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        // Register ClickHouse with a connection string
        services.AddClickHouseDataSource("Host=localhost;Port=8123;Protocol=http;Username=default;Password=;Database=default");

        // Build the service provider
        var serviceProvider = services.BuildServiceProvider();

        // Resolve and use a ClickHouseConnection
        // Note: ClickHouseConnection is registered as Transient by default
        using (var scope = serviceProvider.CreateScope())
        {
            var connection = scope.ServiceProvider.GetRequiredService<ClickHouseConnection>();
            await connection.OpenAsync();

            var version = await connection.ExecuteScalarAsync("SELECT version()");
            Console.WriteLine($"   Connected to ClickHouse version: {version}");
        }
    }

    private static async Task Example2_SettingsRegistration()
    {
        // Build configuration from appsettings.example.json
        // Use AppContext.BaseDirectory to get the directory where the assembly is located
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.example.json", optional: false, reloadOnChange: false)
            .Build();

        var services = new ServiceCollection();

        // Add logging, this will pull in the logging settings from our configuration file
        services.AddLogging(builder => builder.AddConsole());

        // Load ClickHouse settings from configuration
        var settings = configuration.GetSection("ClickHouse").Get<ClickHouseClientSettings>();

        if (settings == null)
        {
            throw new InvalidOperationException("Failed to load ClickHouse settings from configuration");
        }

        Console.WriteLine($"   Loaded settings from configuration: Host={settings.Host}, Database={settings.Database}");

        services.AddClickHouseDataSource(settings);

        var serviceProvider = services.BuildServiceProvider();

        using (var scope = serviceProvider.CreateScope())
        {
            // You can resolve either ClickHouseConnection, ClickHouseDataSource, or the interfaces
            var dataSource = scope.ServiceProvider.GetRequiredService<ClickHouseDataSource>();
            using var connection = dataSource.CreateConnection();
            await connection.OpenAsync();

            var version = await connection.ExecuteScalarAsync("SELECT version()");
            Console.WriteLine($"   Connected to ClickHouse version: {version}");
        }
    }

    private static async Task Example3_HttpClientFactoryRegistration()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        // Register a named HttpClient with custom configuration
        // This is important for production scenarios where you want to control:
        // - Timeout settings
        // - Retry policies (using Polly)
        // - Custom message handlers
        services.AddHttpClient("ClickHouseClient", client =>
        {
            client.Timeout = TimeSpan.FromMinutes(5);
        }).ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate, // Required for compression support
            MaxConnectionsPerServer = 100,
            PooledConnectionIdleTimeout = TimeSpan.FromSeconds(5), // Make sure to set this to a value lower than the server idle timeout (10s for Cloud)
        });

        // Register ClickHouse using the HttpClient factory
        var connectionString = "Host=localhost;Port=8123;Protocol=http;Username=default;Password=;Database=default";

        // Build the service provider so we can access IHttpClientFactory
        var serviceProvider = services.BuildServiceProvider();
        var factory = serviceProvider.GetRequiredService<IHttpClientFactory>();

        // Re-create service collection with ClickHouse registration
        services.AddClickHouseDataSource(
            connectionString,
            factory,
            httpClientName: "ClickHouseClient"
        );

        // Rebuild the service provider with the complete registration
        serviceProvider = services.BuildServiceProvider();

        using (var scope = serviceProvider.CreateScope())
        {
            var connection = scope.ServiceProvider.GetRequiredService<ClickHouseConnection>();
            await connection.OpenAsync();

            var database = connection.Database;
            Console.WriteLine($"   Connected to database: {database}");
        }
    }
}
#endif
