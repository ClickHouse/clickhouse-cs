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
///
/// <para>
/// <strong>Recommended Approach: ClickHouseClient as Singleton</strong>
/// </para>
/// <para>
/// <see cref="ClickHouseClient"/> is the recommended API for new code. It is thread-safe, manages
/// HTTP connection pooling internally, and is designed for singleton usage. Register it as a singleton
/// in your DI container and inject it wherever you need to interact with ClickHouse.
/// </para>
///
/// <para>
/// <strong>ADO.NET Compatibility: ClickHouseDataSource</strong>
/// </para>
/// <para>
/// For ADO.NET compatibility (e.g., with ORMs like Dapper or EF Core), use <see cref="ClickHouseDataSource"/>.
/// The DataSource owns a <see cref="ClickHouseClient"/> internally and creates connections that share it.
/// </para>
///
/// <para>
/// <strong>Connection Pooling</strong>
/// </para>
/// <para>
/// Both <see cref="ClickHouseClient"/> and <see cref="ClickHouseDataSource"/> manage HTTP connection pooling.
/// The "connection" in ClickHouse is a logical concept - the actual HTTP connections are pooled and reused.
/// You don't need to worry about connection exhaustion as long as you use a singleton client or data source.
/// </para>
/// </summary>
public static class DependencyInjection
{
    public static async Task Run()
    {
        Console.WriteLine("ClickHouse Dependency Injection Examples (.NET 7+)\n");

        // Example 1: ClickHouseClient as singleton (RECOMMENDED)
        Console.WriteLine("1. ClickHouseClient as singleton (recommended):");
        await Example1_ClientAsSingleton();

        // Example 2: ClickHouseDataSource for ADO.NET compatibility
        Console.WriteLine("\n2. ClickHouseDataSource for ADO.NET compatibility:");
        await Example2_DataSourceRegistration();

        // Example 3: With IHttpClientFactory
        Console.WriteLine("\n3. Registration with IHttpClientFactory:");
        await Example3_HttpClientFactoryRegistration();

        Console.WriteLine("\nAll dependency injection examples completed!");
    }

    private static async Task Example1_ClientAsSingleton()
    {
        // Create a service collection (this would normally be done by the framework)
        var services = new ServiceCollection();

        // Add logging (optional but recommended)
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        // Register ClickHouseClient as a singleton.
        // This is the recommended approach for new code.
        services.AddSingleton<ClickHouseClient>(sp =>
        {
            var loggerFactory = sp.GetService<ILoggerFactory>();
            return new ClickHouseClient(new ClickHouseClientSettings
            {
                Host = "localhost",
                LoggerFactory = loggerFactory,
            });
        });

        // Build the service provider
        var serviceProvider = services.BuildServiceProvider();

        // Resolve and use the ClickHouseClient directly
        var client = serviceProvider.GetRequiredService<ClickHouseClient>();

        var version = (string)await client.ExecuteScalarAsync("SELECT version()");
        Console.WriteLine($"   Connected to ClickHouse version: {version}");

        // The client can be injected anywhere in your application
        // It's thread-safe and designed for concurrent use
    }

    private static async Task Example2_DataSourceRegistration()
    {
        // Build configuration from appsettings.example.json
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.example.json", optional: false, reloadOnChange: false)
            .Build();

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        // Load ClickHouse settings from configuration
        var settings = configuration.GetSection("ClickHouse").Get<ClickHouseClientSettings>();

        if (settings == null)
        {
            throw new InvalidOperationException("Failed to load ClickHouse settings from configuration");
        }

        Console.WriteLine($"   Loaded settings from configuration: Host={settings.Host}, Database={settings.Database}");

        // Register ClickHouseDataSource for ADO.NET compatibility (e.g., with ORMs)
        // The DataSource owns a ClickHouseClient internally
        services.AddClickHouseDataSource(settings);

        var serviceProvider = services.BuildServiceProvider();

        using (var scope = serviceProvider.CreateScope())
        {
            // Resolve ClickHouseConnection - all connections share the same underlying client
            var connection = scope.ServiceProvider.GetRequiredService<ClickHouseConnection>();
            await connection.OpenAsync();

            var version = await connection.ExecuteScalarAsync("SELECT version()");
            Console.WriteLine($"   Connected to ClickHouse version: {version}");
        }
    }

    private static async Task Example3_HttpClientFactoryRegistration()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        // Register a named HttpClient with custom configuration.
        // This is important for production scenarios where you want to control
        // connection pooling, timeouts, and other HTTP settings.
        services.AddHttpClient("ClickHouseClient", client =>
        {
            client.Timeout = TimeSpan.FromMinutes(5);
        }).ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
            MaxConnectionsPerServer = 100,
            PooledConnectionIdleTimeout = TimeSpan.FromSeconds(5), // Keep lower than server idle timeout (10s for Cloud)
        });

        // Option A: Register ClickHouseClient directly (recommended for new code)
        services.AddSingleton<ClickHouseClient>(sp =>
        {
            var factory = sp.GetRequiredService<IHttpClientFactory>();
            var loggerFactory = sp.GetService<ILoggerFactory>();
            return new ClickHouseClient(new ClickHouseClientSettings
            {
                Host = "localhost",
                HttpClientFactory = factory,
                HttpClientName = "ClickHouseClient",
                LoggerFactory = loggerFactory,
            });
        });

        // Option B: Also register DataSource for ADO.NET compatibility
        services.AddClickHouseDataSource(sp =>
        {
            var factory = sp.GetRequiredService<IHttpClientFactory>();
            return new ClickHouseClientSettings
            {
                Host = "localhost",
                HttpClientFactory = factory,
                HttpClientName = "ClickHouseClient",
            };
        });

        var serviceProvider = services.BuildServiceProvider();

        // Use ClickHouseClient directly
        var client = serviceProvider.GetRequiredService<ClickHouseClient>();
        var version = (string)await client.ExecuteScalarAsync("SELECT version()");
        Console.WriteLine($"   Using ClickHouseClient: {version}");

        // Or use ClickHouseConnection for ADO.NET compatibility
        using (var scope = serviceProvider.CreateScope())
        {
            var connection = scope.ServiceProvider.GetRequiredService<ClickHouseConnection>();
            await connection.OpenAsync();
            version = (string)await connection.ExecuteScalarAsync("SELECT version()");
            Console.WriteLine($"   Using ClickHouseConnection: {version}");
        }
    }
}
#endif
