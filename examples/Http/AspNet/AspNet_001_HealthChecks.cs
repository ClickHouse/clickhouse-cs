#if NET7_0_OR_GREATER
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to implement ASP.NET health checks for ClickHouse.
/// Shows BOTH the ClickHouseClient and ClickHouseConnection approaches for health checks.
///
/// API CHOICE GUIDE:
/// - ClickHouseClient: Recommended for direct database operations (queries, inserts)
/// - ClickHouseDataSource + ClickHouseConnection: Required for ORMs (Dapper, EF Core, linq2db)
///
/// For health checks, either approach works. This example shows both patterns.
/// </summary>
public static class AspNetHealthChecks
{
    public static async Task Run()
    {
        Console.WriteLine("ClickHouse ASP.NET Health Checks Example\n");

        var connectionString = "Host=localhost;Port=8123;Protocol=http;Username=default;Password=;Database=default";

        // =======================================================================
        // OPTION 1: Using ClickHouseClient (recommended for direct operations)
        // =======================================================================
        Console.WriteLine("1. Health check using ClickHouseClient:");
        await DemoClientHealthCheck(connectionString);

        // =======================================================================
        // OPTION 2: Using ClickHouseDataSource + ClickHouseConnection (for ORMs)
        // =======================================================================
        Console.WriteLine("\n2. Health check using ClickHouseDataSource (for ADO.NET/ORM compatibility):");
        await DemoDataSourceHealthCheck(connectionString);

        Console.WriteLine("\nAll health check examples completed!");
    }

    private static async Task DemoClientHealthCheck(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        // Register ClickHouseClient as a singleton
        services.AddSingleton(_ => new ClickHouseClient(connectionString));

        // Register health check using ClickHouseClient
        services.AddHealthChecks()
            .AddClickHouseClient(name: "clickhouse-client", tags: ["database", "clickhouse"]);

        var serviceProvider = services.BuildServiceProvider();
        var healthCheckService = serviceProvider.GetRequiredService<HealthCheckService>();
        var report = await healthCheckService.CheckHealthAsync();

        Console.WriteLine($"   Overall status: {report.Status}");
        foreach (var entry in report.Entries)
        {
            Console.WriteLine($"   - {entry.Key}: {entry.Value.Status}");
        }
    }

    private static async Task DemoDataSourceHealthCheck(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        // Register ClickHouse data source (for ADO.NET/ORM compatibility)
        services.AddClickHouseDataSource(connectionString);

        // Register health check using ClickHouseConnection
        services.AddHealthChecks()
            .AddClickHouseConnection(name: "clickhouse-connection", tags: ["database", "clickhouse"]);

        var serviceProvider = services.BuildServiceProvider();
        var healthCheckService = serviceProvider.GetRequiredService<HealthCheckService>();
        var report = await healthCheckService.CheckHealthAsync();

        Console.WriteLine($"   Overall status: {report.Status}");
        foreach (var entry in report.Entries)
        {
            Console.WriteLine($"   - {entry.Key}: {entry.Value.Status}");
        }
    }
}

/// <summary>
/// Health check implementation using ClickHouseClient.
/// Recommended for applications that use ClickHouseClient directly.
/// </summary>
public class ClickHouseClientHealthCheck : IHealthCheck
{
    private readonly ClickHouseClient _client;

    public ClickHouseClientHealthCheck(ClickHouseClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.ExecuteScalarAsync("SELECT 1", cancellationToken: cancellationToken).ConfigureAwait(false);
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return new HealthCheckResult(
                context.Registration.FailureStatus,
                description: ex.Message,
                exception: ex);
        }
    }
}

/// <summary>
/// Health check implementation using ClickHouseConnection.
/// Use this when your application uses ClickHouseDataSource for ORM compatibility.
/// </summary>
public class ClickHouseConnectionHealthCheck : IHealthCheck
{
    private readonly ClickHouseConnection _connection;

    public ClickHouseConnectionHealthCheck(ClickHouseConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT 1";
            await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return new HealthCheckResult(
                context.Registration.FailureStatus,
                description: ex.Message,
                exception: ex);
        }
    }
}

/// <summary>
/// Extension methods for adding ClickHouse health checks to the health check builder.
/// </summary>
public static class ClickHouseHealthCheckBuilderExtensions
{
    /// <summary>
    /// Adds a health check for ClickHouse using ClickHouseClient from DI.
    /// Recommended for applications that use ClickHouseClient directly.
    /// </summary>
    public static IHealthChecksBuilder AddClickHouseClient(
        this IHealthChecksBuilder builder,
        string name = "clickhouse",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new ClickHouseClientHealthCheck(sp.GetRequiredService<ClickHouseClient>()),
            failureStatus,
            tags,
            timeout));
    }

    /// <summary>
    /// Adds a health check for ClickHouse using ClickHouseConnection from DI.
    /// Use this when your application uses ClickHouseDataSource for ORM compatibility.
    /// </summary>
    public static IHealthChecksBuilder AddClickHouseConnection(
        this IHealthChecksBuilder builder,
        string name = "clickhouse",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new ClickHouseConnectionHealthCheck(sp.GetRequiredService<ClickHouseConnection>()),
            failureStatus,
            tags,
            timeout));
    }
}
#endif
