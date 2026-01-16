#if NET7_0_OR_GREATER
using ClickHouse.Driver.ADO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to implement ASP.NET health checks for ClickHouse.
/// Shows the health check implementation and how to register it in an ASP.NET application.
/// </summary>
public static class AspNetHealthChecks
{
    public static async Task Run()
    {
        Console.WriteLine("ClickHouse ASP.NET Health Checks Example\n");

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        var connectionString = "Host=localhost;Port=8123;Protocol=http;Username=default;Password=;Database=default";

        // Register ClickHouse data source
        services.AddClickHouseDataSource(connectionString);

        // Register health checks using the extension method
        services.AddHealthChecks()
            .AddClickHouse(name: "clickhouse", tags: ["database", "clickhouse"]);

        var serviceProvider = services.BuildServiceProvider();

        // Resolve and run the health check
        var healthCheckService = serviceProvider.GetRequiredService<HealthCheckService>();
        var report = await healthCheckService.CheckHealthAsync();

        Console.WriteLine($"   Overall status: {report.Status}");
        foreach (var entry in report.Entries)
        {
            Console.WriteLine($"   - {entry.Key}: {entry.Value.Status}");
            if (!string.IsNullOrEmpty(entry.Value.Description))
            {
                Console.WriteLine($"     Description: {entry.Value.Description}");
            }
        }

        Console.WriteLine("\nAll health check examples completed!");
    }
}

/// <summary>
/// A health check for ClickHouse databases.
/// </summary>
public class ClickHouseHealthCheck : IHealthCheck
{
    private readonly ClickHouseConnection _connection;

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseHealthCheck"/> class.
    /// </summary>
    /// <param name="connection">The ClickHouse connection to use for health checks.</param>
    public ClickHouseHealthCheck(ClickHouseConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
    }

    /// <inheritdoc />
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
    /// Adds a health check for ClickHouse using a ClickHouseConnection from DI.
    /// </summary>
    /// <param name="builder">The health check builder.</param>
    /// <param name="name">The name of the health check. Defaults to "clickhouse".</param>
    /// <param name="failureStatus">The status to report when the health check fails. Defaults to Unhealthy.</param>
    /// <param name="tags">Optional tags for the health check.</param>
    /// <param name="timeout">Optional timeout for the health check.</param>
    /// <returns>The health check builder for chaining.</returns>
    public static IHealthChecksBuilder AddClickHouse(
        this IHealthChecksBuilder builder,
        string name = "clickhouse",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new ClickHouseHealthCheck(sp.GetRequiredService<ClickHouseConnection>()),
            failureStatus,
            tags,
            timeout));
    }
}
#endif
