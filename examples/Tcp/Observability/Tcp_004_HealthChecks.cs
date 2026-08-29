using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ClickHouse.Driver.Examples;

/// <summary>Uses PingAsync in a Microsoft.Extensions.Diagnostics health check.</summary>
public static class TcpHealthChecks
{
    public static async Task Run()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddClickHouseTcpDataSource(ExampleConfig.TcpConnectionString);
        services.AddHealthChecks().Add(new HealthCheckRegistration(
            "clickhouse-native",
            provider => new TcpPingHealthCheck(
                provider.GetRequiredService<IClickHouseTcpClient>()),
            failureStatus: HealthStatus.Unhealthy,
            tags: new[] { "clickhouse", "ready" }));

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthReport report = await provider
            .GetRequiredService<HealthCheckService>()
            .CheckHealthAsync();

        foreach ((string name, HealthReportEntry entry) in report.Entries)
        {
            Console.WriteLine($"{name}: {entry.Status} ({entry.Description})");
        }
    }

    private sealed class TcpPingHealthCheck(IClickHouseTcpClient client) : IHealthCheck
    {
        public async Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                // Ping verifies native connectivity, not access to an application table.
                await client.PingAsync(cancellationToken);
                return HealthCheckResult.Healthy("Pong");
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return new HealthCheckResult(
                    context.Registration.FailureStatus,
                    ex.Message,
                    ex);
            }
        }
    }
}
