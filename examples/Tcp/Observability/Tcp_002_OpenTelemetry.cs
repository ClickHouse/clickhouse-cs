using System.Diagnostics;
using ClickHouse.Driver.Tcp;
using OpenTelemetry;
using OpenTelemetry.Trace;

namespace ClickHouse.Driver.Examples;

/// <summary>Collects OpenTelemetry spans emitted by the native client.</summary>
public static class TcpOpenTelemetry
{
    public static async Task Run()
    {
        var exporter = new ActivityCollector();

        // Subscribe to the driver's ActivitySource before creating the client.
        using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
            .AddSource(ClickHouseTcpDiagnostics.ActivitySourceName)
            .AddProcessor(new SimpleActivityExportProcessor(exporter))
            .Build()!;

        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions() with
        {
            IncludeSqlInActivityTags = true,
            StatementMaxLength = 100,
        };

        await using (var client = new ClickHouseTcpClient(options))
        {
            await client.PingAsync();
            await client.ExecuteScalarAsync("SELECT count() FROM numbers(1000)");
        }

        foreach (Activity activity in exporter.Activities)
        {
            Console.WriteLine(
                $"{activity.OperationName}: {activity.Status}, " +
                $"{activity.Duration.TotalMilliseconds:0.0} ms");

            foreach (KeyValuePair<string, object?> tag in activity.TagObjects)
            {
                Console.WriteLine($"  {tag.Key} = {tag.Value}");
            }
        }

        // SQL can contain sensitive data. IncludeSqlInActivityTags is disabled by default.
    }

    private sealed class ActivityCollector : BaseExporter<Activity>
    {
        private readonly List<Activity> activities = [];

        public IReadOnlyList<Activity> Activities => activities;

        public override ExportResult Export(in Batch<Activity> batch)
        {
            foreach (Activity activity in batch)
            {
                activities.Add(activity);
            }

            return ExportResult.Success;
        }
    }
}
