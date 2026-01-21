using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Diagnostic;
using ClickHouse.Driver.Utility;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates OpenTelemetry tracing with the ClickHouse driver.
/// The driver emits traces via System.Diagnostics.ActivitySource which can be
/// collected by OpenTelemetry and exported to various backends.
///
/// This example uses the Console Exporter to showcase the trace data. In production,
/// you would use something like the OpenTelemetry .NET client. If you already have an
/// OpenTelemetry setup, just add the "ClickHouse.Driver" source to your TracerProvider
/// and traces will flow automatically.
///
/// For a guide on setting up OpenTelemetry with ClickHouse as a backend, see:
/// https://clickhouse.com/docs/observability/integrating-opentelemetry
/// </summary>
public static class OpenTelemetryTracing
{
    public static async Task Run()
    {
        // ActivitySource is configurable through ClickHouseDiagnosticsOptions
        ClickHouseDiagnosticsOptions.IncludeSqlInActivityTags = true; // Disabled by default for security
        ClickHouseDiagnosticsOptions.StatementMaxLength = 500;

        // Set up OpenTelemetry with console exporter
        using var tracerProvider = Sdk.CreateTracerProviderBuilder()
            .AddSource(ClickHouseDiagnosticsOptions.ActivitySourceName)
            .SetResourceBuilder(ResourceBuilder.CreateDefault()
                .AddService("ClickHouse.Driver.Examples"))
            .AddConsoleExporter()
            .Build();

        Console.WriteLine("OpenTelemetry tracing configured");
        Console.WriteLine($"Listening to ActivitySource: {ClickHouseDiagnosticsOptions.ActivitySourceName}");
        Console.WriteLine("SQL in traces: enabled\n");

        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        // Query with results - shows read statistics
        await ExecuteQueryWithResults(connection);

        // Insert operation - shows write statistics
        await ExecuteInsert(connection);

        // Error trace demonstration
        await ExecuteWithError(connection);

        // Reset to default
        ClickHouseDiagnosticsOptions.IncludeSqlInActivityTags = false;
    }

    private static async Task ExecuteQueryWithResults(ClickHouseConnection connection)
    {
        Console.WriteLine("\n1. Query with results (shows read statistics):");
        Console.WriteLine("   Trace will include: db.clickhouse.read_rows, db.clickhouse.read_bytes\n");

        using var reader = await connection.ExecuteReaderAsync(
            "SELECT number, toString(number) as str FROM system.numbers LIMIT 100");

        var count = 0;
        while (reader.Read()) count++;
        Console.WriteLine($"   Read {count} rows\n");
    }

    private static async Task ExecuteInsert(ClickHouseConnection connection)
    {
        Console.WriteLine("\n2. Insert operation (shows write statistics):");
        Console.WriteLine("   Trace will include: db.clickhouse.written_rows, db.clickhouse.written_bytes\n");

        await connection.ExecuteStatementAsync(@"
            CREATE TABLE IF NOT EXISTS example_otel_trace (
                id UInt32,
                value String
            ) ENGINE = Memory
        ");

        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO example_otel_trace VALUES ({id:UInt32}, {value:String})";
        command.AddParameter("id", 1);
        command.AddParameter("value", "test");
        await command.ExecuteNonQueryAsync();

        Console.WriteLine("   Inserted 1 row\n");

        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS example_otel_trace");
    }

    private static async Task ExecuteWithError(ClickHouseConnection connection)
    {
        Console.WriteLine("\n3. Error trace (intentional error):");
        Console.WriteLine("   Trace will show: otel.status_code=ERROR, Activity.StatusDescription will include exception details\n");

        try
        {
            await connection.ExecuteScalarAsync("SELECT * FROM non_existent_table_12345");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   Expected error: {ex.Message.Split('\n')[0]}\n");
        }
    }
}
