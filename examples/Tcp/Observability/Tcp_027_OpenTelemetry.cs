using System.Diagnostics;
using ClickHouse.Driver.Diagnostic;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Utility;
using OpenTelemetry;
using OpenTelemetry.Trace;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Tracing the native client: <see cref="ClickHouseTcpDiagnostics.ActivitySourceName"/>, the spans it emits and
/// the attributes they carry, and <see cref="ClickHouseTcpClientOptions.IncludeSqlInActivityTags"/>.
///
/// <para>
/// The spans are collected here by an exporter that keeps them, and printed, because a console exporter's output
/// is too wide to read next to the code that produced it. The wiring is otherwise exactly what an application
/// does — <c>Sdk.CreateTracerProviderBuilder().AddSource(ClickHouseTcpDiagnostics.ActivitySourceName)</c> — so
/// swapping in <c>AddOtlpExporter()</c> is the only change needed to send these spans somewhere real.
/// </para>
///
/// <para>
/// The attribute names are the current OpenTelemetry database conventions (<c>db.system.name</c>,
/// <c>db.namespace</c>, <c>db.query.text</c>, <c>server.address</c>), which is where this transport differs from
/// the HTTP one: it still emits the older <c>db.system</c>/<c>db.statement</c> set. The two also use different
/// source names, so either can be collected without the other.
/// </para>
/// </summary>
public static class TcpOpenTelemetry
{
    private const string TableName = "example_tcp_open_telemetry";

    /// <summary>Stands in for the application's own instrumentation, so the client's spans have a parent.</summary>
    private static readonly ActivitySource AppSource = new("ClickHouse.Driver.Examples.Tcp027");

    public static async Task Run()
    {
        Console.WriteLine($"The native client's ActivitySource: {ClickHouseTcpDiagnostics.ActivitySourceName}");
        Console.WriteLine($"The HTTP transport's, for comparison: {ClickHouseDiagnosticsOptions.ActivitySourceName}\n");

        await using var client = ExampleConfig.CreateTcpClient();
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        await client.ExecuteAsync($"CREATE TABLE {TableName} (id UInt64, note String) ENGINE = MergeTree ORDER BY id");

        try
        {
            await OneSpanPerOperation();
            await TheParentChildShape();
            await StatementTextIsOptIn();
            await TheServerJoinsTheSameTrace(client);
            await TwoTransportsTwoSources();
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            Console.WriteLine($"\nDropped {TableName}.");
        }
    }

    private static async Task OneSpanPerOperation()
    {
        Console.WriteLine("1. One span per operation, named after the statement\n");

        var collector = new SpanCollector();
        using (TracerProvider provider = Collect(collector, ClickHouseTcpDiagnostics.ActivitySourceName))
        {
            await using var client = new ClickHouseTcpClient(Options() with
            {
                IncludeSqlInActivityTags = true,
                StatementMaxLength = 120,
            });

            await client.PingAsync();
            _ = await client.ExecuteScalarAsync("SELECT count() FROM numbers(50000)");
            await client.InsertRowsAsync(
                $"INSERT INTO {TableName} (id, note) VALUES",
                [[1UL, "one"], [2UL, "two"], [3UL, "three"]]);

            // An error after the server accepted the query, so the span records a failure and the connection
            // stays usable.
            try
            {
                _ = await client.ExecuteScalarAsync("SELECT * FROM example_tcp_open_telemetry_no_such_table");
            }
            catch (ClickHouseTcpServerException)
            {
            }
        }

        foreach (Activity span in collector.Spans)
        {
            Print(span);
        }

        Console.WriteLine("   The span name is the statement's leading keyword, uppercased, which keeps it low");
        Console.WriteLine("   cardinality — a generated statement that does not start with a word is named 'query'.");
        Console.WriteLine("   A Ping is its own span, and so is a dial (below).");
        Console.WriteLine();
        Console.WriteLine("   db.clickhouse.read_rows and read_bytes come from the server's Progress packets, so they");
        Console.WriteLine("   describe what the query read rather than what it returned; result_rows and result_bytes");
        Console.WriteLine("   are the execution summary. An insert has neither pair: the server sends no Progress for");
        Console.WriteLine("   rows streamed to it, so db.clickhouse.written_rows is the client's own count.");
    }

    private static async Task TheParentChildShape()
    {
        Console.WriteLine("\n2. Where the client's spans sit in a trace\n");

        var collector = new SpanCollector();
        using (TracerProvider provider = Collect(collector, ClickHouseTcpDiagnostics.ActivitySourceName, AppSource.Name))
        {
            // The application's own span. Everything the client starts while it is current becomes a descendant.
            using (Activity? request = AppSource.StartActivity("handle-request"))
            {
                // A client of its own, so its pool is empty and the first operation has to dial.
                await using var client = new ClickHouseTcpClient(Options() with
                {
                    IncludeSqlInActivityTags = true,
                    StatementMaxLength = 60,
                });

                _ = await client.ExecuteScalarAsync("SELECT 'first, so this one dials'");
                _ = await client.ExecuteScalarAsync("SELECT 'second, so this one does not'");
            }
        }

        PrintTree(collector.Spans);

        Console.WriteLine();
        Console.WriteLine("   'connect' covers the socket connect, the TLS negotiation and the handshake, and it is a");
        Console.WriteLine("   child of whichever operation had to wait for the connection — so a slow first request");
        Console.WriteLine("   shows why in the trace rather than only in the total. The second statement has no such");
        Console.WriteLine("   child because it reused the pooled connection.");
        Console.WriteLine();
        Console.WriteLine("   With no ambient Activity the client's spans are roots, one trace each — which is what");
        Console.WriteLine("   section 1 above produced. A parent is also what the server is told about, so the shape");
        Console.WriteLine("   above reaches further than this process: section 4.");
    }

    private static async Task StatementTextIsOptIn()
    {
        Console.WriteLine("\n3. IncludeSqlInActivityTags, and how much text it lets through\n");

        const string sql = "SELECT 'a statement long enough that StatementMaxLength has something to cut'";

        (bool include, int max)[] cases =
        [
            (false, 200),
            (true, 40),
            (true, 200),
            (true, 0),
        ];

        foreach ((bool include, int max) in cases)
        {
            var collector = new SpanCollector();
            using (TracerProvider provider = Collect(collector, ClickHouseTcpDiagnostics.ActivitySourceName))
            {
                await using var client = new ClickHouseTcpClient(Options() with
                {
                    IncludeSqlInActivityTags = include,
                    StatementMaxLength = max,
                });

                _ = await client.ExecuteScalarAsync(sql);
            }

            Activity span = collector.Spans.First(s => s.OperationName == "SELECT");
            object? text = span.GetTagItem("db.query.text");
            Console.WriteLine($"     IncludeSqlInActivityTags = {include,-5}  StatementMaxLength = {max,3}  db.query.text = {(text is null ? "(not set)" : "\"" + text + "\"")}");
        }

        Console.WriteLine();
        Console.WriteLine($"   The statement was {sql.Length} characters. Off is the default, because a statement can carry");
        Console.WriteLine("   data a trace is not meant to hold — a literal in a WHERE clause is often the very value");
        Console.WriteLine("   you are not allowed to export. StatementMaxLength defaults to 5, a stub rather than a");
        Console.WriteLine("   statement, so recording query text takes both settings; zero suppresses the attribute");
        Console.WriteLine("   even with the opt-in on. It caps the Debug log line by the same rule (Tcp_026).");
    }

    /// <summary>
    /// The client writes the current span's W3C trace context into the Query packet, so the spans the server
    /// records for the same query land under the caller's trace id.
    /// </summary>
    /// <param name="reader">A client for reading the server's span log, whose own spans are not collected.</param>
    private static async Task TheServerJoinsTheSameTrace(ClickHouseTcpClient reader)
    {
        Console.WriteLine("\n4. The server's own spans join the same trace\n");

        var collector = new SpanCollector();
        string traceId;

        using (TracerProvider provider = Collect(collector, ClickHouseTcpDiagnostics.ActivitySourceName, AppSource.Name))
        {
            await using var client = new ClickHouseTcpClient(Options());

            using Activity? request = AppSource.StartActivity("handle-request");
            traceId = request!.TraceId.ToHexString();
            _ = await client.ExecuteScalarAsync("SELECT count() FROM numbers(100000)");
        }

        Console.WriteLine($"   Trace id on this side: {traceId}");
        Console.WriteLine($"   Spans collected here:  {string.Join(", ", collector.Spans.Select(s => s.OperationName))}");

        // The server's spans are queued like any system log, so the flush and the read are retried rather than
        // read once — the same shape Tcp_020 uses for system.query_log.
        long serverSpans = 0;
        var names = new List<string>();
        for (int attempt = 1; attempt <= 5 && serverSpans == 0; attempt++)
        {
            await reader.ExecuteAsync("SYSTEM FLUSH LOGS");
            serverSpans = Convert.ToInt64(await reader.ExecuteScalarAsync(
                "SELECT count() FROM system.opentelemetry_span_log WHERE lower(hex(trace_id)) = {trace:String}",
                new ClickHouseTcpQueryOptions
                {
                    Parameters = new ClickHouseTcpParameterCollection { { "trace", traceId } },
                }));

            if (serverSpans == 0)
            {
                await Task.Delay(50);
            }
        }

        await foreach (object[] row in reader.QueryAsync(
            "SELECT DISTINCT operation_name FROM system.opentelemetry_span_log " +
            "WHERE lower(hex(trace_id)) = {trace:String} ORDER BY operation_name LIMIT 6",
            new ClickHouseTcpQueryOptions
            {
                Parameters = new ClickHouseTcpParameterCollection { { "trace", traceId } },
            }))
        {
            names.Add((string)row[0]);
        }

        Console.WriteLine($"   Spans the server recorded under the same trace id: {serverSpans}");
        Console.WriteLine($"     {string.Join(", ", names)}");
        Console.WriteLine();
        Console.WriteLine("   The Query packet's ClientInfo carries the W3C trace context of Activity.Current when the");
        Console.WriteLine("   negotiated protocol revision is 54442 or newer, which every supported server is. So the");
        Console.WriteLine("   server's account of the query — every stage, in system.opentelemetry_span_log — is part");
        Console.WriteLine("   of the same trace as the request that issued it, with no header to set and nothing to");
        Console.WriteLine("   correlate by hand. That is the strongest reason to give the client an ambient Activity.");
        Console.WriteLine();
        Console.WriteLine("   Two conditions. The current Activity's id has to be W3C, which it is unless something");
        Console.WriteLine("   set ActivityIdFormat.Hierarchical; and the server has to have its span log switched on,");
        Console.WriteLine("   which the stock configuration does. The flush above is only so this example can read the");
        Console.WriteLine("   table immediately; nothing about the propagation needs it.");
        Console.WriteLine();
        Console.WriteLine("   db.clickhouse.query_id is the other join, and it needs a QueryId you chose (Tcp_020):");
        Console.WriteLine("   when you supply none, the id the server assigns never reaches the client.");
    }

    private static async Task TwoTransportsTwoSources()
    {
        Console.WriteLine("\n5. The two transports are separate sources\n");

        // Only the native source. The HTTP query below runs, and is not collected.
        var nativeOnly = new SpanCollector();
        using (TracerProvider provider = Collect(nativeOnly, ClickHouseTcpDiagnostics.ActivitySourceName))
        {
            await BothTransports();
        }

        // Both sources, same workload.
        var both = new SpanCollector();
        using (TracerProvider provider = Collect(
            both,
            ClickHouseTcpDiagnostics.ActivitySourceName,
            ClickHouseDiagnosticsOptions.ActivitySourceName))
        {
            await BothTransports();
        }

        Console.WriteLine("   One native query and one HTTP query, collected twice:\n");
        Report("AddSource(native)", nativeOnly);
        Report("AddSource(native, http)", both);

        Console.WriteLine();
        Console.WriteLine("   So a service that has moved its reads to the native client and left its writes on HTTP");
        Console.WriteLine("   can trace one, the other, or both, and tell them apart in the backend by source. The");
        Console.WriteLine("   attribute sets differ as well: the HTTP transport emits db.system and db.statement, this");
        Console.WriteLine("   one db.system.name and db.query.text, so a dashboard built on one does not read the");
        Console.WriteLine("   other without a rule for each. The span names differ too — the HTTP one is named after");
        Console.WriteLine("   the driver method that ran, this one after the statement's keyword.");
        Console.WriteLine();
        Console.WriteLine("   Their opt-ins are separate too, and shaped differently: the HTTP transport's live on the");
        Console.WriteLine("   static ClickHouseDiagnosticsOptions, so they are process-wide, while the native client's");
        Console.WriteLine("   are per client, on the options record.");

        static void Report(string label, SpanCollector collector)
        {
            IEnumerable<string> byTransport = collector.Spans
                .GroupBy(s => s.Source.Name)
                .OrderBy(g => g.Key, StringComparer.Ordinal)
                .Select(g => $"{g.Key} -> {string.Join(", ", g.Select(s => s.OperationName))}");

            Console.WriteLine($"     {label,-24} {collector.Spans.Count} span(s): {string.Join("; ", byTransport)}");
        }
    }

    /// <summary>One query over each transport, so a collector can be asked which of them it saw.</summary>
    private static async Task BothTransports()
    {
        await using var tcp = new ClickHouseTcpClient(Options());
        _ = await tcp.ExecuteScalarAsync("SELECT 'over the native protocol'");

        using var http = ExampleConfig.CreateHttpClient();
        _ = await http.ExecuteScalarAsync("SELECT 'over HTTP'");
    }

    private static ClickHouseTcpClientOptions Options() => ExampleConfig.TcpBuilder().ToOptions();

    /// <summary>
    /// The wiring an application writes, with an exporter that keeps the spans instead of printing them.
    /// </summary>
    private static TracerProvider Collect(SpanCollector collector, params string[] sources)
        => Sdk.CreateTracerProviderBuilder()
            .AddSource(sources)
            .AddProcessor(new SimpleActivityExportProcessor(collector))
            .Build()!;

    private static void Print(Activity span)
    {
        Console.WriteLine($"     {span.OperationName,-8} {span.Kind,-6} {span.Status,-5} {span.Duration.TotalMilliseconds,7:0.0} ms");
        foreach (KeyValuePair<string, object?> tag in span.TagObjects)
        {
            Console.WriteLine($"       {tag.Key,-30} {tag.Value}");
        }

        foreach (ActivityEvent e in span.Events)
        {
            Console.WriteLine($"       event {e.Name,-24} {e.Tags.FirstOrDefault(t => t.Key == "exception.type").Value}");
        }

        Console.WriteLine();
    }

    /// <summary>Prints the spans indented by depth, which is what a trace viewer draws.</summary>
    private static void PrintTree(IReadOnlyList<Activity> spans)
    {
        var byId = spans.ToDictionary(s => s.SpanId.ToHexString(), StringComparer.Ordinal);

        foreach (Activity span in spans.OrderBy(s => s.StartTimeUtc))
        {
            int depth = 0;
            for (Activity? walk = span; walk is not null && depth < 8;)
            {
                walk = byId.TryGetValue(walk.ParentSpanId.ToHexString(), out Activity? parent) ? parent : null;
                if (walk is not null)
                {
                    depth++;
                }
            }

            string? sql = span.GetTagItem("db.query.text") as string;
            Console.WriteLine($"     {new string(' ', depth * 3)}{span.OperationName,-8} {span.Duration.TotalMilliseconds,7:0.0##} ms{(sql is null ? string.Empty : "  " + sql)}");
        }
    }

    /// <summary>
    /// A <see cref="BaseExporter{T}"/> that keeps what it is given. Registered through
    /// <see cref="SimpleActivityExportProcessor"/>, so each span is handed over as it ends and nothing has to be
    /// flushed before a section prints.
    /// </summary>
    private sealed class SpanCollector : BaseExporter<Activity>
    {
        private readonly List<Activity> spans = [];

        public IReadOnlyList<Activity> Spans
        {
            get
            {
                lock (spans)
                {
                    return spans.ToArray();
                }
            }
        }

        public override ExportResult Export(in Batch<Activity> batch)
        {
            lock (spans)
            {
                foreach (Activity span in batch)
                {
                    spans.Add(span);
                }
            }

            return ExportResult.Success;
        }
    }
}
