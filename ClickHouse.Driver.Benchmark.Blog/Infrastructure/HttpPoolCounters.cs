using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Globalization;

namespace ClickHouse.Driver.Benchmark.Blog.Infrastructure;

/// <summary>
/// Subscribes to the runtime's own <c>System.Net.Http</c> and <c>System.Net.Sockets</c> event counters
/// so the overhead-bound family can report connection-pool health, not just latency.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this is not the EventListener the plan deleted.</b> That one duplicated GC telemetry
/// BenchmarkDotNet already collects via <c>EventPipeProfiler</c>. This one covers something
/// BenchmarkDotNet has no diagnoser for at all, and it is the direct evidence for the specific claim
/// the family is built around: a leaked <c>HttpResponseMessage</c> keeps its pooled connection
/// checked out until finalization, so the pool grows connections it should have been reusing.
/// <c>http11_connections_current_total</c> is that, measured.
/// </para>
/// <para>
/// Latency alone cannot make that argument. A degraded pool and a slow server both show up as a fat
/// p99; only the connection count distinguishes them.
/// </para>
/// <para>
/// Counters are polled by the runtime at <see cref="IntervalSeconds"/>, so values are a coarse
/// time series, not per-request truth. Peak and final are what matter here — a pool that ends a
/// 45-second burst with 60 open connections for a concurrency-8 workload has a problem regardless of
/// how it got there.
/// </para>
/// </remarks>
internal sealed class HttpPoolCounters : EventListener
{
    /// <summary>Counter polling interval. 1 s: fine enough to see growth, coarse enough to be free.</summary>
    public const int IntervalSeconds = 1;

    private const string HttpSource = "System.Net.Http";
    private const string SocketsSource = "System.Net.Sockets";

    /// <summary>
    /// Counters kept, and whether they are a level (take max/last) or a rate (take max).
    /// Named exactly as the runtime emits them.
    /// </summary>
    private static readonly string[] Tracked =
    [
        "http11-connections-current-total",
        "http20-connections-current-total",
        "current-requests",
        "requests-queue-duration",
        "requests-started-rate",
        "current-outgoing-connect-attempts",
        "outgoing-connections-established",
        "bytes-sent",
        "bytes-received",
    ];

    private readonly object gate = new();
    private readonly Dictionary<string, double> peak = [];
    private readonly Dictionary<string, double> last = [];
    private readonly List<EventSource> enabled = [];

    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (eventSource.Name is not (HttpSource or SocketsSource))
            return;

        // EnableEvents can fire before the constructor's field initializers have run when the
        // EventSource already exists, so nothing here may touch instance state that is not ready.
        EnableEvents(
            eventSource,
            EventLevel.LogAlways,
            EventKeywords.All,
            new Dictionary<string, string?>
            {
                ["EventCounterIntervalSec"] = IntervalSeconds.ToString(CultureInfo.InvariantCulture),
            });

        lock (gate)
            enabled.Add(eventSource);
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        if (eventData.EventName != "EventCounters" || eventData.Payload is null)
            return;

        foreach (var item in eventData.Payload)
        {
            if (item is not IDictionary<string, object> payload)
                continue;

            if (payload.TryGetValue("Name", out var nameObj) && nameObj is string name && Array.IndexOf(Tracked, name) >= 0)
            {
                // Levels report "Mean", rates report "Increment". Take whichever is present.
                var value = payload.TryGetValue("Mean", out var mean) ? ToDouble(mean)
                    : payload.TryGetValue("Increment", out var inc) ? ToDouble(inc)
                    : double.NaN;

                if (double.IsNaN(value))
                    continue;

                lock (gate)
                {
                    last[name] = value;
                    peak[name] = peak.TryGetValue(name, out var existing) ? Math.Max(existing, value) : value;
                }
            }
        }
    }

    /// <summary>Clears the accumulated window. Call at the start of each measured burst.</summary>
    public void Reset()
    {
        lock (gate)
        {
            peak.Clear();
            last.Clear();
        }
    }

    /// <summary>
    /// Records peak and final values for every tracked counter. Metric names are prefixed
    /// <c>pool_</c> and normalized to snake_case so they group in the CSV.
    /// </summary>
    public void RecordTo(string benchmark, string args)
    {
        lock (gate)
        {
            if (peak.Count == 0)
            {
                // A burst shorter than the polling interval yields no samples at all. Say so rather
                // than record zeros that would read as "no connections were ever open".
                SideMetrics.Record(benchmark, args, "pool_samples", 0);
                return;
            }

            SideMetrics.Record(benchmark, args, "pool_samples", peak.Count);

            foreach (var (name, value) in peak)
                SideMetrics.Record(benchmark, args, "pool_peak_" + Normalize(name), value);

            foreach (var (name, value) in last)
                SideMetrics.Record(benchmark, args, "pool_final_" + Normalize(name), value);
        }
    }

    public override void Dispose()
    {
        lock (gate)
        {
            foreach (var source in enabled)
            {
                try
                {
                    DisableEvents(source);
                }
                catch (Exception)
                {
                    // The source may already be disposed during process teardown; nothing to do.
                }
            }

            enabled.Clear();
        }

        base.Dispose();
    }

    private static string Normalize(string counterName) =>
        counterName.Replace('-', '_');

    private static double ToDouble(object? value) => value switch
    {
        double d => d,
        float f => f,
        long l => l,
        int i => i,
        null => double.NaN,
        _ => double.TryParse(value.ToString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) ? parsed : double.NaN,
    };
}
