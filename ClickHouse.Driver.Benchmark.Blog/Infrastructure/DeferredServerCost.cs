using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Benchmark.Blog.Infrastructure;

/// <summary>
/// Queues server-cost lookups during a measured region and performs them afterwards.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this type exists at all.</b> Reading <c>system.query_log</c> costs a <c>SYSTEM FLUSH LOGS</c>
/// plus a query, and retries with a delay when the record has not materialised yet. Doing that inside
/// a benchmark method means BenchmarkDotNet times it: a 200 ms retry sleep lands in the reported
/// duration of the thing being measured, and the "server CPU" column silently becomes
/// "server CPU plus however long the log took to flush".
/// </para>
/// <para>
/// So the measured region only generates a query id — free — and the lookup happens from
/// <c>[IterationCleanup]</c>, which BenchmarkDotNet runs outside the timed region.
/// </para>
/// <para>
/// <see cref="Drain"/> blocks rather than being async, because BenchmarkDotNet's iteration-level
/// hooks are synchronous. Blocking outside the timed region costs nothing that is being measured.
/// </para>
/// </remarks>
internal sealed class DeferredServerCost
{
    private readonly List<Entry> pending = [];

    private readonly record struct Entry(string Benchmark, string Args, string QueryId, long ExpectedQueries);

    /// <summary>
    /// Registers a query id to look up after the iteration. Call this from inside the measured region;
    /// it does no I/O.
    /// </summary>
    /// <param name="expectedQueries">
    /// How many <c>QueryFinish</c> rows to expect. For a batched insert this is the batch count, not 1
    /// — see <see cref="ServerMetrics.ReadAsync"/>.
    /// </param>
    public void Enqueue(string benchmark, string args, string queryId, long expectedQueries = 1) =>
        pending.Add(new Entry(benchmark, args, queryId, expectedQueries));

    /// <summary>
    /// Looks up and records every queued cost, then clears the queue. Call from
    /// <c>[IterationCleanup]</c>.
    /// </summary>
    /// <remarks>
    /// A lookup failure is reported and swallowed: losing the server-CPU column for one iteration is
    /// worth much less than losing the whole run, and <c>server_queries</c> in the CSV makes the gap
    /// visible to the analysis step rather than hiding it.
    /// </remarks>
    public void Drain(ClickHouseClient client)
    {
        foreach (var entry in pending)
        {
            try
            {
                var cost = ServerMetrics
                    .MeasureAsync(client, entry.QueryId, entry.ExpectedQueries)
                    .GetAwaiter()
                    .GetResult();

                SideMetrics.Record(entry.Benchmark, entry.Args, in cost);
            }
            catch (Exception ex)
            {
                SideMetrics.Record(entry.Benchmark, entry.Args, "server_lookup_failed", 1);
                Console.Error.WriteLine(
                    $"[blog-bench] server cost lookup failed for {entry.Benchmark} ({entry.Args}): {ex.Message}");
            }
        }

        pending.Clear();
    }
}
