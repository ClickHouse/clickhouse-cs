using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Analysis;
using Microsoft.Diagnostics.Tracing.Analysis.GC;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;

namespace ClickHouse.Driver.Benchmark.Blog.Infrastructure;

/// <summary>
/// Turns the <c>.nettrace</c> files <c>EventPipeProfiler</c> writes into two CSVs: one row per garbage
/// collection, and one summary row per trace.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists.</b> <c>MemoryDiagnoser</c> reports exactly two things — allocated bytes per
/// operation, and Gen0/1/2 collection counts per 1k operations. It reports no pause durations, no LOH
/// size, no fragmentation and no percentage of time spent in GC. Those are the numbers the
/// steady-state story is made of, and BenchmarkDotNet does collect them (via
/// <c>EventPipeProfiler(GcVerbose)</c>) but does not surface them in the summary table. This reads
/// them back out.
/// </para>
/// <para>
/// <b>Fragmentation needs detailed GC info</b>, which comes from per-heap-history events. When a trace
/// lacks them every fragmentation figure would silently read 0.00 — indistinguishable from a
/// perfectly compacted heap. <c>has_detailed_gc_info</c> in the summary is therefore load-bearing:
/// check it before plotting anything fragmentation-shaped.
/// </para>
/// <para>Usage: <c>dotnet run -c Release -- gc-report [path-or-directory] [--out DIR]</c></para>
/// </remarks>
internal static class GcTraceReport
{
    private static readonly Gens[] Generations =
        [Gens.Gen0, Gens.Gen1, Gens.Gen2, Gens.GenLargeObj];

    public static int Run(string[] args)
    {
        var inputs = args.Where(a => !a.StartsWith("--", StringComparison.Ordinal)).ToArray();
        var outDir = ArgValue(args, "--out");
        var root = inputs.Length > 0 ? inputs[0] : Directory.GetCurrentDirectory();

        var traces = FindTraces(root);
        if (traces.Count == 0)
        {
            Console.Error.WriteLine(
                $"No .nettrace files under '{root}'. Traces are only written when BENCH_GC_TRACE=1 is set " +
                $"for the benchmark run; without it BenchmarkDotNet attaches no EventPipe profiler.");
            return 1;
        }

        var summaryRows = new List<string>();
        var failures = 0;

        foreach (var trace in traces)
        {
            var destination = outDir ?? Path.GetDirectoryName(trace) ?? ".";
            Directory.CreateDirectory(destination);

            try
            {
                summaryRows.AddRange(Convert(trace, destination));
                Console.WriteLine($"  ok   {Path.GetFileName(trace)}");
            }
            catch (Exception ex)
            {
                failures++;
                Console.Error.WriteLine($"  FAIL {Path.GetFileName(trace)}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        if (summaryRows.Count > 0)
        {
            var summaryPath = Path.Combine(outDir ?? root, "gc-summary.csv");
            Directory.CreateDirectory(Path.GetDirectoryName(summaryPath)!);
            File.WriteAllLines(summaryPath, new[] { SummaryHeader }.Concat(summaryRows), new UTF8Encoding(false));
            Console.WriteLine($"\nSummary: {summaryPath} ({summaryRows.Count} trace(s))");
        }

        return failures == 0 ? 0 : 2;
    }

    private static List<string> FindTraces(string root) =>
        File.Exists(root)
            ? [root]
            : Directory.Exists(root)
                ? Directory.GetFiles(root, "*.nettrace", SearchOption.AllDirectories).OrderBy(f => f).ToList()
                : [];

    /// <summary>Writes the per-GC CSV for one trace and returns its summary row(s), one per process.</summary>
    private static IEnumerable<string> Convert(string tracePath, string outDir)
    {
        var label = Path.GetFileNameWithoutExtension(tracePath);
        var eventsPath = Path.Combine(outDir, label + ".gc-events.csv");

        var summaries = new List<string>();

        using var writer = new StreamWriter(eventsPath, append: false, new UTF8Encoding(false));
        writer.WriteLine(EventsHeader);

        using var source = new EventPipeEventSource(tracePath);
        source.NeedLoadedDotNetRuntimes();
        source.Process();

        foreach (var process in source.Processes())
        {
            var runtime = process.LoadedDotNetRuntime();
            if (runtime is null)
                continue;

            var collections = runtime.GC.GCs;
            if (collections.Count == 0)
                continue;

            var stats = runtime.GC.Stats();

            foreach (var gc in collections)
                writer.WriteLine(EventRow(label, process.ProcessID, gc, stats.HasDetailedGCInfo));

            summaries.Add(SummaryRow(label, process.ProcessID, collections, stats, runtime.GC.GCSettings));
        }

        return summaries;
    }

    private const string EventsHeader =
        "trace,pid,gc_number,generation,type,reason,start_ms,pause_ms,suspend_ms,duration_ms," +
        "pause_pct_since_last_gc,heap_before_mb,heap_after_mb,heap_peak_mb,promoted_mb,alloced_since_last_gc_mb," +
        "gen0_after_mb,gen1_after_mb,gen2_after_mb,loh_after_mb," +
        "gen0_frag_mb,gen1_frag_mb,gen2_frag_mb,loh_frag_mb,loh_frag_pct," +
        "compacting,heap_count,has_detailed_gc_info";

    private static string EventRow(string label, int pid, TraceGC gc, bool detailed)
    {
        // Per-generation sizes and fragmentation come from per-heap histories. A trace without them
        // throws or returns 0 here, and a 0 would read as "no fragmentation" — so absent data is
        // written as an empty cell instead, which no plotting library will silently treat as zero.
        var sizes = Generations.Select(g => Safe(() => gc.GenSizeAfterMB(g), detailed)).ToArray();
        var frags = Generations.Select(g => Safe(() => gc.GenFragmentationMB(g), detailed)).ToArray();
        var lohFragPct = Safe(() => gc.GenFragmentationPercent(Gens.GenLargeObj), detailed);

        return string.Join(',', new[]
        {
            Csv(label),
            pid.ToString(CultureInfo.InvariantCulture),
            gc.Number.ToString(CultureInfo.InvariantCulture),
            gc.Generation.ToString(CultureInfo.InvariantCulture),
            gc.Type.ToString(),
            gc.Reason.ToString(),
            Num(gc.PauseStartRelativeMSec),
            Num(gc.PauseDurationMSec),
            Num(gc.SuspendDurationMSec),
            Num(gc.DurationMSec),
            Num(gc.PauseTimePercentageSinceLastGC),
            Num(gc.HeapSizeBeforeMB),
            Num(gc.HeapSizeAfterMB),
            Num(gc.HeapSizePeakMB),
            Num(gc.PromotedMB),
            Num(gc.AllocedSinceLastGCMB),
            sizes[0], sizes[1], sizes[2], sizes[3],
            frags[0], frags[1], frags[2], frags[3],
            lohFragPct,
            // IsNotCompacting() is the only direction TraceEvent exposes; invert it once, here.
            (!gc.IsNotCompacting()).ToString(CultureInfo.InvariantCulture),
            gc.HeapCount.ToString(CultureInfo.InvariantCulture),
            detailed.ToString(CultureInfo.InvariantCulture),
        });
    }

    private const string SummaryHeader =
        "trace,pid,gc_count,induced_count,gen0_count,gen1_count,gen2_count,gen2_blocking_count," +
        "process_duration_ms,total_pause_ms,pause_pct_of_elapsed," +
        "pause_mean_ms,pause_p50_ms,pause_p95_ms,pause_p99_ms,pause_max_ms," +
        "total_allocated_mb,total_promoted_mb,max_alloc_rate_mb_sec,max_heap_peak_mb," +
        "loh_after_max_mb,loh_frag_max_mb,server_gc_from_trace,heap_count,loh_threshold_bytes," +
        "gc_settings,has_detailed_gc_info";

    private static string SummaryRow(
        string label,
        int pid,
        IReadOnlyList<TraceGC> collections,
        GCStats stats,
        GCSettings settings)
    {
        var pauses = collections.Select(gc => gc.PauseDurationMSec).OrderBy(x => x).ToArray();
        var detailed = stats.HasDetailedGCInfo;

        // A blocking gen2 is the pause that actually hurts; background gen2 is concurrent and is the
        // reason a raw "gen2 count" reads worse than it is.
        var gen2Blocking = collections.Count(gc =>
            gc.Generation == 2 && gc.Type != GCType.BackgroundGC);

        var lohAfterMax = detailed
            ? collections.Select(gc => TrySafe(() => gc.GenSizeAfterMB(Gens.GenLargeObj))).Where(v => v.HasValue).Select(v => v!.Value).DefaultIfEmpty(0).Max()
            : (double?)null;
        var lohFragMax = detailed
            ? collections.Select(gc => TrySafe(() => gc.GenFragmentationMB(Gens.GenLargeObj))).Where(v => v.HasValue).Select(v => v!.Value).DefaultIfEmpty(0).Max()
            : (double?)null;

        return string.Join(',', new[]
        {
            Csv(label),
            pid.ToString(CultureInfo.InvariantCulture),
            stats.Count.ToString(CultureInfo.InvariantCulture),
            stats.NumInduced.ToString(CultureInfo.InvariantCulture),
            collections.Count(gc => gc.Generation == 0).ToString(CultureInfo.InvariantCulture),
            collections.Count(gc => gc.Generation == 1).ToString(CultureInfo.InvariantCulture),
            collections.Count(gc => gc.Generation == 2).ToString(CultureInfo.InvariantCulture),
            gen2Blocking.ToString(CultureInfo.InvariantCulture),
            Num(stats.ProcessDuration),
            Num(stats.TotalPauseTimeMSec),
            Num(stats.GetGCPauseTimePercentage()),
            Num(stats.MeanPauseDurationMSec),
            Num(Percentile(pauses, 0.50)),
            Num(Percentile(pauses, 0.95)),
            Num(Percentile(pauses, 0.99)),
            Num(stats.MaxPauseDurationMSec),
            Num(stats.TotalAllocatedMB),
            Num(stats.TotalPromotedMB),
            Num(stats.MaxAllocRateMBSec),
            Num(stats.MaxSizePeakMB),
            lohAfterMax.HasValue ? Num(lohAfterMax.Value) : string.Empty,
            lohFragMax.HasValue ? Num(lohFragMax.Value) : string.Empty,
            // IsServerGCUsed is a tri-state int: > 0 server, 0 workstation, -1 UNKNOWN. EventPipe
            // traces from BenchmarkDotNet's GcVerbose provider set -1, so writing `false` here — as an
            // `IsServerGCUsed > 0` test would — silently labels every Server GC run as Workstation.
            // Unknown is written as an empty cell, and the authoritative mode comes from joining
            // side-metrics.csv on pid.
            stats.IsServerGCUsed switch
            {
                > 0 => "True",
                0 => "False",
                _ => string.Empty,
            },
            stats.HeapCount.ToString(CultureInfo.InvariantCulture),
            settings is null ? string.Empty : settings.LOHThreshold.ToString(CultureInfo.InvariantCulture),
            settings is null ? string.Empty : Csv(settings.BitSettings.ToString()),
            detailed.ToString(CultureInfo.InvariantCulture),
        });
    }

    /// <summary>Nearest-rank percentile. Explicit so the tail numbers cannot drift with a library upgrade.</summary>
    private static double Percentile(double[] sorted, double quantile)
    {
        if (sorted.Length == 0)
            return 0d;

        var rank = (int)Math.Ceiling(quantile * sorted.Length) - 1;
        return sorted[Math.Clamp(rank, 0, sorted.Length - 1)];
    }

    private static string Safe(Func<double> read, bool detailed)
    {
        if (!detailed)
            return string.Empty;

        var value = TrySafe(read);
        return value.HasValue ? Num(value.Value) : string.Empty;
    }

    private static double? TrySafe(Func<double> read)
    {
        try
        {
            var value = read();
            return double.IsFinite(value) ? value : null;
        }
        catch (Exception)
        {
            // TraceEvent throws rather than returning a sentinel when the per-heap history a
            // generation-scoped reading needs is missing from the trace.
            return null;
        }
    }

    private static string Num(double value) =>
        value.ToString("G17", CultureInfo.InvariantCulture);

    private static string Csv(string value) =>
        value.Contains(',', StringComparison.Ordinal) ? '"' + value.Replace("\"", "\"\"", StringComparison.Ordinal) + '"' : value;

    private static string? ArgValue(string[] args, string name)
    {
        var index = Array.IndexOf(args, name);
        return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
    }
}
