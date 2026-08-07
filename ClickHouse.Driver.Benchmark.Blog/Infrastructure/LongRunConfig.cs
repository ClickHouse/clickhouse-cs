using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using Perfolizer.Mathematics.OutlierDetection;

namespace ClickHouse.Driver.Benchmark.Blog.Infrastructure;

/// <summary>
/// The configuration every harness in this project uses: long sustained iterations, GC state left
/// alone between them, and Server/Workstation GC as a first-class paired arm rather than an
/// environment footnote.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is deliberately not <c>ComparisonConfig</c>.</b> That config drives the
/// <c>/benchmark-compare</c> workflow and the per-PR regression numbers; changing it would move
/// published PR figures. This one is free to override defaults that only matter for a multi-minute
/// steady-state run.
/// </para>
///
/// <para><b>Three BenchmarkDotNet defaults are overridden, all three for a reason:</b></para>
/// <list type="number">
///   <item>
///     <b><c>GcMode.Force</c> defaults to <c>true</c></b> — BenchmarkDotNet calls
///     <c>GC.Collect()</c> between every iteration, which resets exactly the LOH fragmentation and
///     promotion state a steady-state run exists to observe. Forced off here. Verified against
///     0.15.8 via <c>GcResolver.Instance</c>.
///   </item>
///   <item>
///     <b><c>GcMode.Server</c> defaults to <c>false</c></b> — i.e. Workstation GC, which is the wrong
///     mode for a server-side database driver and, notably, the mode every existing GC number in
///     every PR body in this release was measured under. Both modes run as paired jobs; see
///     <see cref="GcModes"/>.
///   </item>
///   <item>
///     <b><c>EventPipeProfiler</c> defaults to <c>performExtraBenchmarksRun: true</c></b> — the
///     attribute form re-runs the entire benchmark a second time just to collect the trace. For a
///     45-second workload that doubles wall clock, and worse, the summary table then comes from the
///     first run while the trace comes from the second: the pause distribution would describe a
///     different process than the throughput number next to it. When tracing is enabled here it is
///     same-run (<c>performExtraBenchmarksRun: false</c>), and it is opt-in so headline throughput
///     runs stay unperturbed.
///   </item>
/// </list>
///
/// <para><b>Environment variables</b></para>
/// <list type="table">
///   <item><term>BENCH_VERSIONS</term><description>Comma-separated driver versions to run as
///     separate arms, e.g. <c>1.0.0,1.2.0,1.3.0,1.4.0-local</c>. The first is the ratio baseline.
///     Unset (default) runs the working tree only. Any entry other than <c>source</c> becomes a
///     <c>PackageReference</c> build, so it can only use API that version shipped — see
///     <c>docs/API-CORRIDOR.md</c>.</description></item>
///   <item><term>NUGET_SOURCE</term><description>Extra restore source, for a locally packed
///     <c>.nupkg</c> such as <c>1.4.0-local</c>.</description></item>
///   <item><term>BENCH_GC_MODES</term><description><c>server</c>, <c>workstation</c>, or
///     <c>both</c> (default). <c>both</c> doubles the job count.</description></item>
///   <item><term>BENCH_GC_TRACE</term><description><c>1</c> to attach
///     <c>EventPipeProfiler(GcVerbose)</c>, which writes a <c>.nettrace</c> per benchmark for
///     <c>gc-report</c> to turn into pause/LOH CSVs. Off by default.</description></item>
///   <item><term>BENCH_GC_TRACE_PROFILE</term><description><c>verbose</c> (default) or
///     <c>collect</c>, the lighter variant, when GcVerbose traces get unwieldy.</description></item>
///   <item><term>BENCH_WARMUP / BENCH_ITERATIONS / BENCH_LAUNCHES / BENCH_PROFILE</term>
///     <description>See <see cref="BenchProfile"/>.</description></item>
/// </list>
/// </remarks>
public class LongRunConfig : ManualConfig
{
    /// <summary>Sentinel in <c>BENCH_VERSIONS</c> meaning "the working tree", i.e. no package override.</summary>
    public const string SourceArm = "source";

    public LongRunConfig()
    {
        AddDiagnoser(MemoryDiagnoser.Default);

        if (GcTraceEnabled)
        {
            // performExtraBenchmarksRun: false — see the class remarks. Without it the trace and the
            // summary table describe two different runs.
            AddDiagnoser(new EventPipeProfiler(GcTraceProfile, performExtraBenchmarksRun: false));
        }

        foreach (var job in BuildJobs())
            AddJob(job);

        SummaryStyle = SummaryStyle.Default.WithRatioStyle(RatioStyle.Percentage);

        // The Arguments column repeats the full MSBuild command line for every row of a multi-version
        // run, which pushes the actual numbers off the edge of any terminal. The Job column already
        // identifies the arm, and the arguments are in the run log.
        HideColumns(Column.Arguments);

        // The summary table's mean is not the interesting statistic for a steady-state run: the tail
        // is. BenchmarkDotNet also writes every individual measurement to *-measurements.csv, which is
        // what the pause CDF and the LOH-over-time charts are built from.
        AddColumn(StatisticColumn.P50, StatisticColumn.P95, StatisticColumn.Max);
    }

    private static bool GcTraceEnabled => BenchEnv.GetFlag("BENCH_GC_TRACE");

    private static EventPipeProfile GcTraceProfile =>
        string.Equals(Environment.GetEnvironmentVariable("BENCH_GC_TRACE_PROFILE")?.Trim(), "collect", StringComparison.OrdinalIgnoreCase)
            ? EventPipeProfile.GcCollect
            : EventPipeProfile.GcVerbose;

    /// <summary>
    /// The version arms. Unset means "working tree only", which is what a local iteration wants; the
    /// four-bar release chart sets all of <c>1.0.0,1.2.0,1.3.0,1.4.0-local</c>.
    /// </summary>
    private static IReadOnlyList<string> Versions
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("BENCH_VERSIONS");
            if (string.IsNullOrWhiteSpace(raw))
                return new[] { SourceArm };

            var parsed = raw
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            return parsed.Length == 0 ? new[] { SourceArm } : parsed;
        }
    }

    /// <summary>
    /// Server and Workstation GC as paired arms. Nearly every PR in this release quotes Gen0
    /// collections per 1k ops, and that metric is a direct function of the gen0 budget — which Server
    /// GC makes tens of MB per heap instead of L2-cache sized. The same real allocation win therefore
    /// reads as a very different-sounding claim in the two modes, so both get measured.
    /// </summary>
    private static IReadOnlyList<bool> GcModes
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("BENCH_GC_MODES")?.Trim();
            return raw?.ToLowerInvariant() switch
            {
                "server" => new[] { true },
                "workstation" or "wks" => new[] { false },
                _ => new[] { true, false },
            };
        }
    }

    private static IEnumerable<Job> BuildJobs()
    {
        var versions = Versions;
        var gcModes = GcModes;
        var nugetSource = Environment.GetEnvironmentVariable("NUGET_SOURCE");
        var first = true;

        foreach (var version in versions)
        {
            foreach (var serverGc in gcModes)
            {
                var job = Job.Default
                    // One workload invocation per iteration, no pilot stage, no overhead subtraction.
                    // The workload itself is what lasts 30-60 s; see BenchProfile.BurstSeconds.
                    .WithStrategy(RunStrategy.Monitoring)
                    .WithInvocationCount(1)
                    .WithUnrollFactor(1)
                    .WithWarmupCount(BenchProfile.Warmup)
                    .WithIterationCount(BenchProfile.Iterations)
                    .WithLaunchCount(BenchProfile.Launches)
                    .WithOutlierMode(OutlierMode.RemoveAll)
                    // The two overrides that matter. GcConcurrent is set explicitly only to record
                    // that background gen2 being on is a deliberate choice, not an oversight.
                    .WithGcForce(false)
                    .WithGcServer(serverGc)
                    .WithGcConcurrent(true)
                    .WithId(JobId(version, serverGc, versions.Count > 1, gcModes.Count > 1));

                if (version != SourceArm)
                {
                    var args = new List<string>
                    {
                        "/p:BenchmarkComparisonMode=true",
                        $"/p:ClickHouseDriverVersion={version}",
                    };

                    if (!string.IsNullOrWhiteSpace(nugetSource))
                        args.Add($"/p:RestoreAdditionalProjectSources={nugetSource}");

                    job = job.WithMsBuildArguments(args.ToArray());
                }

                // The oldest version is the baseline, so ratios read as "how much better did it get",
                // which is the direction the release narrative runs in.
                if (first)
                {
                    job = job.WithBaseline(true);
                    first = false;
                }

                yield return job;
            }
        }
    }

    /// <summary>
    /// Job id, used verbatim in the summary table and — critically — as a <i>directory name</i> for
    /// BenchmarkDotNet's auto-generated project. It must therefore contain no path separator: a job id
    /// of <c>source/wks</c> makes the generator write to a subdirectory that does not exist and the
    /// whole run dies with a DirectoryNotFoundException about a <c>.notcs</c> file.
    /// </summary>
    private static string JobId(string version, bool serverGc, bool manyVersions, bool manyGcModes)
    {
        var gc = serverGc ? "srv" : "wks";

        if (manyVersions && !manyGcModes)
            return Sanitize(version);
        if (manyGcModes && !manyVersions)
            return gc;

        return $"{Sanitize(version)}-{gc}";
    }

    /// <summary>Strips anything that cannot appear in a path segment.</summary>
    private static string Sanitize(string id)
    {
        Span<char> buffer = stackalloc char[id.Length];
        for (var i = 0; i < id.Length; i++)
            buffer[i] = id[i] is '/' or '\\' or ':' or '*' or '?' or '"' or '<' or '>' or '|' ? '_' : id[i];

        return new string(buffer);
    }
}
