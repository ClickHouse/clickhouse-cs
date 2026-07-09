using System;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using Perfolizer.Mathematics.OutlierDetection;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Benchmark configuration that supports two modes:
/// 1. Local mode (default): No env vars needed, uses the project reference
/// 2. Comparison mode: When BASELINE_VERSION and PR_VERSION are set,
///    compares two NuGet package versions with percentage ratios
/// </summary>
public class ComparisonConfig : ManualConfig
{
    public ComparisonConfig()
    {
        var baselineVersion = Environment.GetEnvironmentVariable("BASELINE_VERSION");
        var prVersion = Environment.GetEnvironmentVariable("PR_VERSION");

        // Slow (network-bound) benchmarks can dial these down for a quick pass without changing the
        // defaults the PR-comparison regression runs depend on, e.g.:
        //   BENCH_WARMUP=1 BENCH_ITERATIONS=5 BENCH_LAUNCHES=1 dotnet run -c Release -- --filter ...
        var warmupCount = GetEnvInt("BENCH_WARMUP", 3);
        var iterationCount = GetEnvInt("BENCH_ITERATIONS", 30);
        var launchCount = GetEnvInt("BENCH_LAUNCHES", 2);

        var job = Job.Default
            .WithStrategy(RunStrategy.Monitoring)
            .WithWarmupCount(warmupCount)
            .WithIterationCount(iterationCount)
            .WithLaunchCount(launchCount)
            .WithOutlierMode(OutlierMode.RemoveAll);

        // Comparison mode: both baseline and PR versions are set
        if (!string.IsNullOrEmpty(baselineVersion) && !string.IsNullOrEmpty(prVersion))
        {
            var nugetSource = Environment.GetEnvironmentVariable("NUGET_SOURCE") ?? "";
            var sourceArg = !string.IsNullOrEmpty(nugetSource)
                ? $"/p:RestoreAdditionalProjectSources={nugetSource}"
                : "";

            AddJob(job
                .WithMsBuildArguments($"/p:ClickHouseDriverVersion={baselineVersion}", sourceArg)
                .WithId("baseline")
                .WithBaseline(true));

            AddJob(job
                .WithMsBuildArguments($"/p:ClickHouseDriverVersion={prVersion}", sourceArg)
                .WithId("pr"));

            SummaryStyle = SummaryStyle.Default
                .WithRatioStyle(RatioStyle.Percentage);

            HideColumns(Column.Arguments);
            AddColumn(StatisticColumn.P95);
        }
        // Local mode: use project reference (no NuGet override)
        else
        {
            AddJob(job.WithId("current"));
        }
    }

    // Reads a positive integer from an env var, falling back to the default when unset/invalid.
    // WarmupCount may legitimately be 0 (skip warmup entirely), so allow non-negative here.
    private static int GetEnvInt(string name, int fallback)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        return int.TryParse(raw, out var value) && value >= 0 ? value : fallback;
    }
}
