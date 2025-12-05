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

        var job = Job.Default
            .WithStrategy(RunStrategy.Monitoring)
            .WithWarmupCount(3)
            .WithIterationCount(30)
            .WithLaunchCount(2)
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
}
