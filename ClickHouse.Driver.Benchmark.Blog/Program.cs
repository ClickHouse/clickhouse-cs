using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;
using ClickHouse.Driver.Benchmark.Blog.Infrastructure;

namespace ClickHouse.Driver.Benchmark.Blog;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        // Verbs come before BenchmarkDotNet's own argument parsing; everything else is passed straight
        // through so --filter/--join/--artifacts behave exactly as in the other benchmark project.
        if (args.Length > 0)
        {
            switch (args[0])
            {
                case "gc-report":
                    return GcTraceReport.Run(args.Skip(1).ToArray());

                case "env":
                    return await PrintEnvironmentAsync();

                case "--help" or "-h" or "help" when args.Length == 1:
                    PrintUsage();
                    return 0;
            }
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        return 0;
    }

    /// <summary>
    /// Prints the confounder set for the current environment, in the same key/value shape the results
    /// header uses. Run it before a suite so the run is reproducible from its own output.
    /// </summary>
    private static async Task<int> PrintEnvironmentAsync()
    {
        Console.WriteLine($"# blog benchmark environment @ {DateTime.UtcNow:O}");
        Console.WriteLine($"bench_profile={BenchProfile.Describe()}");
        Console.WriteLine($"driver_version={SideMetrics.ResolvedDriverVersion}");
        Console.WriteLine($"side_metrics_csv={SideMetrics.File}");

        foreach (var (key, value) in await BenchEnv.DescribeAsync())
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"{key}={value}"));

        return 0;
    }

    private static void PrintUsage() => Console.WriteLine(
        """
        ClickHouse.Driver.Benchmark.Blog — long-running macro benchmarks for the release write-up.

        Usage:
          dotnet run -c Release -- [BenchmarkDotNet args]      Run benchmarks (--list flat to enumerate)
          dotnet run -c Release -- env                         Print the confounder set for this box
          dotnet run -c Release -- gc-report [DIR] [--out DIR] Convert .nettrace files to GC CSVs

        The two families (see docs/BENCHMARK-PLAN.md):
          --filter '*Overhead*'    Family O — many small queries, concurrent. Per-query fixed cost.
          --filter '*Hits*'        Family C — one big scan / one big insert. Per-row, per-byte cost.
          --filter '*Codec*'       The codec matrix, both directions, with server CPU.
          --filter '*TypesWide*'   The composite-type long tail.
          --filter '*ColdStart*'   First-query cost.

        Key environment variables (full list in README.md):
          CLICKHOUSE_CONNECTION    Default Host=localhost;Port=8124 (the dedicated bench container)
          BENCH_PROFILE=smoke      Shrink every axis; proves wiring, produces unpublishable numbers
          BENCH_VERSIONS           e.g. 1.0.0,1.2.0,1.3.0,1.4.0-local — one arm per version
          BENCH_GC_MODES           server | workstation | both (default both)
          BENCH_GC_TRACE=1         Attach EventPipeProfiler(GcVerbose) for gc-report
        """);
}
