using System;
using System.Linq;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

namespace ClickHouse.Driver.Benchmark;

internal class Program
{
    public static int Main(string[] args)
    {
        var uncategorized = FindUncategorizedBenchmarks();
        if (uncategorized.Length > 0)
        {
            Console.Error.WriteLine(
                "These benchmark classes carry no [BenchmarkCategory], so every CI run that filters with " +
                "--anyCategories skips them: " + string.Join(", ", uncategorized) +
                ". Tag each one with a BenchmarkCategories constant.");
            return 1;
        }

#if BENCHMARK_COMPARISON
        var methodBaselines = FindMethodBaselines();
        if (methodBaselines.Length > 0)
        {
            Console.Error.WriteLine(
                "Comparison mode makes the job the baseline, and BenchmarkDotNet lets only one of the two " +
                "win. These methods set Baseline = true anyway, which leaves the other rows' ratios mixing " +
                "the package difference with the method difference: " + string.Join(", ", methodBaselines) +
                ". Write [Benchmark(Baseline = BenchmarkModes.MethodBaseline)] instead.");
            return 1;
        }
#endif

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        return 0;
    }

    private static string[] FindUncategorizedBenchmarks() =>
        BenchmarkTypes()
            .Where(t => t.GetCustomAttribute<BenchmarkCategoryAttribute>() == null)
            .Select(t => t.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

#if BENCHMARK_COMPARISON
    private static string[] FindMethodBaselines() =>
        BenchmarkTypes()
            .SelectMany(t => t.GetMethods().Select(m => (Type: t, Method: m)))
            .Where(x => x.Method.GetCustomAttribute<BenchmarkAttribute>()?.Baseline == true)
            .Select(x => x.Type.Name + "." + x.Method.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
#endif

    private static Type[] BenchmarkTypes() =>
        typeof(Program).Assembly.GetTypes()
            .Where(t => t.GetMethods().Any(m => m.GetCustomAttribute<BenchmarkAttribute>() != null))
            .ToArray();
}
