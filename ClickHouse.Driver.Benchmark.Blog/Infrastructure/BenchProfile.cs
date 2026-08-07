using System;
using System.Globalization;

namespace ClickHouse.Driver.Benchmark.Blog.Infrastructure;

/// <summary>
/// One switch that scales the whole suite, so "does it run at all" and "produce publishable numbers"
/// are the same code path at two sizes.
/// </summary>
/// <remarks>
/// <para>
/// Set <c>BENCH_PROFILE=smoke</c> to shrink every axis at once — burst duration, row counts, payload
/// sizes, warmup, iterations, launches. A smoke pass exists to prove the harness wiring, the DDL, the
/// server-CPU join and the trace post-processing all work; its numbers are worthless and are labelled
/// as such in the results header.
/// </para>
/// <para>
/// Individual knobs still win over the profile, so a single axis can be widened without leaving smoke
/// mode: <c>BENCH_PROFILE=smoke BENCH_BURST_SECONDS=20 ...</c>.
/// </para>
/// </remarks>
internal static class BenchProfile
{
    /// <summary>True when running the fast wiring check rather than a measurement.</summary>
    public static bool IsSmoke =>
        string.Equals(
            Environment.GetEnvironmentVariable("BENCH_PROFILE")?.Trim(),
            "smoke",
            StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Length of one sustained burst, i.e. one BenchmarkDotNet <c>Monitoring</c> iteration in the
    /// overhead-bound family.
    /// </summary>
    /// <remarks>
    /// 45 s by default. The point of a long iteration is that LOH fragmentation and connection-pool
    /// degradation accumulate <i>inside</i> a single measurement — a 3 s iteration shows neither, and
    /// with BenchmarkDotNet's default <c>GcForce=true</c> the state would be collected away between
    /// iterations anyway (see <see cref="LongRunConfig"/>).
    /// </remarks>
    public static int BurstSeconds => BenchEnv.GetInt("BENCH_BURST_SECONDS", IsSmoke ? 3 : 45);

    /// <summary>Rows read/written by the content-bound family. 1M by default: the staged size of <c>hits</c>.</summary>
    public static int ContentRows => BenchEnv.GetInt("BENCH_CONTENT_ROWS", IsSmoke ? 20_000 : 1_000_000);

    /// <summary>Rows in the <c>types_wide</c> long-tail scan.</summary>
    public static int TypesWideRows => BenchEnv.GetInt("BENCH_TYPES_WIDE_ROWS", IsSmoke ? 10_000 : 200_000);

    /// <summary>Warmup iterations. One long iteration is enough to JIT and to fill the connection pool.</summary>
    public static int Warmup => BenchEnv.GetInt("BENCH_WARMUP", IsSmoke ? 0 : 1);

    /// <summary>
    /// Iterations per launch. Deliberately not 1: a single iteration leaves no variance estimate and
    /// no way to notice that one run was disturbed.
    /// </summary>
    public static int Iterations => BenchEnv.GetInt("BENCH_ITERATIONS", IsSmoke ? 1 : 5);

    /// <summary>Process launches, which is what separates run-to-run from iteration-to-iteration noise.</summary>
    public static int Launches => BenchEnv.GetInt("BENCH_LAUNCHES", IsSmoke ? 1 : 3);

    /// <summary>Human-readable one-liner for the results header.</summary>
    public static string Describe() => string.Create(
        CultureInfo.InvariantCulture,
        $"profile={(IsSmoke ? "smoke (NOT PUBLISHABLE)" : "full")} burst={BurstSeconds}s rows={ContentRows} warmup={Warmup} iters={Iterations} launches={Launches}");
}
