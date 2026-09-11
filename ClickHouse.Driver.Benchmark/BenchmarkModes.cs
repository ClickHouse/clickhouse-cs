namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Compile-time switches that differ between a local run and a CI comparison run.
/// </summary>
public static class BenchmarkModes
{
    /// <summary>
    /// Whether a class that exists to compare two approaches should mark one of them as the
    /// baseline: <c>true</c> locally and in the nightly sweep, <c>false</c> in comparison mode.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use it in place of a literal: <c>[Benchmark(Baseline = BenchmarkModes.MethodBaseline)]</c>.
    /// </para>
    /// <para>
    /// BenchmarkDotNet accepts a baseline method or a baseline job, not both. A single job runs
    /// locally, so the method baseline is what gives those classes their point — POCO against
    /// <c>GetValue</c>, blit against boxing. Comparison mode instead runs two jobs, one per package
    /// version, and the question there is whether the PR moved each benchmark; a method baseline
    /// answers a different question and, worse, wins over the job, leaving every other row's ratio a
    /// mix of the two effects.
    /// </para>
    /// </remarks>
#if BENCHMARK_COMPARISON
    public const bool MethodBaseline = false;
#else
    public const bool MethodBaseline = true;
#endif
}
