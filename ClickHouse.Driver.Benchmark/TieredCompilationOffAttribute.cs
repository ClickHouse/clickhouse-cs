using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Runs the benchmarks of a class with tiered compilation off (<c>DOTNET_TieredCompilation=0</c>), in every job of
/// the class's config. The JIT then compiles each method once, fully optimized, on its first call.
/// </summary>
/// <remarks>
/// <para>
/// With tiered compilation on, the JIT compiles a method again at a higher tier after enough calls, and Dynamic PGO
/// adds an instrumented tier between the two. For a benchmark that runs a few hundred operations in a launch, that
/// change can come at any iteration, or not at all, and the final code can differ from one launch to the next. The
/// median of such a benchmark then moves by 20% or more between two runs of the same code.
/// </para>
/// <para>
/// With this attribute the code does not change during a run. It is optimized code without Dynamic PGO, so an effect
/// that only Dynamic PGO gives is not measured. Both sides of a comparison run the same way.
/// </para>
/// </remarks>
[AttributeUsage(AttributeTargets.Class)]
public sealed class TieredCompilationOffAttribute : JobMutatorConfigBaseAttribute
{
    public TieredCompilationOffAttribute()
        : base(Job.Default.WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredCompilation", "0")))
    {
    }
}
