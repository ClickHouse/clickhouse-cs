using System;
using System.Globalization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace ClickHouse.Driver.Benchmark.Blog.Families;

/// <summary>
/// What the <i>first</i> query costs: type-system construction, schema caches, JIT, connection setup.
/// </summary>
/// <remarks>
/// <para>
/// Every other harness in this project deliberately measures steady state — a client built in
/// <c>[GlobalSetup]</c>, a warmed connection pool, JIT already done. That is the right default, and it
/// hides something real: a short-lived process, a serverless invocation or a CLI tool pays the
/// one-time costs on every run and never reaches steady state at all.
/// </para>
/// <para>
/// <b>This is the one harness that must not reuse anything.</b> Each invocation builds a fresh client
/// and issues one query, so the measurement includes everything the steady-state numbers amortise
/// away. <c>MaxConnectionsPerServer</c>, DNS, TCP, and the driver's own static type registry all land
/// here.
/// </para>
/// <para>
/// <b>Reading it.</b> <see cref="FirstQuery"/> is the cold number. <see cref="SecondQuery"/> issues two
/// queries on the same fresh client and returns after the second: the difference between them is the
/// part of cold start that is genuinely one-time rather than per-query. And because the process itself
/// is warm after the first BenchmarkDotNet iteration — the CLR is JITted, the assemblies loaded —
/// these are "new client in a warm process", not "new process". Read the first iteration of the first
/// launch (in <c>*-measurements.csv</c>, <c>WarmupCount=0</c>) for the closest thing to a true cold
/// process, and say which one any published figure is.
/// </para>
/// </remarks>
[Config(typeof(LongRunConfig))]
public class ColdStartBenchmark
{
    private const string Name = nameof(ColdStartBenchmark);

    private readonly Consumer consumer = new();

    [IterationSetup]
    public void IterationSetup() => SideMetrics.NextIteration();

    /// <summary>One query on a brand-new client: the full cold cost.</summary>
    [Benchmark(Baseline = true)]
    public async Task<object> FirstQuery()
    {
        var cpu = CpuProbe.Start();

        using var client = BenchEnv.CreateClient();
        var result = await client.ExecuteScalarAsync("SELECT 1");

        Record(nameof(FirstQuery), cpu);
        return result;
    }

    /// <summary>
    /// Two queries on a brand-new client. Subtract <see cref="FirstQuery"/> to get the marginal cost of
    /// a second query, i.e. how much of cold start was genuinely one-time.
    /// </summary>
    [Benchmark]
    public async Task<object> SecondQuery()
    {
        var cpu = CpuProbe.Start();

        using var client = BenchEnv.CreateClient();
        consumer.Consume(await client.ExecuteScalarAsync("SELECT 1"));
        var result = await client.ExecuteScalarAsync("SELECT 1");

        Record(nameof(SecondQuery), cpu);
        return result;
    }

    /// <summary>
    /// A fresh client's first read through the reader path, which touches the type registry and the
    /// column-slot machinery that <see cref="FirstQuery"/>'s scalar path does not.
    /// </summary>
    [Benchmark]
    public async Task<long> FirstReaderQuery()
    {
        var cpu = CpuProbe.Start();
        long rows = 0;

        using var client = BenchEnv.CreateClient();
        using (var reader = await client.ExecuteReaderAsync(
            "SELECT number, toString(number), toDateTime(number, 'UTC') FROM system.numbers LIMIT 100"))
        {
            while (reader.Read())
            {
                consumer.Consume(reader.GetValue(0));
                rows++;
            }
        }

        Record(nameof(FirstReaderQuery), cpu);
        return rows;
    }

    private void Record(string arm, CpuProbe cpu)
    {
        var args = string.Create(CultureInfo.InvariantCulture, $"arm={arm}");

        SideMetrics.Record(Name, args, "client_cpu_us", cpu.TotalMicroseconds);
        SideMetrics.Record(Name, args, "elapsed_us", cpu.ElapsedMicroseconds);
    }
}
