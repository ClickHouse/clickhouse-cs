using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Runs a fixed set of queries through one client serially and at several widths, so the pool's
/// throughput is measured rather than assumed.
/// </summary>
/// <remarks>
/// <para>
/// Both arms issue <see cref="Queries"/> queries whatever <see cref="Degree"/> is, so every row does
/// the same work and the ratio is the speedup at that width. The serial arm ignores
/// <see cref="Degree"/>, which makes its rows a consistency check: they should all report the same
/// time.
/// </para>
/// <para>
/// The pool is sized to <see cref="Degree"/>, so waiting for a connection is not part of the
/// measurement. The runner's core count caps the speedup well below <see cref="Degree"/> at the wider
/// settings, on the client and on a local server alike.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpPoolConcurrency
{
    private const string Sql = "SELECT sum(number) FROM numbers(2000000)";

    private ClickHouseTcpClient client;

    [Params(64)]
    public int Queries { get; set; }

    [Params(1, 8, 32)]
    public int Degree { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        client = BenchmarkServer.CreateTcpClient(builder =>
        {
            builder.MaxPoolSize = Degree;
            return builder;
        });

        await client.PingAsync();
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    /// <summary>One query at a time, reusing a single pooled connection.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task<object> Serial()
    {
        object last = null;
        for (int i = 0; i < Queries; i++)
        {
            last = await client.ExecuteScalarAsync(Sql);
        }

        return last;
    }

    /// <summary>The same queries with <see cref="Degree"/> of them in flight throughout.</summary>
    [Benchmark]
    public async Task Concurrent() =>
        await Parallel.ForEachAsync(
            Enumerable.Range(0, Queries),
            new ParallelOptions { MaxDegreeOfParallelism = Degree },
            async (_, token) => await client.ExecuteScalarAsync(Sql, cancellationToken: token));
}
