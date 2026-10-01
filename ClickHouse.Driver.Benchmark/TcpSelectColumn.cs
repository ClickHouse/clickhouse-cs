using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// The native-protocol counterpart to <see cref="SelectColumn"/>: the same expressions at the same
/// row count, read as columnar blocks.
/// </summary>
/// <remarks>
/// <para>
/// Both classes drain the result the cheapest way their API allows, which is not the same amount of
/// work. HTTP's <c>Read()</c> decodes every column of the row into a reused <c>object[]</c>, so it
/// boxes each value; the block reader decodes every column into typed storage and boxes nothing. A
/// per-expression gap between the two classes therefore mixes the wire and decode difference with
/// that materialization difference.
/// </para>
/// <para>
/// <see cref="TransportRead"/> is the comparison whose arms consume every value on both sides, and
/// <see cref="TcpReadTiers"/> prices the native materialization tiers against each other.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpSelectColumn
{
    private ClickHouseTcpClient client;

    [Params(500000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup() => client = BenchmarkServer.CreateTcpClient();

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    [Benchmark]
    public Task<long> SelectInt32() => RunBlockBenchmark("toInt32(number)");

    [Benchmark]
    public Task<long> SelectUInt32() => RunBlockBenchmark("toUInt32(number)");

    [Benchmark]
    public Task<long> SelectInt64() => RunBlockBenchmark("toInt64(number)");

    [Benchmark]
    public Task<long> SelectUInt64() => RunBlockBenchmark("toUInt64(number)");

    [Benchmark]
    public Task<long> SelectFloat32() => RunBlockBenchmark("toFloat32(number)");

    [Benchmark]
    public Task<long> SelectFloat64() => RunBlockBenchmark("toFloat64(number)");

    [Benchmark]
    public Task<long> SelectDecimal64() => RunBlockBenchmark("toDecimal64(number,5)");

    [Benchmark]
    public Task<long> SelectDecimal128() => RunBlockBenchmark("toDecimal128(number,5)");

    [Benchmark]
    public Task<long> SelectDecimal256() => RunBlockBenchmark("toDecimal256(number,5)");

    [Benchmark]
    public Task<long> SelectDate() => RunBlockBenchmark("toDate(18942+number)");

    [Benchmark]
    public Task<long> SelectDate32() => RunBlockBenchmark("toDate32(18942+number)");

    [Benchmark]
    public Task<long> SelectDateTime() => RunBlockBenchmark("toDateTime(18942+number,'UTC')");

    [Benchmark]
    public Task<long> SelectString() => RunBlockBenchmark("concat('test',toString(number))");

    [Benchmark]
    public Task<long> SelectArray() => RunBlockBenchmark("array(1, number, 3)");

    [Benchmark]
    public Task<long> SelectNullableInt32() => RunBlockBenchmark("CAST(toInt32(number) AS Nullable(Int32))");

    [Benchmark]
    public Task<long> SelectTuple() => RunBlockBenchmark("tuple(number, toString(number))");

    private async Task<long> RunBlockBenchmark(string expression)
    {
        long rows = 0;
        await foreach (Block block in client.StreamAsync($"SELECT {expression} FROM system.numbers LIMIT {Count}"))
        {
            rows += block.RowCount;
        }

        return rows;
    }
}
