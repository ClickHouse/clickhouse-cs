using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Prices the native client's read tiers against one another on a single mixed shape, so the cost
/// of choosing a tier is visible rather than inferred.
/// </summary>
/// <remarks>
/// <para>
/// All five arms read the same three columns and the same rows; only the accessor differs. The
/// block-tier arms separate the borrowed span from the typed indexer and from the boxing
/// <c>GetValue</c>, because those are three different costs behind one tier.
/// </para>
/// <para>
/// <see cref="TcpSelectColumn"/> covers the decode with no materialization at all, which is the
/// floor every arm here pays.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpReadTiers
{
    private ClickHouseTcpClient client;

    [Params(500_000)]
    public int Count { get; set; }

    private string Sql =>
        "SELECT toUInt64(number) AS id, concat('city', toString(number % 100)) AS city, " +
        $"toFloat64(number) / 7 AS temperature FROM system.numbers LIMIT {Count}";

    [GlobalSetup]
    public void Setup() => client = BenchmarkServer.CreateTcpClient();

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    /// <summary>Borrowed columnar spans: no per-row call, no allocation per row.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task<double> BlockSpans()
    {
        double total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            var ids = block.Column<ulong>("id").Values;
            var cities = block.Column<string>("city").Values;
            var temperatures = block.Column<double>("temperature").Values;

            for (int row = 0; row < block.RowCount; row++)
            {
                total += (double)ids[row] + cities[row].Length + temperatures[row];
            }
        }

        return total;
    }

    /// <summary>The typed indexer: one interface call per value, still no boxing.</summary>
    [Benchmark]
    public async Task<double> BlockTypedIndexer()
    {
        double total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            IColumn<ulong> ids = block.Column<ulong>("id");
            IColumn<string> cities = block.Column<string>("city");
            IColumn<double> temperatures = block.Column<double>("temperature");

            for (int row = 0; row < block.RowCount; row++)
            {
                total += (double)ids[row] + cities[row].Length + temperatures[row];
            }
        }

        return total;
    }

    /// <summary>The untyped accessor: one boxed object per value.</summary>
    [Benchmark]
    public async Task<double> BlockBoxedGetValue()
    {
        double total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            IColumn ids = block[0];
            IColumn cities = block[1];
            IColumn temperatures = block[2];

            for (int row = 0; row < block.RowCount; row++)
            {
                total += (double)(ulong)ids.GetValue(row);
                total += ((string)cities.GetValue(row)).Length;
                total += (double)temperatures.GetValue(row);
            }
        }

        return total;
    }

    /// <summary>The row tier: one <c>object[]</c> per row, every value boxed.</summary>
    [Benchmark]
    public async Task<double> RowObjectArray()
    {
        double total = 0;
        await foreach (object[] row in client.QueryAsync(Sql))
        {
            total += (double)(ulong)row[0] + ((string)row[1]).Length + (double)row[2];
        }

        return total;
    }

    /// <summary>The POCO tier: one object per row, values assigned through compiled setters.</summary>
    [Benchmark]
    public async Task<double> Poco()
    {
        double total = 0;
        await foreach (Reading row in client.QueryAsync<Reading>(Sql))
        {
            total += (double)row.Id + row.City.Length + row.Temperature;
        }

        return total;
    }

    private sealed class Reading
    {
        public ulong Id { get; set; }

        public string City { get; set; }

        public double Temperature { get; set; }
    }
}
