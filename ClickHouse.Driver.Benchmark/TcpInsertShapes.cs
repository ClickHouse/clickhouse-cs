using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Prices the native client's three write tiers on one shape: columnar, <c>object[]</c> rows, and POCO rows.
/// </summary>
/// <remarks>
/// <para>
/// The source data is built once in <see cref="Setup"/> and shared by all three arms, so the table
/// reports the insert path and not the cost of generating rows. Column wrappers are still built per
/// operation because <c>ClickHouseTcpColumn.Create</c> takes the array over rather than copying it,
/// which is what a caller does.
/// </para>
/// <para>
/// The target is an <c>ENGINE Null</c> table, so the server discards the rows and the measurement
/// stays on the client's serialization plus the wire.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpInsertShapes
{
    private const string TableName = "test.benchmark_tcp_insert_shapes";
    private const string Statement = "INSERT INTO " + TableName + " (id, city, temperature) VALUES";

    private ClickHouseTcpClient client;
    private ulong[] ids;
    private string[] cities;
    private double[] temperatures;
    private object[][] objectRows;
    private Reading[] pocoRows;

    [Params(500_000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        client = BenchmarkServer.CreateTcpClient();
        await client.ExecuteAsync("CREATE DATABASE IF NOT EXISTS test");
        await client.ExecuteAsync(
            $"CREATE TABLE IF NOT EXISTS {TableName} (id UInt64, city String, temperature Float64) ENGINE Null");

        ids = new ulong[Count];
        cities = new string[Count];
        temperatures = new double[Count];
        objectRows = new object[Count][];
        pocoRows = new Reading[Count];

        for (int i = 0; i < Count; i++)
        {
            ids[i] = (ulong)i;
            cities[i] = "city" + (i % 100);
            temperatures[i] = i / 7.0;

            objectRows[i] = new object[] { ids[i], cities[i], temperatures[i] };
            pocoRows[i] = new Reading { Id = ids[i], City = cities[i], Temperature = temperatures[i] };
        }
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    /// <summary>Columnar: data already grouped by column, nothing transposed and nothing boxed.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task Columnar()
    {
        var columns = new IColumn[]
        {
            ClickHouseTcpColumn.Create("id", ids),
            ClickHouseTcpColumn.Create("city", cities),
            ClickHouseTcpColumn.Create("temperature", temperatures),
        };

        await client.InsertAsync(Statement, columns);
    }

    /// <summary>Row tier: the client transposes rows into columns, reading through boxed values.</summary>
    [Benchmark]
    public async Task RowObjectArray() => await client.InsertRowsAsync(Statement, objectRows);

    /// <summary>POCO tier: the same transpose, driven by compiled per-property accessors.</summary>
    [Benchmark]
    public async Task Poco() => await client.InsertRowsAsync(Statement, pocoRows);

    private sealed class Reading
    {
        public ulong Id { get; set; }

        public string City { get; set; }

        public double Temperature { get; set; }
    }
}
