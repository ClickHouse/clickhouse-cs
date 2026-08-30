using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Inserts the same rows over both transports, pairing each shape a caller can hand the client with
/// its counterpart on the other transport.
/// </summary>
/// <remarks>
/// <para>
/// Two pairs are directly comparable: the <c>object[]</c> rows, and the POCO rows. The columnar arm
/// is the native protocol's own shape, which HTTP has no counterpart for; it is here because it is
/// what a caller who can group data by column should reach for.
/// </para>
/// <para>
/// The target is an <c>ENGINE Null</c> table, so the server discards the rows and the measurement
/// stays on serialization plus the wire. The source data is built once in <see cref="Setup"/>. The
/// column names match the POCO's property names, because the HTTP binary insert maps a property to
/// the column of that name and ClickHouse column names are case-sensitive.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.Cross)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TransportInsert
{
    private const string TableName = "test.benchmark_transport_insert";
    private const string Statement = "INSERT INTO " + TableName + " (Id, City, Temperature) VALUES";

    private static readonly string[] ColumnNames = { "Id", "City", "Temperature" };

    private ClickHouseClient httpClient;
    private ClickHouseTcpClient tcpClient;
    private ulong[] ids;
    private string[] cities;
    private double[] temperatures;
    private object[][] rows;
    private Reading[] pocoRows;

    [Params(500_000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        httpClient = new ClickHouseClient(BenchmarkServer.Http);
        httpClient.RegisterBinaryInsertType<Reading>();
        tcpClient = BenchmarkServer.CreateTcpClient();

        await httpClient.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test");

        // Dropped rather than created only if absent: the name is fixed, so a schema change here
        // would otherwise keep using whatever an earlier revision left behind.
        await httpClient.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {TableName}");
        await httpClient.ExecuteNonQueryAsync(
            $"CREATE TABLE {TableName} (Id UInt64, City String, Temperature Float64) ENGINE Null");

        ids = new ulong[Count];
        cities = new string[Count];
        temperatures = new double[Count];
        rows = new object[Count][];
        pocoRows = new Reading[Count];

        for (int i = 0; i < Count; i++)
        {
            ids[i] = (ulong)i;
            cities[i] = "city" + (i % 100);
            temperatures[i] = i / 7.0;
            rows[i] = new object[] { ids[i], cities[i], temperatures[i] };
            pocoRows[i] = new Reading { Id = ids[i], City = cities[i], Temperature = temperatures[i] };
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        httpClient?.Dispose();
        tcpClient?.Dispose();
    }

    /// <summary>HTTP, through the binary insert path: one <c>object[]</c> per row.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task<long> HttpRows() => await httpClient.InsertBinaryAsync(TableName, ColumnNames, rows);

    /// <summary>
    /// The same rows in one request. <c>InsertOptions.BatchSize</c> defaults to 100,000, so
    /// <see cref="HttpRows"/> sends five sequential requests where the native arms send one block.
    /// This arm separates that batching from the protocol.
    /// </summary>
    [Benchmark]
    public async Task<long> HttpRowsOneBatch() =>
        await httpClient.InsertBinaryAsync(
            TableName, ColumnNames, rows, new InsertOptions { BatchSize = Count });

    /// <summary>Native protocol, the same rows through its row tier.</summary>
    [Benchmark]
    public async Task TcpRows() => await tcpClient.InsertRowsAsync(Statement, rows);

    /// <summary>HTTP, reading the values off the POCO's compiled property accessors.</summary>
    [Benchmark]
    public async Task<long> HttpPoco() => await httpClient.InsertBinaryAsync(TableName, pocoRows);

    /// <summary>The same POCOs in one request, for the same reason as <see cref="HttpRowsOneBatch"/>.</summary>
    [Benchmark]
    public async Task<long> HttpPocoOneBatch() =>
        await httpClient.InsertBinaryAsync(TableName, pocoRows, new InsertOptions { BatchSize = Count });

    /// <summary>Native protocol, the same POCOs through its row tier.</summary>
    [Benchmark]
    public async Task TcpPoco() => await tcpClient.InsertRowsAsync(Statement, pocoRows);

    /// <summary>Native protocol, columnar: nothing transposed and nothing boxed.</summary>
    [Benchmark]
    public async Task TcpColumnar()
    {
        var columns = new IColumn[]
        {
            ClickHouseTcpColumn.Create("Id", ids),
            ClickHouseTcpColumn.Create("City", cities),
            ClickHouseTcpColumn.Create("Temperature", temperatures),
        };

        await tcpClient.InsertAsync(Statement, columns);
    }

    public class Reading
    {
        public ulong Id { get; set; }

        public string City { get; set; }

        public double Temperature { get; set; }
    }
}
