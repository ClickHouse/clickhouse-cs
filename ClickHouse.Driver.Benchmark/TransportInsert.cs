using System.Collections.Generic;
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
/// Two things that would otherwise dominate are turned off, because each lands on the transports
/// unequally and would report itself as a protocol difference:
/// </para>
/// <para>
/// <b>Server-side insert buffering.</b> Every arm sets <c>async_insert = 0</c>. A server with async
/// inserts on buffers any insert that carries a client data block and, with
/// <c>wait_for_async_insert</c>, holds the response until the buffer flushes — the adaptive wait
/// starts at <c>async_insert_busy_timeout_min_ms</c>, 50 ms by default. Measured on 26.6.1.1193,
/// that is 55 ms per HTTP insert against 1.9 ms with the setting off, and the native arms never paid
/// it.
/// </para>
/// <para>
/// <b>Request compression.</b> The two transports do not default to the same codec — HTTP to ZSTD,
/// the native client to LZ4 — and on this payload the codec costs more than the serialization the
/// arms exist to compare, so it hides the difference. Both sides run uncompressed here.
/// <see cref="TcpCompression"/> owns the codec axis, and over loopback there is no bandwidth to
/// save, so nothing of value is left out.
/// </para>
/// <para>
/// Each arm also sends its rows in one request: <c>InsertOptions.BatchSize</c> defaults to 100,000,
/// so this insert would otherwise go as fifty sequential requests.
/// <see cref="HttpRowsDefaultBatching"/> keeps the default for comparison, and measures about 2.6 ms
/// per extra request - the round trip, and little else once the server's buffering is off.
/// </para>
/// <para>
/// The target is an <c>ENGINE Null</c> table, so the server discards the rows and the measurement
/// stays on serialization plus the wire. The source data is built once in <see cref="Setup"/>. The
/// column names match the POCO's property names, because the HTTP binary insert maps a property to
/// the column of that name and ClickHouse column names are case-sensitive.
/// </para>
/// <para>
/// <see cref="Count"/> is 5,000,000 because at 500,000 every arm ran under 60 ms, where the ratios
/// carried a standard deviation of 0.16 to 0.76 and told a reader nothing. At this size they land at
/// 0.04 to 0.11 and hold across the tenfold change. It costs about 1 GB of source rows held for the
/// run, which is the reason to raise it no further.
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

    private static readonly IReadOnlyDictionary<string, string> TcpSettings =
        new Dictionary<string, string> { ["async_insert"] = "0" };

    private ClickHouseClient httpClient;
    private ClickHouseTcpClient tcpClient;
    private ulong[] ids;
    private string[] cities;
    private double[] temperatures;
    private object[][] rows;
    private Reading[] pocoRows;

    [Params(5_000_000)]
    public int Count { get; set; }

    // Compressor, not the connection string's Compression key: a binary insert does not consult that
    // setting (see ClickHouseClientSettings.UseCompression), so this is what turns the codec off here.
    private InsertOptions HttpOptions => new()
    {
        BatchSize = Count,
        Compressor = null,
        CustomSettings = new Dictionary<string, object> { ["async_insert"] = 0 },
    };

    private ClickHouseTcpInsertOptions TcpOptions => new() { Settings = TcpSettings };

    [GlobalSetup]
    public async Task Setup()
    {
        httpClient = new ClickHouseClient(BenchmarkServer.HttpUncompressed);
        httpClient.RegisterBinaryInsertType<Reading>();
        tcpClient = BenchmarkServer.CreateUncompressedTcpClient();

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
    public async Task<long> HttpRows() =>
        await httpClient.InsertBinaryAsync(TableName, ColumnNames, rows, HttpOptions);

    /// <summary>Native protocol, the same rows through its row tier.</summary>
    [Benchmark]
    public async Task TcpRows() => await tcpClient.InsertRowsAsync(Statement, rows, TcpOptions);

    /// <summary>HTTP, reading the values off the POCO's compiled property accessors.</summary>
    [Benchmark]
    public async Task<long> HttpPoco() =>
        await httpClient.InsertBinaryAsync(TableName, pocoRows, HttpOptions);

    /// <summary>Native protocol, the same POCOs through its row tier.</summary>
    [Benchmark]
    public async Task TcpPoco() => await tcpClient.InsertRowsAsync(Statement, pocoRows, TcpOptions);

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

        await tcpClient.InsertAsync(Statement, columns, TcpOptions);
    }

    /// <summary>
    /// The same rows at the default <c>BatchSize</c> of 100,000, so the table shows what the
    /// batching costs on top of the protocol.
    /// </summary>
    [Benchmark]
    public async Task<long> HttpRowsDefaultBatching() =>
        await httpClient.InsertBinaryAsync(
            TableName,
            ColumnNames,
            rows,
            new InsertOptions
            {
                Compressor = null,
                CustomSettings = new Dictionary<string, object> { ["async_insert"] = 0 },
            });

    public class Reading
    {
        public ulong Id { get; set; }

        public string City { get; set; }

        public double Temperature { get; set; }
    }
}
