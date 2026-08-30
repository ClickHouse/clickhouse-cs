using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>The payload shape a compression measurement runs on.</summary>
public enum CompressionShape
{
    /// <summary>One fixed-width column: little redundancy per byte, so the codec has least to work with.</summary>
    Narrow,

    /// <summary>An integer, a repeating short string and a float: what a real result set looks like.</summary>
    Wide,
}

/// <summary>
/// Prices the native block codecs — none, LZ4 and Zstandard — on the read and the insert path.
/// </summary>
/// <remarks>
/// <para>
/// <b>This measures the cost of compression, not its benefit.</b> Over loopback there is no bandwidth
/// to save, so the wall clock here is compression CPU plus framing and checksum, with none of the
/// transfer saving that motivates the feature. Do not read this table as a verdict on the default
/// codec. Deciding that needs either a bandwidth-limited path or a real network, and it is tracked
/// as T1a.
/// </para>
/// <para>
/// What this table is good for: catching a codec or framing change that makes the client's own work
/// slower or allocate more, which is a regression whatever the network looks like.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.Compression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpCompression
{
    private const string TableName = "test.benchmark_tcp_compression";

    private readonly Dictionary<string, ClickHouseTcpClient> clients = new();
    private ulong[] ids;
    private string[] cities;
    private double[] temperatures;

    [Params(200_000)]
    public int Count { get; set; }

    [Params("none", "lz4", "zstd")]
    public string Codec { get; set; }

    [ParamsAllValues]
    public CompressionShape Shape { get; set; }

    private ClickHouseTcpClient Client => clients[Codec];

    private string ReadSql => Shape == CompressionShape.Narrow
        ? $"SELECT toUInt64(number) AS id FROM system.numbers LIMIT {Count}"
        : "SELECT toUInt64(number) AS id, concat('city', toString(number % 100)) AS city, " +
          $"toFloat64(number) / 7 AS temperature FROM system.numbers LIMIT {Count}";

    private string InsertStatement => Shape == CompressionShape.Narrow
        ? $"INSERT INTO {TableName} (id) VALUES"
        : $"INSERT INTO {TableName} (id, city, temperature) VALUES";

    [GlobalSetup]
    public async Task Setup()
    {
        foreach (string codec in new[] { "none", "lz4", "zstd" })
        {
            clients[codec] = BenchmarkServer.CreateTcpClient(builder =>
            {
                builder.Compression = codec;
                return builder;
            });
        }

        await Client.ExecuteAsync("CREATE DATABASE IF NOT EXISTS test");
        await Client.ExecuteAsync(
            $"CREATE TABLE IF NOT EXISTS {TableName} (id UInt64, city String, temperature Float64) ENGINE Null");

        ids = new ulong[Count];
        cities = new string[Count];
        temperatures = new double[Count];
        for (int i = 0; i < Count; i++)
        {
            ids[i] = (ulong)i;
            cities[i] = "city" + (i % 100);
            temperatures[i] = i / 7.0;
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        foreach (var client in clients.Values)
        {
            client.Dispose();
        }

        clients.Clear();
    }

    [Benchmark]
    public async Task<long> Read()
    {
        long rows = 0;
        await foreach (Block block in Client.StreamAsync(ReadSql))
        {
            rows += block.RowCount;
        }

        return rows;
    }

    [Benchmark]
    public async Task Insert()
    {
        var columns = Shape == CompressionShape.Narrow
            ? new IColumn[] { ClickHouseTcpColumn.Create("id", ids) }
            : new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", ids),
                ClickHouseTcpColumn.Create("city", cities),
                ClickHouseTcpColumn.Create("temperature", temperatures),
            };

        await Client.InsertAsync(InsertStatement, columns);
    }
}
