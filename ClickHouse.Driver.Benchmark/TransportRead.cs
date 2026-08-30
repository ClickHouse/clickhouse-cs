using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

/// <summary>The column shape a cross-transport read is measured on.</summary>
public enum TransportReadShape
{
    /// <summary>One fixed-width column: the closest thing to raw decode throughput.</summary>
    NarrowUInt64,

    /// <summary>An integer, a short string and a float: the shape most result sets resemble.</summary>
    MixedThreeColumn,

    /// <summary>Three string columns, where variable-length decoding dominates.</summary>
    StringHeavy,
}

/// <summary>
/// Reads the same rows over both transports in one process, so the choice between them can be made
/// from numbers rather than from the protocol's reputation.
/// </summary>
/// <remarks>
/// <para>
/// Both clients run at their own defaults, which is what a caller gets: response compression is on
/// for HTTP and LZ4 framing is on for the native protocol. The codecs differ, so this table answers
/// "what do I get today", not "which codec is faster" — <see cref="TcpCompression"/> owns that axis.
/// </para>
/// <para>
/// Every arm consumes every value, because a transport that only decodes is not comparable with one
/// that also materializes. The per-row <c>switch</c> on <see cref="Shape"/> is identical in all three
/// arms, so it cannot bias the comparison.
/// </para>
/// <para>
/// This is characterization, not a regression net: both arms move together when the server or the
/// runner changes. The per-transport classes (<see cref="SelectColumn"/>,
/// <see cref="TcpSelectColumn"/>) are what a PR comparison reads.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.Cross)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TransportRead
{
    private ClickHouseConnection httpConnection;
    private ClickHouseTcpClient tcpClient;

    [Params(500_000)]
    public int Count { get; set; }

    [ParamsAllValues]
    public TransportReadShape Shape { get; set; }

    private string Sql => Shape switch
    {
        TransportReadShape.NarrowUInt64 =>
            $"SELECT toUInt64(number) AS id FROM system.numbers LIMIT {Count}",
        TransportReadShape.MixedThreeColumn =>
            "SELECT toUInt64(number) AS id, concat('city', toString(number % 100)) AS city, " +
            $"toFloat64(number) / 7 AS temperature FROM system.numbers LIMIT {Count}",
        _ =>
            "SELECT concat('city', toString(number % 100)) AS city, " +
            "concat('region', toString(number % 7)) AS region, " +
            $"toString(number) AS label FROM system.numbers LIMIT {Count}",
    };

    [GlobalSetup]
    public void Setup()
    {
        httpConnection = new ClickHouseConnection(BenchmarkServer.Http);
        tcpClient = BenchmarkServer.CreateTcpClient();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        httpConnection?.Dispose();
        tcpClient?.Dispose();
    }

    /// <summary>HTTP, through the ADO reader's typed accessor.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task<double> Http()
    {
        double total = 0;
        using var reader = await httpConnection.ExecuteReaderAsync(Sql);
        while (reader.Read())
        {
            switch (Shape)
            {
                case TransportReadShape.NarrowUInt64:
                    total += reader.GetFieldValue<ulong>(0);
                    break;
                case TransportReadShape.MixedThreeColumn:
                    total += (double)reader.GetFieldValue<ulong>(0)
                        + reader.GetFieldValue<string>(1).Length
                        + reader.GetFieldValue<double>(2);
                    break;
                default:
                    total += reader.GetFieldValue<string>(0).Length
                        + reader.GetFieldValue<string>(1).Length
                        + reader.GetFieldValue<string>(2).Length;
                    break;
            }
        }

        return total;
    }

    /// <summary>Native protocol, block tier: borrowed columnar spans.</summary>
    [Benchmark]
    public async Task<double> TcpBlocks()
    {
        double total = 0;
        await foreach (Block block in tcpClient.StreamAsync(Sql))
        {
            switch (Shape)
            {
                case TransportReadShape.NarrowUInt64:
                    foreach (ulong id in block.Column<ulong>(0).Values)
                    {
                        total += id;
                    }

                    break;

                case TransportReadShape.MixedThreeColumn:
                {
                    var ids = block.Column<ulong>(0).Values;
                    var cities = block.Column<string>(1).Values;
                    var temperatures = block.Column<double>(2).Values;
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        total += (double)ids[row] + cities[row].Length + temperatures[row];
                    }

                    break;
                }

                default:
                {
                    var first = block.Column<string>(0).Values;
                    var second = block.Column<string>(1).Values;
                    var third = block.Column<string>(2).Values;
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        total += first[row].Length + second[row].Length + third[row].Length;
                    }

                    break;
                }
            }
        }

        return total;
    }

    /// <summary>Native protocol, row tier: one <c>object[]</c> per row, every value boxed.</summary>
    [Benchmark]
    public async Task<double> TcpRows()
    {
        double total = 0;
        await foreach (object[] row in tcpClient.QueryAsync(Sql))
        {
            switch (Shape)
            {
                case TransportReadShape.NarrowUInt64:
                    total += (ulong)row[0];
                    break;
                case TransportReadShape.MixedThreeColumn:
                    total += (double)(ulong)row[0] + ((string)row[1]).Length + (double)row[2];
                    break;
                default:
                    total += ((string)row[0]).Length + ((string)row[1]).Length + ((string)row[2]).Length;
                    break;
            }
        }

        return total;
    }
}
