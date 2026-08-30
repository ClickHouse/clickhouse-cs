using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Prices the two shapes an <c>Array(T)</c> insert accepts: the wire's flat elements plus offsets, and
/// one array per row.
/// </summary>
/// <remarks>
/// <para>
/// The dense shape is what a read hands back, so a read-transform-write pipeline can pass it straight
/// through. The jagged shape is what a caller building rows in memory has, and the client has to
/// flatten it: an array per row to walk and every element copied.
/// </para>
/// <para>
/// <see cref="ElementsPerRow"/> is the axis that decides how much that flattening costs, so both a
/// short and a long row are measured. The source data is built once in <see cref="Setup"/>; only the
/// column wrappers are built per operation, as a caller does.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpArrayWriteShape
{
    private const string TableName = "test.benchmark_tcp_array_write";
    private const string Statement = "INSERT INTO " + TableName + " (id, readings) VALUES";

    private ClickHouseTcpClient client;
    private ulong[] ids;
    private double[] flat;
    private int[] offsets;
    private double[][] jagged;

    [Params(100_000)]
    public int Count { get; set; }

    [Params(4, 32)]
    public int ElementsPerRow { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        client = BenchmarkServer.CreateTcpClient();
        await client.ExecuteAsync("CREATE DATABASE IF NOT EXISTS test");
        await client.ExecuteAsync(
            $"CREATE TABLE IF NOT EXISTS {TableName} (id UInt64, readings Array(Float64)) ENGINE Null");

        ids = new ulong[Count];
        flat = new double[(long)Count * ElementsPerRow];
        offsets = new int[Count + 1];
        jagged = new double[Count][];

        for (int row = 0; row < Count; row++)
        {
            ids[row] = (ulong)row;
            var elements = new double[ElementsPerRow];
            for (int i = 0; i < ElementsPerRow; i++)
            {
                double value = (row + i) / 7.0;
                elements[i] = value;
                flat[(row * ElementsPerRow) + i] = value;
            }

            jagged[row] = elements;
            offsets[row + 1] = (row + 1) * ElementsPerRow;
        }
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    /// <summary>The wire's own layout: flat elements plus offsets, nothing to flatten.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task Dense()
    {
        var columns = new IColumn[]
        {
            ClickHouseTcpColumn.Create("id", ids),
            ClickHouseTcpColumn.CreateArray(
                "readings",
                ClickHouseTcpColumn.Create("readings", flat),
                offsets),
        };

        await client.InsertAsync(Statement, columns);
    }

    /// <summary>One array per row, which the client walks and flattens.</summary>
    [Benchmark]
    public async Task Jagged()
    {
        var columns = new IColumn[]
        {
            ClickHouseTcpColumn.Create("id", ids),
            ClickHouseTcpColumn.Create("readings", jagged),
        };

        await client.InsertAsync(Statement, columns);
    }
}
