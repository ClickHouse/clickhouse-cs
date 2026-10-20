using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Prices the <c>QBit</c> codec through the client: an insert transposes each row vector into the
/// type's bit planes, and a read transposes the planes back when a row vector is materialized.
/// </summary>
/// <remarks>
/// <para>
/// 1024 elements is a typical embedding width. 100 elements is not a multiple of the 32-element blocks
/// the codec works in, so it also covers the partial block at the end of each row, and several rows fit
/// in the codec's staging buffer. <c>Int8</c> has 8 planes and a different kernel from <c>Float32</c>'s 32.
/// </para>
/// <para>
/// The insert goes to a <c>Null</c> table, so the server only parses the block. The read selects from a
/// <c>Memory</c> table filled once in <see cref="Setup"/>, and reads every row through the typed indexer,
/// which is where the client rebuilds the row vector.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpQBit
{
    private ClickHouseTcpClient client;
    private float[][] floats;
    private sbyte[][] bytes;

    [Params("Float32", "Int8")]
    public string Element { get; set; }

    [Params(100, 1024)]
    public int Dimension { get; set; }

    [Params(1024)]
    public int Count { get; set; }

    private string NullTable => $"test.benchmark_tcp_qbit_{Element}_{Dimension}_null";

    private string MemoryTable => $"test.benchmark_tcp_qbit_{Element}_{Dimension}_memory";

    [GlobalSetup]
    public async Task Setup()
    {
        client = BenchmarkServer.CreateTcpClient(builder =>
        {
            builder["set_allow_experimental_qbit_type"] = 1;
            return builder;
        });

        var random = new Random(42);
        floats = new float[Count][];
        bytes = new sbyte[Count][];
        for (int row = 0; row < Count; row++)
        {
            floats[row] = new float[Dimension];
            bytes[row] = new sbyte[Dimension];
            for (int i = 0; i < Dimension; i++)
            {
                floats[row][i] = (float)((random.NextDouble() * 2) - 1);
                bytes[row][i] = (sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue + 1);
            }
        }

        string type = $"QBit({Element}, {Dimension})";
        await client.ExecuteAsync("CREATE DATABASE IF NOT EXISTS test");
        await client.ExecuteAsync($"CREATE TABLE IF NOT EXISTS {NullTable} (v {type}) ENGINE Null");
        await client.ExecuteAsync($"CREATE TABLE IF NOT EXISTS {MemoryTable} (v {type}) ENGINE Memory");
        await client.ExecuteAsync($"TRUNCATE TABLE {MemoryTable}");
        await client.InsertAsync($"INSERT INTO {MemoryTable} (v) VALUES", new[] { Column() });
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    /// <summary>Row vectors in, bit planes on the wire.</summary>
    [Benchmark]
    public ValueTask Insert() => client.InsertAsync($"INSERT INTO {NullTable} (v) VALUES", new[] { Column() });

    /// <summary>Bit planes off the wire, one row vector rebuilt per row.</summary>
    [Benchmark]
    public async Task<double> Read()
    {
        double total = 0;
        await foreach (Block block in client.StreamAsync($"SELECT v FROM {MemoryTable}"))
        {
            if (Element == "Int8")
            {
                IColumn<sbyte[]> column = block.Column<sbyte[]>("v");
                for (int row = 0; row < block.RowCount; row++)
                {
                    total += column[row][0];
                }
            }
            else
            {
                IColumn<float[]> column = block.Column<float[]>("v");
                for (int row = 0; row < block.RowCount; row++)
                {
                    total += column[row][0];
                }
            }
        }

        return total;
    }

    private IColumn Column() => Element == "Int8"
        ? ClickHouseTcpColumn.Create("v", bytes)
        : ClickHouseTcpColumn.Create("v", floats);
}
