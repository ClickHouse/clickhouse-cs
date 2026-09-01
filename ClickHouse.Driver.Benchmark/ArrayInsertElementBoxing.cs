using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Measures the rank-1 <c>Array(...)</c> binary write path (issue #549), which now probes a typed
/// per-element writer before falling back to the boxing <c>IList</c> loop. The three shapes cover the
/// three outcomes of that probe, so a run on this branch against one on the base branch shows both the
/// allocation win on the matching shape and the cost of the probe on the shapes that do not take it:
/// <list type="bullet">
/// <item><description><see cref="TypedInt32"/> — <c>int[]</c> into <c>Array(Int32)</c>: the probe
/// matches, elements are written box-free.</description></item>
/// <item><description><see cref="CoercedInt64"/> — <c>long[]</c> into <c>Array(Int32)</c>: the probe
/// finds a writer for <c>long</c>, the column declines it (it only accepts <c>long</c> by coercion), and
/// the boxing loop runs. This is the worst case for the probe: its whole cost is paid for nothing.</description></item>
/// <item><description><see cref="StringFallback"/> — <c>string[]</c> into <c>Array(String)</c>: the
/// dictionary lookup misses and the boxing loop runs.</description></item>
/// </list>
/// Inserts into <c>Null</c>-engine tables to isolate client serialization.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class ArrayInsertElementBoxing
{
    private const string Int32Table = "test.benchmark_array_int32";
    private const string StringTable = "test.benchmark_array_string";

    private ClickHouseClient client;
    private List<object[]> int32Rows;
    private List<object[]> int64Rows;
    private List<object[]> stringRows;

    [Params(500)]
    public int Rows { get; set; }

    [Params(100)]
    public int Elements { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";
        client = new ClickHouseClient(connectionString);

        client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test").GetAwaiter().GetResult();
        client.ExecuteNonQueryAsync($"CREATE TABLE IF NOT EXISTS {Int32Table} (arr Array(Int32)) ENGINE Null").GetAwaiter().GetResult();
        client.ExecuteNonQueryAsync($"CREATE TABLE IF NOT EXISTS {StringTable} (arr Array(String)) ENGINE Null").GetAwaiter().GetResult();

        int32Rows = new List<object[]>(Rows);
        int64Rows = new List<object[]>(Rows);
        stringRows = new List<object[]>(Rows);
        for (var n = 0; n < Rows; n++)
        {
            var ints = new int[Elements];
            var longs = new long[Elements];
            var strings = new string[Elements];
            for (var i = 0; i < Elements; i++)
            {
                var v = (n * Elements) + i;
                ints[i] = v;
                longs[i] = v;
                strings[i] = v.ToString(CultureInfo.InvariantCulture);
            }

            int32Rows.Add(new object[] { ints });
            int64Rows.Add(new object[] { longs });
            stringRows.Add(new object[] { strings });
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        client?.Dispose();
    }

    [Benchmark]
    public async Task<long> TypedInt32() =>
        await client.InsertBinaryAsync(Int32Table, new[] { "arr" }, int32Rows);

    [Benchmark]
    public async Task<long> CoercedInt64() =>
        await client.InsertBinaryAsync(Int32Table, new[] { "arr" }, int64Rows);

    [Benchmark]
    public async Task<long> StringFallback() =>
        await client.InsertBinaryAsync(StringTable, new[] { "arr" }, stringRows);
}
