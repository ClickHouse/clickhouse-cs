using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>The composite type a read is measured on.</summary>
public enum CompositeShape
{
    /// <summary><c>Array(Float64)</c>: flattened elements plus offsets.</summary>
    Array,

    /// <summary><c>Map(String, Int64)</c>: a key column, a value column and offsets.</summary>
    Map,

    /// <summary><c>Nullable(Float64)</c>: an inner column plus a null map.</summary>
    Nullable,

    /// <summary><c>LowCardinality(String)</c>: a dictionary plus integer keys.</summary>
    LowCardinality,

    /// <summary><c>Tuple(UInt64, String)</c>: one column per field.</summary>
    Tuple,
}

/// <summary>
/// Prices the four ways to read a composite column, which do not rank the same for every shape.
/// </summary>
/// <remarks>
/// <para>
/// Every composite stores its rows as flat columns plus an index, and the arms differ in how far from
/// that storage they read: the composite interface walks it directly, the typed indexer converts one
/// row per access, <c>Values</c> converts the whole column into a cache first, and the row tier boxes
/// on top of that.
/// </para>
/// <para>
/// Which arms differ depends on the shape. A row of <see cref="CompositeShape.Array"/> or
/// <see cref="CompositeShape.Map"/> is itself a collection, so reading row values builds one array per
/// row however it is reached. A row of <see cref="CompositeShape.Nullable"/>,
/// <see cref="CompositeShape.LowCardinality"/> or <see cref="CompositeShape.Tuple"/> is a single value
/// the indexer reads straight from the storage, so for those three the table separates the indexer's
/// per-access cost from what <c>Values</c> pays to build its cache first.
/// </para>
/// <para>
/// Each arm does the same arithmetic per element, so the difference is the access path and not the
/// work. Where a shape holds strings, the borrowed arm takes a length from
/// <see cref="IStringColumn.Offsets"/> and the others take <c>string.Length</c>: the values here are
/// ASCII, so the two counts agree. <see cref="TcpStringRead"/> prices that difference on its own.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpCompositeRead
{
    private ClickHouseTcpClient client;

    [Params(500_000)]
    public int Count { get; set; }

    [ParamsAllValues]
    public CompositeShape Shape { get; set; }

    private string Sql => Shape switch
    {
        CompositeShape.Array =>
            $"SELECT [toFloat64(number), toFloat64(number + 1)] AS v FROM system.numbers LIMIT {Count}",
        CompositeShape.Map =>
            "SELECT map('floor', toInt64(number % 5), 'zone', toInt64(number % 7)) AS v " +
            $"FROM system.numbers LIMIT {Count}",
        CompositeShape.Nullable =>
            $"SELECT if(number % 4 = 0, NULL, toFloat64(number)) AS v FROM system.numbers LIMIT {Count}",
        CompositeShape.LowCardinality =>
            "SELECT toLowCardinality(concat('city', toString(number % 100))) AS v " +
            $"FROM system.numbers LIMIT {Count}",
        _ =>
            "SELECT (toUInt64(number), concat('c', toString(number % 10))) AS v " +
            $"FROM system.numbers LIMIT {Count}",
    };

    [GlobalSetup]
    public void Setup() => client = BenchmarkServer.CreateTcpClient();

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    /// <summary>The composite interface: the flattened storage, borrowed, with no row value built.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task<double> BorrowedStorage()
    {
        double total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            switch (Shape)
            {
                case CompositeShape.Array:
                {
                    var column = (IArrayColumn<double>)block[0];
                    foreach (double value in column.InnerValues)
                    {
                        total += value;
                    }

                    break;
                }

                case CompositeShape.Map:
                {
                    var column = (IMapColumn<string, long>)block[0];
                    var keyOffsets = ((IStringColumn)column.KeyColumn).Offsets;
                    var values = column.ValueColumn.Values;
                    for (int i = 0; i < values.Length; i++)
                    {
                        total += (keyOffsets[i + 1] - keyOffsets[i]) + values[i];
                    }

                    break;
                }

                case CompositeShape.Nullable:
                {
                    var column = (INullableColumn<double>)block[0];
                    var nullMap = column.NullMap;
                    var values = column.Inner.Values;
                    for (int row = 0; row < nullMap.Length; row++)
                    {
                        total += nullMap[row] == 0 ? values[row] : 0;
                    }

                    break;
                }

                case CompositeShape.LowCardinality:
                {
                    var column = (ILowCardinalityColumn<string>)block[0];
                    var dictionary = column.Dictionary.Values;
                    foreach (int key in column.Keys)
                    {
                        total += dictionary[key].Length;
                    }

                    break;
                }

                default:
                {
                    var column = (ITupleColumn)block[0];
                    var ids = ((IColumn<ulong>)column.Children[0]).Values;
                    var labelOffsets = ((IStringColumn)column.Children[1]).Offsets;
                    for (int row = 0; row < ids.Length; row++)
                    {
                        total += (double)ids[row] + (labelOffsets[row + 1] - labelOffsets[row]);
                    }

                    break;
                }
            }
        }

        return total;
    }

    /// <summary>The typed indexer: one row value per access, read from the storage.</summary>
    [Benchmark]
    public async Task<double> TypedIndexer()
    {
        double total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            switch (Shape)
            {
                case CompositeShape.Array:
                {
                    IColumn<double[]> column = block.Column<double[]>(0);
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        foreach (double value in column[row])
                        {
                            total += value;
                        }
                    }

                    break;
                }

                case CompositeShape.Map:
                {
                    IColumn<KeyValuePair<string, long>[]> column =
                        block.Column<KeyValuePair<string, long>[]>(0);
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        foreach (var pair in column[row])
                        {
                            total += pair.Key.Length + pair.Value;
                        }
                    }

                    break;
                }

                case CompositeShape.Nullable:
                {
                    IColumn<double?> column = block.Column<double?>(0);
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        total += column[row] ?? 0;
                    }

                    break;
                }

                case CompositeShape.LowCardinality:
                {
                    IColumn<string> column = block.Column<string>(0);
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        total += column[row].Length;
                    }

                    break;
                }

                default:
                {
                    IColumn<(ulong Id, string Label)> column = block.Column<(ulong, string)>(0);
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        var value = column[row];
                        total += (double)value.Id + value.Label.Length;
                    }

                    break;
                }
            }
        }

        return total;
    }

    /// <summary><c>Values</c>: the whole column converted into a cache before the first read.</summary>
    [Benchmark]
    public async Task<double> TypedValues()
    {
        double total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            switch (Shape)
            {
                case CompositeShape.Array:
                    foreach (double[] values in block.Column<double[]>(0).Values)
                    {
                        foreach (double value in values)
                        {
                            total += value;
                        }
                    }

                    break;

                case CompositeShape.Map:
                    foreach (var pairs in block.Column<KeyValuePair<string, long>[]>(0).Values)
                    {
                        foreach (var pair in pairs)
                        {
                            total += pair.Key.Length + pair.Value;
                        }
                    }

                    break;

                case CompositeShape.Nullable:
                    foreach (double? value in block.Column<double?>(0).Values)
                    {
                        total += value ?? 0;
                    }

                    break;

                case CompositeShape.LowCardinality:
                    foreach (string value in block.Column<string>(0).Values)
                    {
                        total += value.Length;
                    }

                    break;

                default:
                    foreach (var value in block.Column<(ulong Id, string Label)>(0).Values)
                    {
                        total += (double)value.Id + value.Label.Length;
                    }

                    break;
            }
        }

        return total;
    }

    /// <summary>The row tier: the same values arriving boxed in an <c>object[]</c>.</summary>
    [Benchmark]
    public async Task<double> RowTier()
    {
        double total = 0;
        await foreach (object[] row in client.QueryAsync(Sql))
        {
            switch (Shape)
            {
                case CompositeShape.Array:
                    foreach (double value in (double[])row[0])
                    {
                        total += value;
                    }

                    break;

                case CompositeShape.Map:
                    foreach (var pair in (KeyValuePair<string, long>[])row[0])
                    {
                        total += pair.Key.Length + pair.Value;
                    }

                    break;

                case CompositeShape.Nullable:
                    total += row[0] is null ? 0 : (double)row[0];
                    break;

                case CompositeShape.LowCardinality:
                    total += ((string)row[0]).Length;
                    break;

                default:
                {
                    var value = ((ulong Id, string Label))row[0];
                    total += (double)value.Id + value.Label.Length;
                    break;
                }
            }
        }

        return total;
    }
}
