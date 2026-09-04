using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>The conversion a projected read is measured on.</summary>
public enum ProjectionShape
{
    /// <summary><c>DateTime64(3)</c>, stored as <see cref="long"/>, read as <see cref="DateTimeOffset"/>.</summary>
    DateTime64ToOffset,

    /// <summary><c>Enum8</c>, stored as <see cref="sbyte"/>, read as its label.</summary>
    Enum8ToLabel,

    /// <summary><c>Array(DateTime)</c>, read element-wise into a <see cref="DateTimeOffset"/> array per row.</summary>
    ArrayOfDateTimeToOffset,
}

/// <summary>
/// Prices <c>Block.ReadAs&lt;T&gt;</c> against reading the column's own storage type, so the cost of
/// asking for a convenient type is visible before it is paid per row.
/// </summary>
/// <remarks>
/// <para>
/// The native arm reads the stored type and converts nothing, which is the floor. The two projected
/// arms differ in one thing: the indexer runs the compiled reader per access, while <c>Values</c>
/// converts the whole column once into an array it allocates.
/// </para>
/// <para>
/// The array shape is the one where a projection cannot avoid allocating: the element conversion
/// builds a new array per row, so it prices the composite path rather than a widening.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpProjectedRead
{
    private ClickHouseTcpClient client;

    [Params(500_000)]
    public int Count { get; set; }

    [ParamsAllValues]
    public ProjectionShape Shape { get; set; }

    private string Sql => Shape switch
    {
        ProjectionShape.DateTime64ToOffset =>
            $"SELECT toDateTime64(number / 1000, 3, 'UTC') AS v FROM system.numbers LIMIT {Count}",
        ProjectionShape.Enum8ToLabel =>
            "SELECT CAST(number % 3 AS Enum8('red' = 0, 'green' = 1, 'blue' = 2)) AS v " +
            $"FROM system.numbers LIMIT {Count}",
        _ =>
            "SELECT [toDateTime(number, 'UTC'), toDateTime(number + 1, 'UTC')] AS v " +
            $"FROM system.numbers LIMIT {Count}",
    };

    [GlobalSetup]
    public void Setup() => client = BenchmarkServer.CreateTcpClient();

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    /// <summary>The stored type, borrowed: no conversion and no per-row call.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task<long> NativeSpan()
    {
        long total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            switch (Shape)
            {
                case ProjectionShape.DateTime64ToOffset:
                    foreach (long value in block.Column<long>(0).Values)
                    {
                        total += value;
                    }

                    break;

                case ProjectionShape.Enum8ToLabel:
                    foreach (sbyte value in block.Column<sbyte>(0).Values)
                    {
                        total += value;
                    }

                    break;

                default:
                    foreach (uint value in ((IArrayColumn<uint>)block[0]).InnerValues)
                    {
                        total += value;
                    }

                    break;
            }
        }

        return total;
    }

    /// <summary>The projected view's indexer: the compiled reader runs once per access.</summary>
    [Benchmark]
    public async Task<long> ProjectedIndexer()
    {
        long total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            switch (Shape)
            {
                case ProjectionShape.DateTime64ToOffset:
                {
                    IColumn<DateTimeOffset> values = block.ReadAs<DateTimeOffset>(0);
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        total += values[row].Ticks;
                    }

                    break;
                }

                case ProjectionShape.Enum8ToLabel:
                {
                    IColumn<string> labels = block.ReadAs<string>(0);
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        total += labels[row].Length;
                    }

                    break;
                }

                default:
                {
                    IColumn<DateTimeOffset[]> values = block.ReadAs<DateTimeOffset[]>(0);
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        foreach (DateTimeOffset value in values[row])
                        {
                            total += value.Ticks;
                        }
                    }

                    break;
                }
            }
        }

        return total;
    }

    /// <summary>The projected view's <c>Values</c>: one conversion pass into an array it allocates.</summary>
    [Benchmark]
    public async Task<long> ProjectedValues()
    {
        long total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            switch (Shape)
            {
                case ProjectionShape.DateTime64ToOffset:
                    foreach (DateTimeOffset value in block.ReadAs<DateTimeOffset>(0).Values)
                    {
                        total += value.Ticks;
                    }

                    break;

                case ProjectionShape.Enum8ToLabel:
                    foreach (string label in block.ReadAs<string>(0).Values)
                    {
                        total += label.Length;
                    }

                    break;

                default:
                    foreach (DateTimeOffset[] values in block.ReadAs<DateTimeOffset[]>(0).Values)
                    {
                        foreach (DateTimeOffset value in values)
                        {
                            total += value.Ticks;
                        }
                    }

                    break;
            }
        }

        return total;
    }
}
