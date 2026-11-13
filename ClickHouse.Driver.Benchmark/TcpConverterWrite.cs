using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Benchmark;

/// <summary>The column that <see cref="TcpConverterWrite"/> inserts, and the CLR type it is written from.</summary>
public enum ConverterWriteShape
{
    /// <summary><c>LowCardinality(String)</c> from <c>string</c>: 100 distinct values.</summary>
    LowCardinalityStringHighRepeat,

    /// <summary><c>LowCardinality(String)</c> from <c>string</c>: every value distinct.</summary>
    LowCardinalityStringLowRepeat,

    /// <summary><c>LowCardinality(FixedString(16))</c> from <c>byte[]</c>: 100 distinct values.</summary>
    LowCardinalityFixedStringBytesHighRepeat,

    /// <summary><c>LowCardinality(FixedString(16))</c> from <c>byte[]</c>: every value distinct.</summary>
    LowCardinalityFixedStringBytesLowRepeat,

    /// <summary><c>Nullable(DateTime('UTC'))</c> from <c>DateTimeOffset?</c>: every fifth row NULL.</summary>
    NullableDateTimeFromOffset,

    /// <summary><c>Array(String)</c> from <c>string[]</c>: 0 to 4 elements in a row.</summary>
    ArrayStringFromText,
}

/// <summary>
/// Prices the writes that the converter layer (ClickHouse/integrations#801) changes, through the code that an insert
/// runs for one block: the plan of the column, then the block writer. In memory, with no server.
/// </summary>
/// <remarks>
/// <para>
/// The benchmark measures whichever implementation is behind those entry points, so a run on two commits compares
/// the two implementations. The shapes are the write shapes of the converter spike.
/// </para>
/// <para>
/// One operation does what <c>InsertAsync</c> does for one block of a column that the caller builds with
/// <see cref="ClickHouseTcpColumn"/>: it resolves the codec of the target type, asks the codec whether it accepts the
/// column, writes the data block and flushes. An invocation inserts <see cref="BlocksPerInvocation"/> different
/// columns of <see cref="Rows"/> rows, rows <c>[0, 100,000)</c> of the spike's data. The writer stays open between
/// invocations, as the writer of a connection does, and its stream discards the bytes.
/// </para>
/// <para>
/// The class runs with tiered compilation off (<see cref="TieredCompilationOffAttribute"/>), so the code does not
/// change during a run.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
[InvocationCount(1, 1)]
[WarmupCount(5)]
[TieredCompilationOff]
public class TcpConverterWrite
{
    /// <summary>The number of blocks that one invocation inserts; each block is one operation.</summary>
    public const int BlocksPerInvocation = 10;

    private static readonly NegotiatedProtocol Negotiated = new(NegotiatedProtocol.ClientTcpProtocolVersion);

    // The context of the server's sample block, which an insert resolves the target types with.
    private static readonly ResolveContext SchemaContext = new() { ServerTimezone = "UTC" };

    private readonly IColumn[] columns = new IColumn[BlocksPerInvocation];
    private string columnType;
    private ClickHouseBinaryWriter writer;

    /// <summary>The number of rows in one block.</summary>
    [Params(10_000)]
    public int Rows { get; set; }

    /// <summary>The column to insert.</summary>
    [ParamsAllValues]
    public ConverterWriteShape Shape { get; set; }

    /// <summary>Builds the columns once, and opens the writer.</summary>
    [GlobalSetup]
    public void Setup()
    {
        for (int b = 0; b < BlocksPerInvocation; b++)
        {
            (columnType, columns[b]) = Column(firstRow: b * Rows);
        }

        writer = new ClickHouseBinaryWriter(Stream.Null);
    }

    /// <summary>Closes the writer.</summary>
    [GlobalCleanup]
    public void Cleanup() => writer?.Dispose();

    /// <summary>For each column: plans it, writes it as one data block and flushes.</summary>
    /// <returns>The number of bytes written.</returns>
    [Benchmark(OperationsPerInvoke = BlocksPerInvocation)]
    public async Task<long> Insert()
    {
        long before = writer.BytesWritten;
        foreach (IColumn column in columns)
        {
            IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, SchemaContext);
            if (!codec.CanWrite(column))
            {
                throw new InvalidOperationException($"The codec of '{columnType}' does not accept the column.");
            }

            InsertColumn[] plan = { new("value", columnType, codec, column) };
            await BlockWriter.WriteDataBlockAsync(writer, Negotiated, plan, start: 0, Rows, BlockWriter.DefaultFlushThresholdBytes, CancellationToken.None);
            await writer.FlushAsync(CancellationToken.None);
        }

        return writer.BytesWritten - before;
    }

    // Rows [firstRow, firstRow + Rows) of the shape's data.
    private (string Type, IColumn Column) Column(int firstRow)
    {
        IEnumerable<int> rows = Enumerable.Range(firstRow, Rows);
        int all = BlocksPerInvocation * Rows;
        return Shape switch
        {
            ConverterWriteShape.LowCardinalityStringHighRepeat => ("LowCardinality(String)", Strings(rows, distinct: 100)),
            ConverterWriteShape.LowCardinalityStringLowRepeat => ("LowCardinality(String)", Strings(rows, distinct: all)),
            ConverterWriteShape.LowCardinalityFixedStringBytesHighRepeat => ("LowCardinality(FixedString(16))", Keys(rows, distinct: 100)),
            ConverterWriteShape.LowCardinalityFixedStringBytesLowRepeat => ("LowCardinality(FixedString(16))", Keys(rows, distinct: all)),
            ConverterWriteShape.NullableDateTimeFromOffset => ("Nullable(DateTime('UTC'))", ClickHouseTcpColumn.Create(
                "value",
                rows.Select(i => i % 5 == 0 ? (DateTimeOffset?)null : DateTimeOffset.FromUnixTimeSeconds(1_700_000_000 + i)).ToArray())),
            _ => ("Array(String)", ClickHouseTcpColumn.Create(
                "value",
                rows.Select(i => Enumerable.Range(0, i % 5).Select(j => $"item-{i}-{j}").ToArray()).ToArray())),
        };
    }

    private static IColumn Strings(IEnumerable<int> rows, int distinct)
        => ClickHouseTcpColumn.Create("value", rows.Select(i => $"category-{i % distinct}").ToArray());

    // 16 ASCII bytes each, the width of the FixedString.
    private static IColumn Keys(IEnumerable<int> rows, int distinct)
        => ClickHouseTcpColumn.Create("value", rows.Select(i => Encoding.ASCII.GetBytes($"key-{i % distinct:D12}")).ToArray());
}
