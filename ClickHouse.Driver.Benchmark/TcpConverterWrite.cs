using System;
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
/// Each invocation does what <c>InsertAsync</c> does for one block of a column that the caller builds with
/// <see cref="ClickHouseTcpColumn"/>: it resolves the codec of the target type, asks the codec whether it accepts the
/// column, writes the data block and flushes. The writer stays open between invocations, as the writer of a
/// connection does, and its stream discards the bytes.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpConverterWrite
{
    private static readonly NegotiatedProtocol Negotiated = new(NegotiatedProtocol.ClientTcpProtocolVersion);

    // The context of the server's sample block, which an insert resolves the target types with.
    private static readonly ResolveContext SchemaContext = new() { ServerTimezone = "UTC" };

    private string columnType;
    private IColumn column;
    private ClickHouseBinaryWriter writer;

    /// <summary>The number of rows in the block.</summary>
    [Params(100_000)]
    public int Rows { get; set; }

    /// <summary>The column to insert.</summary>
    [ParamsAllValues]
    public ConverterWriteShape Shape { get; set; }

    /// <summary>Builds the column once, and opens the writer.</summary>
    [GlobalSetup]
    public void Setup()
    {
        (columnType, column) = Shape switch
        {
            ConverterWriteShape.LowCardinalityStringHighRepeat => ("LowCardinality(String)", Strings(distinct: 100)),
            ConverterWriteShape.LowCardinalityStringLowRepeat => ("LowCardinality(String)", Strings(distinct: Rows)),
            ConverterWriteShape.LowCardinalityFixedStringBytesHighRepeat => ("LowCardinality(FixedString(16))", Keys(distinct: 100)),
            ConverterWriteShape.LowCardinalityFixedStringBytesLowRepeat => ("LowCardinality(FixedString(16))", Keys(distinct: Rows)),
            ConverterWriteShape.NullableDateTimeFromOffset => ("Nullable(DateTime('UTC'))", ClickHouseTcpColumn.Create(
                "value",
                Enumerable.Range(0, Rows).Select(i => i % 5 == 0 ? (DateTimeOffset?)null : DateTimeOffset.FromUnixTimeSeconds(1_700_000_000 + i)).ToArray())),
            _ => ("Array(String)", ClickHouseTcpColumn.Create(
                "value",
                Enumerable.Range(0, Rows).Select(i => Enumerable.Range(0, i % 5).Select(j => $"item-{i}-{j}").ToArray()).ToArray())),
        };

        writer = new ClickHouseBinaryWriter(Stream.Null);
    }

    /// <summary>Closes the writer.</summary>
    [GlobalCleanup]
    public void Cleanup() => writer?.Dispose();

    /// <summary>Plans the column, writes it as one data block and flushes.</summary>
    /// <returns>The number of bytes written.</returns>
    [Benchmark]
    public async Task<long> Insert()
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, SchemaContext);
        if (!codec.CanWrite(column))
        {
            throw new InvalidOperationException($"The codec of '{columnType}' does not accept the column.");
        }

        long before = writer.BytesWritten;
        InsertColumn[] plan = { new("value", columnType, codec, column) };
        await BlockWriter.WriteDataBlockAsync(writer, Negotiated, plan, start: 0, Rows, BlockWriter.DefaultFlushThresholdBytes, CancellationToken.None);
        await writer.FlushAsync(CancellationToken.None);
        return writer.BytesWritten - before;
    }

    private IColumn Strings(int distinct)
        => ClickHouseTcpColumn.Create("value", Enumerable.Range(0, Rows).Select(i => $"category-{i % distinct}").ToArray());

    // 16 ASCII bytes each, the width of the FixedString.
    private IColumn Keys(int distinct)
        => ClickHouseTcpColumn.Create("value", Enumerable.Range(0, Rows).Select(i => Encoding.ASCII.GetBytes($"key-{i % distinct:D12}")).ToArray());
}
