using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Benchmark;

/// <summary>The column that <see cref="TcpConverterWriteRoutes"/> inserts, and how the caller builds it.</summary>
public enum ConverterRouteShape
{
    /// <summary>
    /// <c>Dynamic</c> from <c>object</c>: eight kinds of value in turn (Int32, String of 100 distinct values, Float64, NULL,
    /// Int64, an Int32 array of 0 to 3 elements, a tuple of Int32 and String, a DateTime).
    /// </summary>
    DynamicMixedKinds,

    /// <summary>
    /// <c>Array(LowCardinality(String))</c> from <see cref="ClickHouseTcpColumn.CreateArray{TElement}"/> over a column of
    /// strings: 0 to 4 elements in a row, 100 distinct values.
    /// </summary>
    CreateArrayLowCardinalityString,

    /// <summary>
    /// <c>Array(Nullable(Int32))</c> from <see cref="ClickHouseTcpColumn.CreateArray{TElement}"/> over a column of
    /// <c>int?</c>: 0 to 4 elements in a row, every fifth element NULL.
    /// </summary>
    CreateArrayNullableInt32,

    /// <summary><c>LowCardinality(UUID)</c> from <see cref="Guid"/>: 100 distinct values.</summary>
    LowCardinalityUuidHighRepeat,

    /// <summary><c>LowCardinality(UUID)</c> from <see cref="Guid"/>: every value distinct.</summary>
    LowCardinalityUuidLowRepeat,

    /// <summary><c>LowCardinality(UInt32)</c> from <c>uint</c>: 100 distinct values.</summary>
    LowCardinalityUInt32HighRepeat,
}

/// <summary>
/// Prices the writes of <c>Dynamic</c>, of the dense arrays that <see cref="ClickHouseTcpColumn.CreateArray{TElement}"/>
/// builds, and of LowCardinality over fixed-width leaves, through the code that an insert runs for one block: the plan of
/// the column, then the block writer. In memory, with no server.
/// </summary>
/// <remarks>
/// <para>
/// The benchmark measures whichever implementation is behind those entry points, so a run on two commits compares the
/// two implementations.
/// </para>
/// <para>
/// One operation does what <c>InsertAsync</c> does for one block of a column that the caller builds: it resolves the
/// codec of the target type, plans the write of the column (<see cref="InsertColumnWrite.For"/>), writes the data block
/// and flushes. An invocation inserts <see cref="BlocksPerInvocation"/> different columns of <see cref="Rows"/> rows. The
/// writer stays open between invocations, as the writer of a connection does, and its stream discards the bytes.
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
public class TcpConverterWriteRoutes
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
    public ConverterRouteShape Shape { get; set; }

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
            InsertColumnWrite write = InsertColumnWrite.For(codec, column, columnType, SchemaContext, ColumnCodecRegistry.Default.Converters)
                ?? throw new InvalidOperationException($"The insert plan of '{columnType}' does not accept the column.");

            InsertColumn[] plan = { new("value", columnType, codec, column, write) };
            await BlockWriter.WriteDataBlockAsync(writer, Negotiated, plan, start: 0, Rows, BlockWriter.DefaultFlushThresholdBytes, CancellationToken.None);
            await writer.FlushAsync(CancellationToken.None);
        }

        return writer.BytesWritten - before;
    }

    // Rows [firstRow, firstRow + Rows) of the shape's data.
    private (string Type, IColumn Column) Column(int firstRow)
    {
        IEnumerable<int> rows = Enumerable.Range(firstRow, Rows);
        return Shape switch
        {
            ConverterRouteShape.DynamicMixedKinds => ("Dynamic", ClickHouseTcpColumn.Create("value", rows.Select(DynamicValue).ToArray())),
            ConverterRouteShape.CreateArrayLowCardinalityString => ("Array(LowCardinality(String))", Dense(rows, element => $"category-{element % 100}")),
            ConverterRouteShape.CreateArrayNullableInt32 => ("Array(Nullable(Int32))", Dense(rows, element => element % 5 == 0 ? null : (int?)element)),
            ConverterRouteShape.LowCardinalityUuidHighRepeat => ("LowCardinality(UUID)", ClickHouseTcpColumn.Create("value", rows.Select(i => Uuid(i % 100)).ToArray())),
            ConverterRouteShape.LowCardinalityUuidLowRepeat => ("LowCardinality(UUID)", ClickHouseTcpColumn.Create("value", rows.Select(Uuid).ToArray())),
            _ => ("LowCardinality(UInt32)", ClickHouseTcpColumn.Create("value", rows.Select(i => (uint)(i % 100) * 2_654_435_761u).ToArray())),
        };
    }

    private static object DynamicValue(int row) => (row % 8) switch
    {
        0 => row,
        1 => $"category-{row % 100}",
        2 => row * 0.5,
        3 => null,
        4 => (long)row << 20,
        5 => Enumerable.Range(row, row % 4).ToArray(),
        6 => (row, $"item-{row % 100}"),
        _ => DateTime.UnixEpoch.AddSeconds(row),
    };

    // A UUID for each number, with the bits of the number spread over all 16 bytes.
    private static Guid Uuid(int number)
    {
        ulong first = (ulong)number * 0x9E3779B97F4A7C15UL;
        ulong second = (first ^ (ulong)number) * 0xBF58476D1CE4E5B9UL;
        var bytes = new byte[16];
        BitConverter.TryWriteBytes(bytes.AsSpan(0, 8), first);
        BitConverter.TryWriteBytes(bytes.AsSpan(8, 8), second);
        return new Guid(bytes);
    }

    // A dense array of the rows: row i has i % 5 elements, numbered across the rows of the block.
    private static IColumn Dense<T>(IEnumerable<int> rows, Func<int, T> element)
    {
        int[] lengths = rows.Select(i => i % 5).ToArray();
        var offsets = new int[lengths.Length + 1];
        for (int i = 0; i < lengths.Length; i++)
        {
            offsets[i + 1] = offsets[i] + lengths[i];
        }

        T[] elements = Enumerable.Range(0, offsets[lengths.Length]).Select(element).ToArray();
        return ClickHouseTcpColumn.CreateArray("value", ClickHouseTcpColumn.Create("value", elements), offsets);
    }
}
