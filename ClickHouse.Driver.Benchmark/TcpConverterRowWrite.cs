using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Benchmark;

/// <summary>The columns that <see cref="TcpConverterRowWrite"/> inserts from rows, and the CLR type of their values.</summary>
public enum ConverterRowWriteShape
{
    /// <summary><c>LowCardinality(String)</c> from <c>string</c>: 100 distinct values.</summary>
    LowCardinalityStringHighRepeat,

    /// <summary><c>LowCardinality(String)</c> from <c>string</c>: every value distinct.</summary>
    LowCardinalityStringLowRepeat,

    /// <summary><c>Nullable(DateTime('UTC'))</c> from <c>DateTimeOffset?</c>: every fifth row NULL.</summary>
    NullableDateTimeFromOffset,

    /// <summary><c>Array(String)</c> from <c>string[]</c>: 0 to 4 elements in a row.</summary>
    ArrayStringFromText,

    /// <summary>
    /// Six columns in one row: the four columns above (<c>LowCardinality(String)</c> with 100 distinct values),
    /// <c>String</c> from <c>string</c>, <c>DateTime('UTC')</c> from <c>DateTimeOffset</c>, and
    /// <c>LowCardinality(Nullable(String))</c> from <c>string</c> (30 distinct values, every tenth row NULL).
    /// </summary>
    Wide,
}

/// <summary>
/// Prices the row inserts that the converter layer (ClickHouse/integrations#801) changes, through the code that an
/// insert runs for one block: <c>InsertRowsAsync&lt;T&gt;</c> (the POCO write plan) and
/// <c>InsertRowsAsync(object[])</c> (the untyped row columns), then the plan of the columns and the block writer. In
/// memory, with no server.
/// </summary>
/// <remarks>
/// <para>
/// The benchmark measures whichever implementation is behind those entry points, so a run on two commits compares
/// the two implementations. The shapes are the write shapes of the converter spike, as rows.
/// </para>
/// <para>
/// One operation does what an insert of one block of rows does: it opens the column source of the insert over the
/// rows (<see cref="Poco"/>: the cached POCO write plan; <see cref="Untyped"/>: the untyped row columns, which choose
/// the CLR type of each column from the values), plans the write of each column as <c>InsertAsync</c> plans the
/// columns of a row insert, gathers the rows into the columns, writes the data block and flushes. An invocation
/// inserts <see cref="BlocksPerInvocation"/> different arrays of <see cref="Rows"/> rows, rows <c>[0, 100,000)</c> of
/// the spike's data. The writer stays open between invocations, as the writer of a connection does, and its stream
/// discards the bytes.
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
public class TcpConverterRowWrite
{
    /// <summary>The number of blocks that one invocation inserts; each block is one operation.</summary>
    public const int BlocksPerInvocation = 10;

    private static readonly NegotiatedProtocol Negotiated = new(NegotiatedProtocol.ClientTcpProtocolVersion);

    // The context of the server's sample block, which an insert resolves the target types with.
    private static readonly ResolveContext SchemaContext = new() { ServerTimezone = "UTC" };

    private readonly PocoTypeRegistry registry = new();
    private Block schema;
    private Func<int, ValueTask> insertPoco;
    private object[][][] untypedRows;
    private ClickHouseBinaryWriter writer;

    /// <summary>The number of rows in one block.</summary>
    [Params(10_000)]
    public int Rows { get; set; }

    /// <summary>The columns to insert.</summary>
    [ParamsAllValues]
    public ConverterRowWriteShape Shape { get; set; }

    /// <summary>Builds the rows once, the sample block of the target columns, and opens the writer.</summary>
    [GlobalSetup]
    public void Setup()
    {
        (string Name, string Type)[] targets = Targets(Shape);
        schema = new Block(
            string.Empty,
            BlockInfo.Default,
            rowCount: 0,
            targets.Select(t => (IColumn)new ArrayColumn<object>(t.Name, t.Type, Array.Empty<object>())).ToArray(),
            ColumnCodecRegistry.Default,
            SchemaContext);

        untypedRows = new object[BlocksPerInvocation][][];
        for (int b = 0; b < BlocksPerInvocation; b++)
        {
            untypedRows[b] = Enumerable.Range(b * Rows, Rows).Select(i => UntypedRow(Shape, i)).ToArray();
        }

        insertPoco = Shape switch
        {
            ConverterRowWriteShape.LowCardinalityStringHighRepeat => PocoInserter(i => new CategoryRow { Category = Category(i, distinct: 100) }),
            ConverterRowWriteShape.LowCardinalityStringLowRepeat => PocoInserter(i => new CategoryRow { Category = Category(i, distinct: AllRows) }),
            ConverterRowWriteShape.NullableDateTimeFromOffset => PocoInserter(i => new SeenAtRow { SeenAt = SeenAt(i) }),
            ConverterRowWriteShape.ArrayStringFromText => PocoInserter(i => new ItemsRow { Items = Items(i) }),
            _ => PocoInserter(i => new WideRow
            {
                Items = Items(i),
                Category = Category(i, distinct: 100),
                SeenAt = SeenAt(i),
                Text = Text(i),
                CreatedAt = CreatedAt(i),
                Tag = Tag(i),
            }),
        };

        writer = new ClickHouseBinaryWriter(Stream.Null);
    }

    /// <summary>Closes the writer.</summary>
    [GlobalCleanup]
    public void Cleanup() => writer?.Dispose();

    /// <summary>For each array of POCO rows: the insert of <c>InsertRowsAsync&lt;T&gt;</c> as one data block.</summary>
    /// <returns>The number of bytes written.</returns>
    [Benchmark(OperationsPerInvoke = BlocksPerInvocation)]
    public async Task<long> Poco()
    {
        long before = writer.BytesWritten;
        for (int b = 0; b < BlocksPerInvocation; b++)
        {
            await insertPoco(b);
        }

        return writer.BytesWritten - before;
    }

    /// <summary>For each array of <c>object[]</c> rows: the insert of <c>InsertRowsAsync(object[])</c> as one data block.</summary>
    /// <returns>The number of bytes written.</returns>
    [Benchmark(OperationsPerInvoke = BlocksPerInvocation)]
    public async Task<long> Untyped()
    {
        long before = writer.BytesWritten;
        foreach (object[][] rows in untypedRows)
        {
            using var buffer = PocoRowBuffer<object[]>.Create(rows, "rows", Rows, CancellationToken.None);
            using PocoInsertSource<object[]> source = UntypedRowColumns.CreateSource(schema, buffer, Rows);
            await WriteBlockAsync(source);
        }

        return writer.BytesWritten - before;
    }

    private int AllRows => BlocksPerInvocation * Rows;

    private static (string Name, string Type)[] Targets(ConverterRowWriteShape shape) => shape switch
    {
        ConverterRowWriteShape.LowCardinalityStringHighRepeat or ConverterRowWriteShape.LowCardinalityStringLowRepeat => new[] { ("category", "LowCardinality(String)") },
        ConverterRowWriteShape.NullableDateTimeFromOffset => new[] { ("seen_at", "Nullable(DateTime('UTC'))") },
        ConverterRowWriteShape.ArrayStringFromText => new[] { ("items", "Array(String)") },
        _ => new[]
        {
            ("items", "Array(String)"),
            ("category", "LowCardinality(String)"),
            ("seen_at", "Nullable(DateTime('UTC'))"),
            ("text", "String"),
            ("created_at", "DateTime('UTC')"),
            ("tag", "LowCardinality(Nullable(String))"),
        },
    };

    private static string[] Items(int i) => Enumerable.Range(0, i % 5).Select(j => $"item-{i}-{j}").ToArray();

    private static string Category(int i, int distinct) => $"category-{i % distinct}";

    private static DateTimeOffset? SeenAt(int i) => i % 5 == 0 ? null : DateTimeOffset.FromUnixTimeSeconds(1_700_000_000 + i);

    private static string Text(int i) => $"text-{i}";

    private static DateTimeOffset CreatedAt(int i) => DateTimeOffset.FromUnixTimeSeconds(1_600_000_000 + i);

    private static string Tag(int i) => i % 10 == 0 ? null : $"tag-{i % 30}";

    // A boxed DateTimeOffset? is a boxed DateTimeOffset or null, as in a row that a caller builds.
    private object[] UntypedRow(ConverterRowWriteShape shape, int i) => shape switch
    {
        ConverterRowWriteShape.LowCardinalityStringHighRepeat => new object[] { Category(i, distinct: 100) },
        ConverterRowWriteShape.LowCardinalityStringLowRepeat => new object[] { Category(i, distinct: AllRows) },
        ConverterRowWriteShape.NullableDateTimeFromOffset => new object[] { SeenAt(i) },
        ConverterRowWriteShape.ArrayStringFromText => new object[] { Items(i) },
        _ => new object[] { Items(i), Category(i, distinct: 100), SeenAt(i), Text(i), CreatedAt(i), Tag(i) },
    };

    // The insert of one array of rows, for each block: the source over the cached plan, then the block.
    private Func<int, ValueTask> PocoInserter<TRow>(Func<int, TRow> row)
        where TRow : class
    {
        var rows = new TRow[BlocksPerInvocation][];
        for (int b = 0; b < BlocksPerInvocation; b++)
        {
            rows[b] = Enumerable.Range(b * Rows, Rows).Select(row).ToArray();
        }

        return async b =>
        {
            using var buffer = PocoRowBuffer<TRow>.Create(rows[b], "rows", Rows, CancellationToken.None);
            using PocoInsertSource<TRow> source = registry.WritePlanFor<TRow>(schema).CreateSource(buffer, Rows);
            await WriteBlockAsync(source);
        };
    }

    // Plans the columns, gathers the rows into them, writes them as one data block and flushes.
    private async ValueTask WriteBlockAsync(IInsertColumnSource source)
    {
        InsertColumn[] plan = Plan(source.Columns);
        source.Gather(0, Rows);
        await BlockWriter.WriteDataBlockAsync(writer, Negotiated, plan, start: 0, Rows, BlockWriter.DefaultFlushThresholdBytes, CancellationToken.None);
        await writer.FlushAsync(CancellationToken.None);
    }

    // The plan that InsertAsync builds for the columns of a row insert (ClickHouseTcpConnection.BuildInsertPlan, which
    // is private): the codec of each target type, and the write of the column.
    private InsertColumn[] Plan(IReadOnlyList<IColumn> columns)
    {
        var plan = new InsertColumn[columns.Count];
        for (int i = 0; i < plan.Length; i++)
        {
            IColumn column = columns[i];
            IColumnCodec codec = schema.Codecs.Resolve(column.TypeName, schema.Context);
            InsertColumnWrite write = codec.CanWrite(column)
                ? InsertColumnWrite.ThroughCodec(codec)
                : throw new InvalidOperationException($"The insert plan of '{column.TypeName}' does not accept the column.");
            plan[i] = new InsertColumn(column.Name, column.TypeName, codec, column, write);
        }

        return plan;
    }

    /// <summary>A row of <see cref="ConverterRowWriteShape.LowCardinalityStringHighRepeat"/> and <see cref="ConverterRowWriteShape.LowCardinalityStringLowRepeat"/>.</summary>
    public sealed class CategoryRow
    {
        public string Category { get; set; }
    }

    /// <summary>A row of <see cref="ConverterRowWriteShape.NullableDateTimeFromOffset"/>.</summary>
    public sealed class SeenAtRow
    {
        public DateTimeOffset? SeenAt { get; set; }
    }

    /// <summary>A row of <see cref="ConverterRowWriteShape.ArrayStringFromText"/>.</summary>
    public sealed class ItemsRow
    {
        public string[] Items { get; set; }
    }

    /// <summary>A row of <see cref="ConverterRowWriteShape.Wide"/>.</summary>
    public sealed class WideRow
    {
        public string[] Items { get; set; }

        public string Category { get; set; }

        public DateTimeOffset? SeenAt { get; set; }

        public string Text { get; set; }

        public DateTimeOffset CreatedAt { get; set; }

        public string Tag { get; set; }
    }
}
