using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Benchmark;

/// <summary>The columns that <see cref="TcpConverterRead"/> reads, and the CLR type it reads each as.</summary>
public enum ConverterReadShape
{
    /// <summary><c>Array(String)</c> as <c>byte[][]</c>: 0 to 4 elements in a row.</summary>
    ArrayStringAsBytes,

    /// <summary><c>LowCardinality(String)</c> as <c>byte[]</c>: 100 distinct values.</summary>
    LowCardinalityStringAsBytes,

    /// <summary><c>Nullable(DateTime('UTC'))</c> as <c>DateTimeOffset?</c>: every fifth row NULL.</summary>
    NullableDateTimeAsOffset,

    /// <summary><c>DateTime('UTC')</c> as <c>DateTimeOffset</c>.</summary>
    DateTimeAsOffset,

    /// <summary><c>String</c> as <c>string</c>: every value distinct.</summary>
    StringAsString,

    /// <summary><c>LowCardinality(Nullable(String))</c> as <c>string</c>: 30 distinct values, every tenth row NULL.</summary>
    LowCardinalityNullableStringAsString,

    /// <summary>The six columns above in one block.</summary>
    Wide,
}

/// <summary>
/// Prices the reads that the converter layer (ClickHouse/integrations#801) changes, through the client's own entry
/// points: <c>Block.ReadAs&lt;T&gt;</c> and the POCO read plan that <c>QueryAsync&lt;T&gt;</c> uses. In memory, with
/// no server.
/// </summary>
/// <remarks>
/// <para>
/// The benchmark measures whichever implementation is behind those entry points, so a run on two commits compares
/// the two implementations. The shapes are the read shapes of the converter spike.
/// </para>
/// <para>
/// Each invocation reads a block that <see cref="FreshBlock"/> decodes for that invocation only, with the block reader
/// that a query uses. <c>StringColumn</c> and the <c>LowCardinality</c> columns keep their decoded values after the
/// first read, so a block that two invocations share measures cache hits.
/// </para>
/// <para>
/// <see cref="Columnar"/> reads <c>Values</c> of each column, which converts every row. <see cref="Poco"/> reads in
/// windows of 256 rows, as <c>QueryAsync&lt;T&gt;</c> does, with the plan that the client builds. That plan makes rows
/// with a compiled constructor.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
[InvocationCount(1, 1)]
public class TcpConverterRead
{
    // ClickHouseTcpClient.MaterializationWindowRows.
    private const int WindowRows = 256;

    private static readonly NegotiatedProtocol Negotiated = new(NegotiatedProtocol.ClientTcpProtocolVersion);

    private static readonly ResolveContext Context = new() { ServerTimezone = "UTC" };

    private byte[] encoded;
    private Block block;
    private Func<Block, int> poco;

    /// <summary>The number of rows in the block.</summary>
    [Params(100_000)]
    public int Rows { get; set; }

    /// <summary>The columns of the block.</summary>
    [ParamsAllValues]
    public ConverterReadShape Shape { get; set; }

    /// <summary>Encodes the block once, and builds the POCO read plan for its shape.</summary>
    [GlobalSetup]
    public void Setup()
    {
        encoded = Encode(Columns(Shape, Rows), Rows);
        block = Decode();
        poco = Shape switch
        {
            ConverterReadShape.ArrayStringAsBytes => PocoReader<ItemsRow>(block),
            ConverterReadShape.LowCardinalityStringAsBytes => PocoReader<CategoryRow>(block),
            ConverterReadShape.NullableDateTimeAsOffset => PocoReader<SeenAtRow>(block),
            ConverterReadShape.DateTimeAsOffset => PocoReader<CreatedAtRow>(block),
            ConverterReadShape.StringAsString => PocoReader<TextRow>(block),
            ConverterReadShape.LowCardinalityNullableStringAsString => PocoReader<TagRow>(block),
            _ => PocoReader<WideRow>(block),
        };
    }

    /// <summary>Decodes the block again, so that no column cache is filled.</summary>
    [IterationSetup]
    public void FreshBlock()
    {
        block?.Dispose();
        block = Decode();
    }

    /// <summary>Releases the last block.</summary>
    [GlobalCleanup]
    public void Cleanup() => block?.Dispose();

    /// <summary><c>Block.ReadAs&lt;T&gt;(i).Values</c> for each column.</summary>
    /// <returns>The number of values read.</returns>
    [Benchmark]
    public int Columnar()
    {
        int values = 0;
        for (int i = 0; i < block.ColumnCount; i++)
        {
            values += block[i].Name switch
            {
                "items" => block.ReadAs<byte[][]>(i).Values.Length,
                "category" => block.ReadAs<byte[]>(i).Values.Length,
                "seen_at" => block.ReadAs<DateTimeOffset?>(i).Values.Length,
                "created_at" => block.ReadAs<DateTimeOffset>(i).Values.Length,
                _ => block.ReadAs<string>(i).Values.Length,
            };
        }

        return values;
    }

    /// <summary>The POCO read plan, in windows of 256 rows.</summary>
    /// <returns>The number of rows read.</returns>
    [Benchmark]
    public int Poco() => poco(block);

    private static Func<Block, int> PocoReader<TRow>(Block first)
        where TRow : class
    {
        PocoReadPlan<TRow> plan = PocoReadPlan<TRow>.Build(PocoTypeDescriptor<TRow>.Build(), first, forcedTier: null);
        return block =>
        {
            if (!plan.MatchesHeader(block))
            {
                throw new InvalidOperationException("The block does not have the shape of the plan.");
            }

            TRow[] rows = ArrayPool<TRow>.Shared.Rent(Math.Min(WindowRows, block.RowCount));
            try
            {
                int read = 0;
                for (int start = 0; start < block.RowCount; start += WindowRows)
                {
                    int count = Math.Min(WindowRows, block.RowCount - start);
                    plan.Materialize(block, rows, start, count, start);
                    read += count;
                }

                return read;
            }
            finally
            {
                ArrayPool<TRow>.Shared.Return(rows, clearArray: true);
            }
        };
    }

    private static List<(string Name, string Type, IColumn Values)> Columns(ConverterReadShape shape, int rows)
    {
        var columns = new List<(string Name, string Type, IColumn Values)>();
        bool wide = shape == ConverterReadShape.Wide;
        if (wide || shape == ConverterReadShape.ArrayStringAsBytes)
        {
            columns.Add(("items", "Array(String)", ClickHouseTcpColumn.Create(
                "items",
                Enumerable.Range(0, rows).Select(i => Enumerable.Range(0, i % 5).Select(j => $"item-{i}-{j}").ToArray()).ToArray())));
        }

        if (wide || shape == ConverterReadShape.LowCardinalityStringAsBytes)
        {
            columns.Add(("category", "LowCardinality(String)", ClickHouseTcpColumn.Create(
                "category",
                Enumerable.Range(0, rows).Select(i => $"category-{i % 100}").ToArray())));
        }

        if (wide || shape == ConverterReadShape.NullableDateTimeAsOffset)
        {
            columns.Add(("seen_at", "Nullable(DateTime('UTC'))", ClickHouseTcpColumn.Create(
                "seen_at",
                Enumerable.Range(0, rows).Select(i => i % 5 == 0 ? (DateTimeOffset?)null : DateTimeOffset.FromUnixTimeSeconds(1_700_000_000 + i)).ToArray())));
        }

        if (wide || shape == ConverterReadShape.StringAsString)
        {
            columns.Add(("text", "String", ClickHouseTcpColumn.Create(
                "text",
                Enumerable.Range(0, rows).Select(i => $"text-{i}").ToArray())));
        }

        if (wide || shape == ConverterReadShape.DateTimeAsOffset)
        {
            columns.Add(("created_at", "DateTime('UTC')", ClickHouseTcpColumn.Create(
                "created_at",
                Enumerable.Range(0, rows).Select(i => DateTimeOffset.FromUnixTimeSeconds(1_600_000_000 + i)).ToArray())));
        }

        if (wide || shape == ConverterReadShape.LowCardinalityNullableStringAsString)
        {
            columns.Add(("tag", "LowCardinality(Nullable(String))", ClickHouseTcpColumn.Create(
                "tag",
                Enumerable.Range(0, rows).Select(i => i % 10 == 0 ? null : $"tag-{i % 30}").ToArray())));
        }

        return columns;
    }

    // The block as an insert writes it.
    private static byte[] Encode(List<(string Name, string Type, IColumn Values)> columns, int rows)
    {
        InsertColumn[] plan = columns
            .Select(c => new InsertColumn(c.Name, c.Type, ColumnCodecRegistry.Default.Resolve(c.Type, Context), c.Values))
            .ToArray();

        using var stream = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(stream))
        {
            BlockWriter.WriteDataBlockAsync(writer, Negotiated, plan, start: 0, rows, BlockWriter.DefaultFlushThresholdBytes, CancellationToken.None)
                .AsTask().GetAwaiter().GetResult();
            writer.FlushAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }

        return stream.ToArray();
    }

    // The block as a query reads it.
    private Block Decode()
    {
        using var reader = new ClickHouseBinaryReader(new MemoryStream(encoded));
        return BlockReader.ReadBlockAsync(reader, Negotiated, ColumnCodecRegistry.Default, Context, CancellationToken.None)
            .AsTask().GetAwaiter().GetResult();
    }

    /// <summary>A row of <see cref="ConverterReadShape.ArrayStringAsBytes"/>.</summary>
    public sealed class ItemsRow
    {
        public byte[][] Items { get; set; }
    }

    /// <summary>A row of <see cref="ConverterReadShape.LowCardinalityStringAsBytes"/>.</summary>
    public sealed class CategoryRow
    {
        public byte[] Category { get; set; }
    }

    /// <summary>A row of <see cref="ConverterReadShape.NullableDateTimeAsOffset"/>.</summary>
    public sealed class SeenAtRow
    {
        public DateTimeOffset? SeenAt { get; set; }
    }

    /// <summary>A row of <see cref="ConverterReadShape.DateTimeAsOffset"/>.</summary>
    public sealed class CreatedAtRow
    {
        public DateTimeOffset CreatedAt { get; set; }
    }

    /// <summary>A row of <see cref="ConverterReadShape.StringAsString"/>.</summary>
    public sealed class TextRow
    {
        public string Text { get; set; }
    }

    /// <summary>A row of <see cref="ConverterReadShape.LowCardinalityNullableStringAsString"/>.</summary>
    public sealed class TagRow
    {
        public string Tag { get; set; }
    }

    /// <summary>A row of <see cref="ConverterReadShape.Wide"/>.</summary>
    public sealed class WideRow
    {
        public byte[][] Items { get; set; }

        public byte[] Category { get; set; }

        public DateTimeOffset? SeenAt { get; set; }

        public string Text { get; set; }

        public DateTimeOffset CreatedAt { get; set; }

        public string Tag { get; set; }
    }
}
