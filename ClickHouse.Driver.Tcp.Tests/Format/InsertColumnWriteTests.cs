using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Types.Converters;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Format;

/// <summary>
/// The write of one insert column (<see cref="InsertColumnWrite"/>): the routing of a column to its codec or to the
/// converter derivation (decisions D2 and D3), the value source of the rows of a block, and slices that start after
/// earlier rows, also through the block writer.
/// </summary>
[TestFixture]
public class InsertColumnWriteTests
{
    private static readonly NegotiatedProtocol Negotiated = new(54476);

    /// <summary>
    /// A decoded <c>String</c> column is written from its stored bytes, so bytes that UTF-8 cannot spell stay as they
    /// are; the same column written through its text would carry U+FFFD.
    /// </summary>
    [TestCase("String")]
    [TestCase("Nullable(String)")]
    [TestCase("Array(String)")]
    [TestCase("LowCardinality(String)")]
    [TestCase("LowCardinality(Nullable(String))")]
    public async Task For_DecodedColumn_WritesItsStoredBytes(string type)
    {
        byte[][] values = { new byte[] { 0x41 }, new byte[] { 0xFF, 0xFE }, type.Contains("Nullable", StringComparison.Ordinal) ? null : new byte[] { 0x42 } };
        byte[] wire = type.StartsWith("Array", StringComparison.Ordinal)
            ? await ConverterHarness.WriteNewAsync(ConverterDerivation.Default.Writer<byte[][]>(type, ConverterHarness.Context), new[] { values, Array.Empty<byte[]>() }, 0, 2)
            : await ConverterHarness.WriteNewAsync(ConverterDerivation.Default.Writer<byte[]>(type, ConverterHarness.Context), values, 0, values.Length);
        int rows = type.StartsWith("Array", StringComparison.Ordinal) ? 2 : values.Length;
        using IColumn decoded = await DecodeAsync(type, wire, rows);
        IColumnCodec codec = ConverterHarness.Codec(type);

        InsertColumnWrite write = InsertColumnWrite.For(codec, decoded, type, ConverterHarness.Context, ConverterDerivation.Default);

        Assert.That(codec.WritesFromStorage(decoded), Is.True);
        Assert.That(Convert.ToHexString(await WriteAsync(write, decoded, 0, rows)), Is.EqualTo(Convert.ToHexString(wire)));
    }

    /// <summary>
    /// A column that a caller builds goes to the converter derivation, which also writes what the codec refuses:
    /// <c>LowCardinality(String)</c> from <see cref="T:byte[]"/> (ClickHouse/integrations#792).
    /// </summary>
    [Test]
    public void For_CallerColumnThatTheCodecRefuses_WritesThroughTheDerivation()
    {
        IColumnCodec codec = ConverterHarness.Codec("LowCardinality(String)");
        var column = new ArrayColumn<byte[]>("c", null, new[] { new byte[] { 0xFF } });

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(column), Is.False);
            Assert.That(codec.WritesFromStorage(column), Is.False);
            Assert.That(InsertColumnWrite.For(codec, column, "LowCardinality(String)", ConverterHarness.Context, ConverterDerivation.Default), Is.Not.Null);
        });
    }

    /// <summary>A column of a CLR type that the type is not written from has no write.</summary>
    [Test]
    public void For_ColumnThatCannotBeWritten_GivesNull()
    {
        IColumnCodec codec = ConverterHarness.Codec("String");

        Assert.Multiple(() =>
        {
            Assert.That(InsertColumnWrite.For(codec, new ArrayColumn<int>("c", null, new[] { 1 }), "String", ConverterHarness.Context, ConverterDerivation.Default), Is.Null);
            Assert.That(InsertColumnWrite.For(ConverterHarness.Codec("UInt8"), new TwoTypedColumn(), "UInt8", ConverterHarness.Context, ConverterDerivation.Default), Is.Null);
        });
    }

    /// <summary>
    /// A column of no single CLR type has no converter tree, so the codec decides, and writes it as it accepts it: the
    /// String codec writes the column's <see cref="string"/> values.
    /// </summary>
    [Test]
    public async Task For_ColumnOfNoSingleClrType_IsWrittenByTheCodecWhenItAcceptsTheColumn()
    {
        IColumnCodec codec = ConverterHarness.Codec("String");
        var column = new TwoTypedColumn();

        InsertColumnWrite write = InsertColumnWrite.For(codec, column, "String", ConverterHarness.Context, ConverterDerivation.Default);

        Assert.That(write, Is.Not.Null);
        Assert.That(Convert.ToHexString(await WriteAsync(write, column, 0, 1)), Is.EqualTo("0161"));
    }

    /// <summary>
    /// The rows of a slice come from the column's span, from its stored values, or through its indexer when its
    /// <c>Values</c> cannot serve; each gives the bytes of the codec for a slice that starts after earlier rows.
    /// </summary>
    [Test]
    public async Task Write_SliceOfEachKindOfColumn_GivesTheBytesOfTheCodec()
    {
        var offsets = new[] { DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddDays(1), DateTimeOffset.UnixEpoch.AddDays(2), DateTimeOffset.UnixEpoch.AddDays(3) };
        IColumn spanColumn = new ArrayColumn<DateTimeOffset>("c", null, offsets);
        using IColumn stored = await ConverterHarness.DecodeAsync("DateTime('UTC')", spanColumn);
        var computed = new IndexerOnlyColumn<DateTimeOffset>(offsets);

        Assert.That(stored, Is.InstanceOf<IStoredValuesColumn>().And.Not.InstanceOf<ISpanColumn<uint>>());
        foreach ((IColumn column, string kind) in new[] { (spanColumn, "span"), (stored, "stored values"), ((IColumn)computed, "indexer") })
        {
            IColumnCodec codec = ConverterHarness.Codec("DateTime('UTC')");
            InsertColumnWrite write = InsertColumnWrite.For(codec, column, "DateTime('UTC')", ConverterHarness.Context, ConverterDerivation.Default);
            byte[] expected = await ConverterHarness.WriteOldAsync("DateTime('UTC')", column is IndexerOnlyColumn<DateTimeOffset> ? spanColumn : column, 1, 3);
            Assert.That(Convert.ToHexString(await WriteAsync(write, column, 1, 3)), Is.EqualTo(Convert.ToHexString(expected)), kind);
        }
    }

    /// <summary>
    /// A block of a slice from row 2 that the block writer writes with the plan's write of each column has the bytes of
    /// the block that the codecs write.
    /// </summary>
    [Test]
    public async Task WriteDataBlockAsync_SliceWithTheConverterWrites_GivesTheBytesOfTheCodecs()
    {
        IColumn[] columns =
        {
            new ArrayColumn<string>("s", null, new[] { "a", null, "b", "c", null }),
            new ArrayColumn<string[]>("lc", null, new[] { new[] { "x" }, new[] { "y", "x" }, Array.Empty<string>(), new[] { "z" }, new[] { "x", "x" } }),
            new ArrayColumn<KeyValuePair<string, int>[]>("m", null, new[] { Pairs(("a", 1)), Pairs(), Pairs(("b", 2), ("c", 3)), Pairs(("d", 4)), Pairs() }),
            new ArrayColumn<(int, string)>("t", null, new[] { (1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e") }),
            new ArrayColumn<object>("v", null, new object[] { "a", 1UL, null, "b", 2UL }),
            new ArrayColumn<DateTimeOffset?>("d", null, new DateTimeOffset?[] { DateTimeOffset.UnixEpoch, null, DateTimeOffset.UnixEpoch.AddDays(1), null, DateTimeOffset.UnixEpoch }),
        };
        string[] types = { "Nullable(String)", "Array(LowCardinality(String))", "Map(String, Int32)", "Tuple(Int32, String)", "Variant(String, UInt64)", "Nullable(DateTime('UTC'))" };

        var throughCodecs = new InsertColumn[columns.Length];
        var throughPlan = new InsertColumn[columns.Length];
        for (int i = 0; i < columns.Length; i++)
        {
            IColumnCodec codec = ConverterHarness.Codec(types[i]);
            throughCodecs[i] = new InsertColumn(columns[i].Name, types[i], codec, columns[i]);
            throughPlan[i] = new InsertColumn(columns[i].Name, types[i], codec, columns[i], InsertColumnWrite.For(codec, columns[i], types[i], ConverterHarness.Context, ConverterDerivation.Default));
        }

        byte[] expected = await WriteBlockAsync(throughCodecs);
        byte[] actual = await WriteBlockAsync(throughPlan);

        Assert.That(Convert.ToHexString(actual), Is.EqualTo(Convert.ToHexString(expected)));
    }

    private static async Task<byte[]> WriteBlockAsync(InsertColumn[] columns)
    {
        using var stream = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(stream))
        {
            await BlockWriter.WriteDataBlockAsync(writer, Negotiated, columns, start: 2, rowCount: 3, BlockWriter.DefaultFlushThresholdBytes, CancellationToken.None);
            await writer.FlushAsync(CancellationToken.None);
        }

        return stream.ToArray();
    }

    // Reads the state prefix and the body of a column from the bytes, as a query read decodes it.
    private static async Task<IColumn> DecodeAsync(string type, byte[] bytes, int rows)
    {
        IColumnCodec codec = ConverterHarness.Codec(type);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        await codec.ReadStatePrefixAsync(reader, CancellationToken.None);
        return await codec.ReadColumnAsync(reader, "c", type, rows, CancellationToken.None);
    }

    private static KeyValuePair<string, int>[] Pairs(params (string Key, int Value)[] pairs)
        => Array.ConvertAll(pairs, pair => new KeyValuePair<string, int>(pair.Key, pair.Value));

    private static Task<byte[]> WriteAsync(InsertColumnWrite write, IColumn column, int start, int length)
        => CodecTestHarness.WriteAsync(w =>
        {
            IColumnWriteState state = write.Begin(column, start, length);
            try
            {
                write.WritePrefix(w, column, start, length, state);
                write.Write(w, column, start, length, state);
            }
            finally
            {
                state?.Dispose();
            }
        });

    // A column whose Values cannot be read, as for a view that computes each value: only the indexer serves.
    private sealed class IndexerOnlyColumn<T> : IColumn<T>
    {
        private readonly T[] values;

        public IndexerOnlyColumn(T[] values) => this.values = values;

        public string Name => "c";

        public string TypeName => null;

        public int RowCount => values.Length;

        public ReadOnlySpan<T> Values => throw new NotSupportedException("This column has no span of its values.");

        public T this[int row] => values[row];

        public object GetValue(int row) => values[row];

        public void Dispose()
        {
        }
    }

    // A column that implements IColumn<> twice, so it has no single element type.
    private sealed class TwoTypedColumn : IColumn<string>, IColumn<int>
    {
        public string Name => "c";

        public string TypeName => null;

        public int RowCount => 1;

        ReadOnlySpan<string> IColumn<string>.Values => new[] { "a" };

        ReadOnlySpan<int> IColumn<int>.Values => new[] { 1 };

        string IColumn<string>.this[int row] => "a";

        int IColumn<int>.this[int row] => 1;

        public object GetValue(int row) => "a";

        public void Dispose()
        {
        }
    }
}
