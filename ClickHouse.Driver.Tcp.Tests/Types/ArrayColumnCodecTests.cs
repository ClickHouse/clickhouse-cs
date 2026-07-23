using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class ArrayColumnCodecTests
{
    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    [Test]
    public async Task ReadColumn_WriteThenRead_FixedWidthInnerRoundTripsWithEmptyRows()
    {
        IColumnCodec codec = Resolve("Array(UInt32)");
        var expected = new[] { new uint[] { 10, 20, 30 }, Array.Empty<uint>(), new uint[] { 40, 50 } };
        var column = new ArrayColumn<uint[]>("c", "Array(UInt32)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Array(UInt32)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(read.TypeName, Is.EqualTo("Array(UInt32)"));
            Assert.That(read.RowCount, Is.EqualTo(3));
            Assert.That(((IColumn<uint[]>)read).Values.ToArray(), Is.EqualTo(expected));
            Assert.That(read.GetValue(1), Is.EqualTo(Array.Empty<uint>()));
        });
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_VariableInnerRoundTrips()
    {
        IColumnCodec codec = Resolve("Array(String)");
        var expected = new[] { new[] { "a", "bb" }, Array.Empty<string>(), new[] { string.Empty, "héllo✓" } };
        var column = new ArrayColumn<string[]>("c", "Array(String)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Array(String)", column.RowCount);

        Assert.That(((IColumn<string[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_NestedArrayRoundTrips()
    {
        IColumnCodec codec = Resolve("Array(Array(UInt32))");
        var expected = new[]
        {
            new[] { new uint[] { 1, 2 } },
            Array.Empty<uint[]>(),
            new[] { new uint[] { 3 }, new uint[] { 4, 5 } },
        };
        var column = new ArrayColumn<uint[][]>("c", "Array(Array(UInt32))", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Array(Array(UInt32))", column.RowCount);

        Assert.That(((IColumn<uint[][]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_NullableInnerRoundTrips()
    {
        IColumnCodec codec = Resolve("Array(Nullable(UInt32))");
        var expected = new[] { new uint?[] { 1, null, 3 }, Array.Empty<uint?>(), new uint?[] { null, null } };
        var column = new ArrayColumn<uint?[]>("c", "Array(Nullable(UInt32))", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Array(Nullable(UInt32))", column.RowCount);

        Assert.That(((IColumn<uint?[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_EmptyColumn_ReadsZeroRowsWithoutConsumingBytes()
    {
        IColumnCodec codec = Resolve("Array(UInt32)");
        var column = new ArrayColumn<uint[]>("c", "Array(UInt32)", Array.Empty<uint[]>());

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column, 0, 0));
        Assert.That(bytes, Is.Empty, "an empty array column writes no offsets and no values");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Array(UInt32)", 0, CodecTestHarness.None);
        Assert.That(read.RowCount, Is.Zero);
    }

    [Test]
    public async Task ReadColumn_EveryRowEmpty_RoundTripsAsAllEmpty()
    {
        IColumnCodec codec = Resolve("Array(UInt32)");
        var expected = new[] { Array.Empty<uint>(), Array.Empty<uint>(), Array.Empty<uint>() };
        var column = new ArrayColumn<uint[]>("c", "Array(UInt32)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Array(UInt32)", column.RowCount);

        Assert.That(((IColumn<uint[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task WriteColumn_DenseArrayValueColumn_RoundTripsWithoutRebuildingValues()
    {
        // A dense ArrayValueColumn<T> (flat inner column + offsets, the wire's own layout) is the zero-copy write
        // path — the same shape a read produces. Writing one and reading it back must preserve the rows.
        IColumnCodec codec = Resolve("Array(UInt32)");
        var inner = PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 10, 20, 30, 40, 50 });
        var dense = new ArrayValueColumn<uint>("c", "Array(UInt32)", inner, new[] { 0, 3, 3, 5 }, rowCount: 3, pooledOffsets: false);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, dense, "Array(UInt32)", dense.RowCount);

        Assert.That(((IColumn<uint[]>)read).Values.ToArray(), Is.EqualTo(new[] { new uint[] { 10, 20, 30 }, Array.Empty<uint>(), new uint[] { 40, 50 } }));
    }

    [Test]
    public async Task WriteColumn_SlicedRange_WritesOffsetsRelativeToTheSlice()
    {
        // Writing only rows [1, 3) of a four-row column (the insert splitter's per-block path) must emit offsets
        // relative to that block's own values stream, not the full column.
        IColumnCodec codec = Resolve("Array(UInt32)");
        var full = new ArrayColumn<uint[]>("c", "Array(UInt32)", new[]
        {
            new uint[] { 1 },
            new uint[] { 2, 3 },
            Array.Empty<uint>(),
            new uint[] { 4, 5, 6 },
        });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, full, start: 1, length: 2));
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Array(UInt32)", 2, CodecTestHarness.None);

        Assert.That(((IColumn<uint[]>)read).Values.ToArray(), Is.EqualTo(new[] { new uint[] { 2, 3 }, Array.Empty<uint>() }));
    }

    [Test]
    public void MeasureRowBytes_FixedWidthInner_CountsOneOffsetPlusElements()
    {
        IColumnCodec codec = Resolve("Array(UInt32)");
        var column = new ArrayColumn<uint[]>("c", "Array(UInt32)", new[] { new uint[] { 10, 20, 30 }, Array.Empty<uint>() });

        Assert.Multiple(() =>
        {
            Assert.That(codec.MeasureRowBytes(column, 0), Is.EqualTo(8 + (3 * 4))); // one UInt64 offset + three UInt32
            Assert.That(codec.MeasureRowBytes(column, 1), Is.EqualTo(8));           // one offset, no elements
            Assert.That(codec.FixedRowByteSize, Is.Null);
        });
    }

    [Test]
    public void WriteColumn_NullRow_ThrowsArgumentException()
    {
        // Array(T) rows are non-nullable, so a null row is rejected rather than silently written as an empty array.
        IColumnCodec codec = Resolve("Array(UInt32)");
        var column = new ArrayColumn<uint[]>("c", "Array(UInt32)", new[] { new uint[] { 1, 2 }, null, new uint[] { 3 } });

        Assert.ThrowsAsync<ArgumentException>(() => CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column)));
    }

    [Test]
    public void MeasureRowBytes_NullRow_ThrowsArgumentException()
    {
        // Sizing must agree with the write path: a null row is invalid, not empty.
        IColumnCodec codec = Resolve("Array(UInt32)");
        var column = new ArrayColumn<uint[]>("c", "Array(UInt32)", new[] { new uint[] { 1, 2 }, null });

        Assert.Throws<ArgumentException>(() => codec.MeasureRowBytes(column, 1));
    }

    [Test]
    public void MeasureRowBytes_VariableInner_WalksEachElement()
    {
        IColumnCodec codec = Resolve("Array(String)");
        var column = new ArrayColumn<string[]>("c", "Array(String)", new[] { new[] { "a", "bb" }, Array.Empty<string>() });

        Assert.Multiple(() =>
        {
            Assert.That(codec.MeasureRowBytes(column, 0), Is.EqualTo(8 + (1 + 1) + (1 + 2))); // offset + "a" + "bb"
            Assert.That(codec.MeasureRowBytes(column, 1), Is.EqualTo(8));
        });
    }

    [Test]
    public async Task MeasureRowBytes_DenseColumn_PricesFromOffsetsAndInner()
    {
        // A dense ArrayValueColumn (the read-back shape) re-inserted goes through the offsets-based measure path,
        // distinct from the jagged-column path. Both a fixed-width and a variable-width inner are priced.
        IColumnCodec fixedCodec = Resolve("Array(UInt32)");
        var inner = PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 10, 20, 30, 40, 50 });
        var dense = new ArrayValueColumn<uint>("c", "Array(UInt32)", inner, new[] { 0, 3, 3, 5 }, rowCount: 3, pooledOffsets: false);

        IColumnCodec stringCodec = Resolve("Array(String)");
        var jagged = new ArrayColumn<string[]>("c", "Array(String)", new[] { new[] { "a", "bb" }, Array.Empty<string>() });
        using IColumn denseStrings = await CodecTestHarness.RoundTripAsync(stringCodec, jagged, "Array(String)", 2);

        Assert.Multiple(() =>
        {
            Assert.That(fixedCodec.MeasureRowBytes(dense, 0), Is.EqualTo(8 + (3 * 4)));
            Assert.That(fixedCodec.MeasureRowBytes(dense, 1), Is.EqualTo(8));
            Assert.That(fixedCodec.MeasureRowBytes(dense, 2), Is.EqualTo(8 + (2 * 4)));
            Assert.That(stringCodec.MeasureRowBytes(denseStrings, 0), Is.EqualTo(8 + (1 + 1) + (1 + 2)));
            Assert.That(stringCodec.MeasureRowBytes(denseStrings, 1), Is.EqualTo(8));
        });
    }

    [Test]
    public void CanWrite_AcceptsOnlyMatchingArrayColumn()
    {
        IColumnCodec codec = Resolve("Array(UInt32)");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<uint[]>("c", "Array(UInt32)", new[] { new uint[] { 1 } })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<int[]>("c", "Array(Int32)", new[] { new[] { 1 } })), Is.False);
            Assert.That(codec.CanWrite(PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 1 })), Is.False);
        });
    }

    [Test]
    public void ElementType_IsInnerElementTypeArray()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Resolve("Array(UInt32)").ElementType, Is.EqualTo(typeof(uint[])));
            Assert.That(Resolve("Array(String)").ElementType, Is.EqualTo(typeof(string[])));
            Assert.That(Resolve("Array(Nullable(UInt32))").ElementType, Is.EqualTo(typeof(uint?[])));
            Assert.That(Resolve("Array(Array(UInt8))").ElementType, Is.EqualTo(typeof(byte[][])));
        });
    }

    [Test]
    public void NullPlaceholder_IsEmptyInnerArray()
        => Assert.That(Resolve("Array(UInt32)").NullPlaceholder, Is.EqualTo(Array.Empty<uint>()));

    [Test]
    public async Task ReadColumn_NonMonotonicOffsets_ThrowsProtocol()
    {
        // Offsets must be non-decreasing; a decrease is corruption. Wire: two UInt64 offsets [2, 1].
        IColumnCodec codec = Resolve("Array(UInt32)");
        byte[] wire = new byte[16];
        BitConverter.TryWriteBytes(wire.AsSpan(0, 8), 2UL);
        BitConverter.TryWriteBytes(wire.AsSpan(8, 8), 1UL);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(wire);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () =>
            await codec.ReadColumnAsync(reader, "c", "Array(UInt32)", 2, CodecTestHarness.None));
    }

    [Test]
    public async Task ReadColumn_OffsetBeyondInt32_ThrowsProtocol()
    {
        // An offset larger than int.MaxValue cannot be addressed by this client and is rejected up front.
        IColumnCodec codec = Resolve("Array(UInt32)");
        byte[] wire = new byte[8];
        BitConverter.TryWriteBytes(wire.AsSpan(0, 8), (ulong)int.MaxValue + 1);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(wire);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () =>
            await codec.ReadColumnAsync(reader, "c", "Array(UInt32)", 1, CodecTestHarness.None));
    }

    [Test]
    public async Task Densify_JaggedColumn_ProducesDenseColumnThatRoundTrips()
    {
        // Densify flattens the jagged T[]-per-row form into the dense wire shape: a per-row offsets array plus a
        // single flat inner column holding every element end-to-end. It must surface the same rows and round-trip.
        IColumnCodec codec = Resolve("Array(UInt32)");
        var expected = new[] { new uint[] { 10, 20, 30 }, Array.Empty<uint>(), new uint[] { 40, 50 } };
        var jagged = new ArrayColumn<uint[]>("c", "Array(UInt32)", expected);

        IColumn densified = codec.Densify(jagged);

        Assert.That(densified, Is.InstanceOf<ArrayValueColumn<uint>>());
        var dense = (ArrayValueColumn<uint>)densified;
        Assert.Multiple(() =>
        {
            Assert.That(dense.Offsets.ToArray(), Is.EqualTo(new[] { 0, 3, 3, 5 }));
            Assert.That(dense.Inner.Values.ToArray(), Is.EqualTo(new uint[] { 10, 20, 30, 40, 50 }));
            Assert.That(((IColumn<uint[]>)densified).Values.ToArray(), Is.EqualTo(expected));
        });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, densified, "Array(UInt32)", densified.RowCount);
        Assert.That(((IColumn<uint[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void Densify_NestedNullableInner_DensifiesInnerToo()
    {
        // Array(Nullable(T)): densify flattens the jagged rows AND recurses the inner codec, turning the
        // concatenated T? run into the dense (inner column + null-map) nullable column — the whole tree is dense.
        IColumnCodec codec = Resolve("Array(Nullable(UInt32))");
        var expected = new[] { new uint?[] { 1, null }, new uint?[] { 3 } };
        var jagged = new ArrayColumn<uint?[]>("c", "Array(Nullable(UInt32))", expected);

        var dense = (ArrayValueColumn<uint?>)codec.Densify(jagged);

        Assert.Multiple(() =>
        {
            Assert.That(dense.Offsets.ToArray(), Is.EqualTo(new[] { 0, 2, 3 }));
            Assert.That(dense.Inner, Is.InstanceOf<NullableValueColumn<uint>>());
            Assert.That(((IColumn<uint?[]>)dense).Values.ToArray(), Is.EqualTo(expected));
        });
    }

    [Test]
    public void Densify_AlreadyDenseColumn_ReturnsSameInstance()
    {
        // Idempotent: a dense column whose inner is already dense is returned by reference (nothing to rebuild).
        IColumnCodec codec = Resolve("Array(UInt32)");
        var inner = PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 10, 20, 30, 40, 50 });
        var dense = new ArrayValueColumn<uint>("c", "Array(UInt32)", inner, new[] { 0, 3, 3, 5 }, rowCount: 3, pooledOffsets: false);

        Assert.That(codec.Densify(dense), Is.SameAs(dense));
    }

    [Test]
    public void Resolve_Array_StampsFullTypeName()
        => Assert.That(Resolve("Array(Nullable(String))").TypeName, Is.EqualTo("Array(Nullable(String))"));

    [TestCase("Array(Int32, Int32)")]
    [TestCase("Array()")]
    public void Resolve_WrongArgumentCount_ThrowsFormat(string type)
        => Assert.Throws<FormatException>(() => Resolve(type));

    [Test]
    public void Resolve_UnsupportedInner_ThrowsNotSupported()
        => Assert.Throws<NotSupportedException>(() => Resolve("Array(NoSuchType)"));
}
