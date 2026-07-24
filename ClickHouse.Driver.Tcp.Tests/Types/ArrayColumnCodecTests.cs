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

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, column, 0, 0);
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

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, full, start: 1, length: 2);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Array(UInt32)", 2, CodecTestHarness.None);

        Assert.That(((IColumn<uint[]>)read).Values.ToArray(), Is.EqualTo(new[] { new uint[] { 2, 3 }, Array.Empty<uint>() }));
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
