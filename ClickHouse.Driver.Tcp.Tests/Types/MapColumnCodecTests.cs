using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class MapColumnCodecTests
{
    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    private static KeyValuePair<TKey, TValue>[] Row<TKey, TValue>(params (TKey Key, TValue Value)[] pairs)
    {
        var result = new KeyValuePair<TKey, TValue>[pairs.Length];
        for (int i = 0; i < pairs.Length; i++)
        {
            result[i] = new KeyValuePair<TKey, TValue>(pairs[i].Key, pairs[i].Value);
        }

        return result;
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_FixedWidthKeyAndValueRoundTripsWithEmptyRows()
    {
        IColumnCodec codec = Resolve("Map(UInt8, UInt8)");
        var expected = new[]
        {
            Row<byte, byte>((1, 10), (2, 20)),
            Array.Empty<KeyValuePair<byte, byte>>(),
            Row<byte, byte>((3, 30)),
        };
        var column = new ArrayColumn<KeyValuePair<byte, byte>[]>("c", "Map(UInt8, UInt8)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Map(UInt8, UInt8)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(read.TypeName, Is.EqualTo("Map(UInt8, UInt8)"));
            Assert.That(read.RowCount, Is.EqualTo(3));
            Assert.That(((IColumn<KeyValuePair<byte, byte>[]>)read).Values.ToArray(), Is.EqualTo(expected));
            Assert.That(read.GetValue(1), Is.EqualTo(Array.Empty<KeyValuePair<byte, byte>>()));
        });
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_StringKeyRoundTrips()
    {
        IColumnCodec codec = Resolve("Map(String, UInt32)");
        var expected = new[]
        {
            Row<string, uint>(("a", 1), ("b", 2)),
            Row<string, uint>(("héllo✓", uint.MaxValue)),
        };
        var column = new ArrayColumn<KeyValuePair<string, uint>[]>("c", "Map(String, UInt32)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Map(String, UInt32)", column.RowCount);

        Assert.That(((IColumn<KeyValuePair<string, uint>[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_DuplicateKeysWithinRowArePreserved()
    {
        // A Map surfaces as KeyValuePair<K, V>[] precisely so duplicate keys and pair order round-trip intact —
        // a Dictionary would silently collapse the duplicate. The wire tolerates duplicates within a row.
        IColumnCodec codec = Resolve("Map(String, UInt8)");
        var expected = new[] { Row<string, byte>(("A", 1), ("A", 2), ("B", 3)) };
        var column = new ArrayColumn<KeyValuePair<string, byte>[]>("c", "Map(String, UInt8)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Map(String, UInt8)", column.RowCount);

        Assert.That(((IColumn<KeyValuePair<string, byte>[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_NullableValueRoundTrips()
    {
        // Map keys are non-nullable in ClickHouse and Nullable(Map(...)) is server-rejected, so nullability
        // composes inside the value: Map(K, Nullable(V)).
        IColumnCodec codec = Resolve("Map(String, Nullable(UInt32))");
        var expected = new[]
        {
            Row<string, uint?>(("a", 1), ("b", null)),
            Array.Empty<KeyValuePair<string, uint?>>(),
            Row<string, uint?>(("c", null)),
        };
        var column = new ArrayColumn<KeyValuePair<string, uint?>[]>("c", "Map(String, Nullable(UInt32))", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Map(String, Nullable(UInt32))", column.RowCount);

        Assert.That(((IColumn<KeyValuePair<string, uint?>[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_ArrayValueRoundTrips()
    {
        IColumnCodec codec = Resolve("Map(String, Array(Int32))");
        var expected = new[]
        {
            Row<string, int[]>(("a", new[] { 1, 2, 3 }), ("b", Array.Empty<int>())),
            Row<string, int[]>(("c", new[] { -1 })),
        };
        var column = new ArrayColumn<KeyValuePair<string, int[]>[]>("c", "Map(String, Array(Int32))", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Map(String, Array(Int32))", column.RowCount);

        Assert.That(((IColumn<KeyValuePair<string, int[]>[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_EmptyColumn_ReadsZeroRowsWithoutConsumingBytes()
    {
        IColumnCodec codec = Resolve("Map(String, UInt32)");
        var column = new ArrayColumn<KeyValuePair<string, uint>[]>("c", "Map(String, UInt32)", Array.Empty<KeyValuePair<string, uint>[]>());

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column, 0, 0));
        Assert.That(bytes, Is.Empty, "an empty map column writes no offsets and no streams");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Map(String, UInt32)", 0, CodecTestHarness.None);
        Assert.That(read.RowCount, Is.Zero);
    }

    [Test]
    public async Task ReadColumn_EveryRowEmpty_RoundTripsAsAllEmpty()
    {
        IColumnCodec codec = Resolve("Map(String, UInt32)");
        var expected = new[]
        {
            Array.Empty<KeyValuePair<string, uint>>(),
            Array.Empty<KeyValuePair<string, uint>>(),
        };
        var column = new ArrayColumn<KeyValuePair<string, uint>[]>("c", "Map(String, UInt32)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Map(String, UInt32)", column.RowCount);

        Assert.That(((IColumn<KeyValuePair<string, uint>[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task WriteColumn_DenseMapColumn_RoundTripsWithoutRebuildingStreams()
    {
        // A dense MapColumn<TKey, TValue> (flat key/value columns + offsets, the wire's own layout) is the
        // zero-copy write path — the same shape a read produces. Writing one and reading it back preserves the rows.
        IColumnCodec codec = Resolve("Map(String, UInt32)");
        var keys = new ArrayColumn<string>("c", "String", new[] { "a", "b", "c" });
        var values = PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 1, 2, 3 });
        var dense = new MapColumn<string, uint>("c", "Map(String, UInt32)", keys, values, new[] { 0, 2, 3 }, rowCount: 2, pooledOffsets: false);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, dense, "Map(String, UInt32)", dense.RowCount);

        Assert.That(((IColumn<KeyValuePair<string, uint>[]>)read).Values.ToArray(), Is.EqualTo(new[]
        {
            Row<string, uint>(("a", 1), ("b", 2)),
            Row<string, uint>(("c", 3)),
        }));
    }

    [Test]
    public async Task WriteColumn_SlicedRange_WritesOffsetsRelativeToTheSlice()
    {
        // Writing only rows [1, 3) of a four-row column (the insert splitter's per-block path) must emit offsets
        // relative to that block's own streams, not the full column.
        IColumnCodec codec = Resolve("Map(String, UInt8)");
        var full = new ArrayColumn<KeyValuePair<string, byte>[]>("c", "Map(String, UInt8)", new[]
        {
            Row<string, byte>(("a", 1)),
            Row<string, byte>(("b", 2), ("c", 3)),
            Array.Empty<KeyValuePair<string, byte>>(),
            Row<string, byte>(("d", 4), ("e", 5), ("f", 6)),
        });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, full, start: 1, length: 2));
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Map(String, UInt8)", 2, CodecTestHarness.None);

        Assert.That(((IColumn<KeyValuePair<string, byte>[]>)read).Values.ToArray(), Is.EqualTo(new[]
        {
            Row<string, byte>(("b", 2), ("c", 3)),
            Array.Empty<KeyValuePair<string, byte>>(),
        }));
    }

    [Test]
    public void WriteColumn_NullRow_ThrowsArgumentException()
    {
        // Map(K, V) rows are non-nullable, so a null row is rejected rather than silently written as an empty map.
        IColumnCodec codec = Resolve("Map(String, UInt8)");
        var column = new ArrayColumn<KeyValuePair<string, byte>[]>("c", "Map(String, UInt8)", new[]
        {
            Row<string, byte>(("a", 1)),
            null,
        });

        Assert.ThrowsAsync<ArgumentException>(() => CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column)));
    }

    [Test]
    public void CanWrite_AcceptsOnlyMatchingMapColumn()
    {
        IColumnCodec codec = Resolve("Map(String, UInt32)");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<KeyValuePair<string, uint>[]>("c", "Map(String, UInt32)", new[] { Row<string, uint>(("a", 1)) })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<KeyValuePair<string, int>[]>("c", "Map(String, Int32)", new[] { Row<string, int>(("a", 1)) })), Is.False);
            Assert.That(codec.CanWrite(PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 1 })), Is.False);
        });
    }

    [Test]
    public void ElementType_IsKeyValuePairArray()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Resolve("Map(String, UInt32)").ElementType, Is.EqualTo(typeof(KeyValuePair<string, uint>[])));
            Assert.That(Resolve("Map(UInt8, String)").ElementType, Is.EqualTo(typeof(KeyValuePair<byte, string>[])));
            Assert.That(Resolve("Map(String, Nullable(UInt32))").ElementType, Is.EqualTo(typeof(KeyValuePair<string, uint?>[])));
            Assert.That(Resolve("Map(String, Array(Int32))").ElementType, Is.EqualTo(typeof(KeyValuePair<string, int[]>[])));
        });
    }

    [Test]
    public void NullPlaceholder_IsEmptyPairArray()
        => Assert.That(Resolve("Map(String, UInt32)").NullPlaceholder, Is.EqualTo(Array.Empty<KeyValuePair<string, uint>>()));

    [Test]
    public async Task ReadColumn_NonMonotonicOffsets_ThrowsProtocol()
    {
        // Offsets must be non-decreasing; a decrease is corruption. Wire: two UInt64 offsets [2, 1].
        IColumnCodec codec = Resolve("Map(UInt8, UInt8)");
        byte[] wire = new byte[16];
        BitConverter.TryWriteBytes(wire.AsSpan(0, 8), 2UL);
        BitConverter.TryWriteBytes(wire.AsSpan(8, 8), 1UL);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(wire);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () =>
            await codec.ReadColumnAsync(reader, "c", "Map(UInt8, UInt8)", 2, CodecTestHarness.None));
    }

    [Test]
    public async Task ReadColumn_OffsetBeyondInt32_ThrowsProtocol()
    {
        // An offset larger than int.MaxValue cannot be addressed by this client and is rejected up front.
        IColumnCodec codec = Resolve("Map(UInt8, UInt8)");
        byte[] wire = new byte[8];
        BitConverter.TryWriteBytes(wire.AsSpan(0, 8), (ulong)int.MaxValue + 1);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(wire);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () =>
            await codec.ReadColumnAsync(reader, "c", "Map(UInt8, UInt8)", 1, CodecTestHarness.None));
    }

    [Test]
    public void Resolve_Map_StampsFullTypeName()
        => Assert.That(Resolve("Map(String, Array(UInt32))").TypeName, Is.EqualTo("Map(String, Array(UInt32))"));

    [TestCase("Map(String)")]
    [TestCase("Map(String, UInt32, UInt8)")]
    [TestCase("Map()")]
    public void Resolve_WrongArgumentCount_ThrowsFormat(string type)
        => Assert.Throws<FormatException>(() => Resolve(type));

    [Test]
    public void Resolve_UnsupportedKeyOrValue_ThrowsNotSupported()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<NotSupportedException>(() => Resolve("Map(NoSuchType, UInt32)"));
            Assert.Throws<NotSupportedException>(() => Resolve("Map(String, NoSuchType)"));
        });
    }

    [Test]
    public void RestrictOwnership_DisposesOnlyOwnedChildColumn()
    {
        // The mechanism the partial densify rebuild relies on: after RestrictOwnership, Dispose frees exactly the
        // child (key/value) column flagged owned (the freshly built one) and leaves the borrowed one untouched.
        var ownedKeys = new DisposeSpyColumn<int>("c", "Int32", new[] { 1 });
        var borrowedValues = new DisposeSpyColumn<int[]>("c", "Array(Int32)", new[] { new[] { 2 } });
        var map = new MapColumn<int, int[]>("c", "Map(Int32, Array(Int32))", ownedKeys, borrowedValues, new[] { 0, 1 }, rowCount: 1, pooledOffsets: false);

        map.RestrictOwnership(keysOwned: true, valuesOwned: false);
        map.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(ownedKeys.DisposeCount, Is.EqualTo(1), "the owned (freshly built) column must be disposed exactly once");
            Assert.That(borrowedValues.DisposeCount, Is.EqualTo(0), "the borrowed column must not be disposed");
        });
    }
}
