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
    public async Task ReadColumn_WriteThenRead_DuplicateKeysWithinRowArePreserved()
    {
        // A Map surfaces as KeyValuePair<K, V>[] precisely so duplicate keys and pair order round-trip intact —
        // a Dictionary would silently collapse the duplicate. The wire tolerates duplicates within a row.
        // It also owns the Values coverage for Map: MapColumn.Values materializes the whole jagged cache in one
        // pass, while GetValue -- the only accessor AssertColumnsEqual uses -- goes through the uncached indexer.
        // The empty row is here for the same reason: Values' length == 0 branch is otherwise unreached.
        IColumnCodec codec = Resolve("Map(String, UInt8)");
        var expected = new[] { Row<string, byte>(("A", 1), ("A", 2), ("B", 3)), Array.Empty<KeyValuePair<string, byte>>() };
        var column = new ArrayColumn<KeyValuePair<string, byte>[]>("c", "Map(String, UInt8)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Map(String, UInt8)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(read.RowCount, Is.EqualTo(2));
            Assert.That(((IColumn<KeyValuePair<string, byte>[]>)read).Values.ToArray(), Is.EqualTo(expected));
        });
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

    // The state-aware overloads take the state this codec's own BeginWrite returned, and nothing else. They used to
    // treat null as "the caller has none to share" and rebuild one, which meant a caller that lost or never opened
    // the state still got correct bytes, so the mistake could not be seen.
    [Test]
    public async Task WriteColumn_StateAwareOverloadGivenNoState_ThrowsArgument()
    {
        IColumnCodec codec = Resolve("Map(String, UInt8)");
        var column = new ArrayColumn<KeyValuePair<string, byte>[]>("c", "Map(String, UInt8)", new[]
        {
            Row<string, byte>(("a", 1)),
        });

        ArgumentException thrown = null;
        await CodecTestHarness.WriteAsync(writer =>
            thrown = Assert.Throws<ArgumentException>(() => codec.WriteColumn(writer, column, 0, 1, state: null)));

        Assert.That(thrown.Message, Does.Contain("Map(String, UInt8)"));
    }

    [Test]
    public async Task WriteStatePrefix_StateAwareOverloadGivenNoState_ThrowsArgument()
    {
        IColumnCodec codec = Resolve("Map(String, UInt8)");
        var column = new ArrayColumn<KeyValuePair<string, byte>[]>("c", "Map(String, UInt8)", new[]
        {
            Row<string, byte>(("a", 1)),
        });

        await CodecTestHarness.WriteAsync(writer =>
            Assert.Throws<ArgumentException>(() => codec.WriteStatePrefix(writer, column, 0, 1, state: null)));
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
    public void CanWrite_NestedValueWithoutDenseNamedFieldColumn_ReturnsFalse()
    {
        const string type = "Map(String, Nested(a UInt8))";
        const string nestedType = "Nested(a UInt8)";
        IColumnCodec codec = Resolve(type);

        // Flattening the ergonomic pairs would leave object[][] values, not Nested's named field columns.
        var ergonomic = new ArrayColumn<KeyValuePair<string, object[][]>[]>(
            "c",
            type,
            Array.Empty<KeyValuePair<string, object[][]>[]>());
        var wrongDense = new MapColumn<string, object[][]>(
            "c",
            type,
            new ArrayColumn<string>("c", "String", Array.Empty<string>()),
            new ArrayColumn<object[][]>("c", nestedType, Array.Empty<object[][]>()),
            new[] { 0 },
            rowCount: 0,
            pooledOffsets: false);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(ergonomic), Is.False, "the row-oriented projection has lost the named fields");
            Assert.That(codec.CanWrite(wrongDense), Is.False, "a dense map still needs a real NestedColumn value");
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

        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () =>
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

        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () =>
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
    public void Indexer_RowPastRowCount_ThrowsRatherThanReadingStaleOffsets()
    {
        // The offsets buffer is normally a pooled array longer than the column, and a previous, larger read leaves
        // monotonic offsets behind in the tail. Reading a row past RowCount through the raw buffer would therefore
        // hand back real-looking pairs instead of failing — silent wrong data, not a crash. Here row 0 is the whole
        // column; the 4 is the stale tail a longer read would have left.
        var keys = new ArrayColumn<int>("c", "Int32", new[] { 1, 2, 3, 4 });
        var values = new ArrayColumn<string>("c", "String", new[] { "a", "b", "c", "d" });
        using var map = new MapColumn<int, string>("c", "Map(Int32, String)", keys, values, new[] { 0, 2, 4 }, rowCount: 1, pooledOffsets: false);

        Assert.Multiple(() =>
        {
            Assert.That(map[0], Is.EqualTo(new[] { new KeyValuePair<int, string>(1, "a"), new KeyValuePair<int, string>(2, "b") }));
            Assert.That(() => map[1], Throws.InstanceOf<IndexOutOfRangeException>());
        });
    }

    [Test]
    public void Constructor_OffsetsShorterThanRowCountPlusOne_Throws()
    {
        // rowCount is load-bearing here — the key and value columns are flat and the offsets are pooled — so it is
        // validated rather than derived. One offset per row plus the leading zero is the minimum.
        var keys = new ArrayColumn<int>("c", "Int32", new[] { 1, 2 });
        var values = new ArrayColumn<string>("c", "String", new[] { "a", "b" });

        Assert.That(
            () => new MapColumn<int, string>("c", "Map(Int32, String)", keys, values, new[] { 0, 2 }, rowCount: 2, pooledOffsets: false),
            Throws.ArgumentException.With.Message.Contains("fewer than"));
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
