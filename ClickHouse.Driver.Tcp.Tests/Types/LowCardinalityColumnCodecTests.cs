using System;
using System.Buffers.Binary;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class LowCardinalityColumnCodecTests
{
    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    [Test]
    public async Task Values_MaterializesTheDictionaryBackedCacheAndAgreesWithGetValue()
    {
        // Row values are covered against a real server by the LowCardinality cases in InsertRoundTripCase. What
        // those cannot reach is this accessor: GetValue routes to the indexer, which short-circuits on
        // `cache is not null ? cache[row] : dictionary[keys[row]]`, and AssertColumnsEqual only ever calls
        // GetValue -- so the lazily materialized ArrayPool-rented Values cache runs nowhere else. Reading Values
        // twice pins the cache reuse, and reading GetValue afterwards pins the cached and uncached paths against
        // each other; each test previously picked one or the other and never compared them.
        IColumnCodec codec = Resolve("LowCardinality(String)");
        var expected = new[] { "a", "b", "a", "c", "b" };
        var column = new ArrayColumn<string>("c", "LowCardinality(String)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "LowCardinality(String)", column.RowCount);
        var typed = (IColumn<string>)read;

        Assert.Multiple(() =>
        {
            Assert.That(read.RowCount, Is.EqualTo(5));
            Assert.That(typed.Values.ToArray(), Is.EqualTo(expected), "first access materializes the cache");
            Assert.That(typed.Values.ToArray(), Is.EqualTo(expected), "second access reuses it");
            for (int row = 0; row < expected.Length; row++)
            {
                Assert.That(read.GetValue(row), Is.EqualTo(expected[row]), $"row {row} through the warm cache");
            }
        });
    }

    [Test]
    public async Task WriteThenReadStatePrefix_RoundTripsTheVersionMarker()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        using IColumn column = DecodedColumns.Of("c", "LowCardinality(String)", "a");
        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, column, 0, column.RowCount, prefix: true);

        Assert.That(bytes.AsSpan(0, 8).ToArray(), Is.EqualTo(new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }));

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        Assert.DoesNotThrowAsync(async () => await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None));
    }

    [Test]
    public void ReadStatePrefix_UnknownVersion_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] bytes = { 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }; // version 2

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () => await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None));
    }

    [Test]
    public void ReadColumn_GlobalDictionaryBitSet_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] metadata = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(metadata, 0x600 | (1UL << 8)); // NeedGlobalDictionaryBit

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(metadata);
        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 1, CodecTestHarness.None));
    }

    [Test]
    public void ReadColumn_AdditionalKeysBitMissing_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] metadata = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(metadata, 1UL << 10); // NeedUpdateDictionary only, no HasAdditionalKeys

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(metadata);
        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 1, CodecTestHarness.None));
    }

    [Test]
    public void ReadColumn_UnknownKeyWidthCode_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] metadata = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(metadata, 0x600 | 4); // key code 4 is undefined

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(metadata);
        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 1, CodecTestHarness.None));
    }

    [Test]
    public async Task ReadColumn_KeysCountDisagreesWithBlock_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        var column = new ArrayColumn<string>("c", "LowCardinality(String)", new[] { "a", "b" });
        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, column, 0, column.RowCount); // keys_count = 2

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 3, CodecTestHarness.None));
    }

    [Test]
    public async Task ReadColumn_KeyOutsideDictionary_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        // metadata 0x600 (code 0), dict_size 2, dict ["", "a"], keys_count 1, key = 5 (out of range).
        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            w.WriteUInt64(0x600);
            w.WriteUInt64(2);
            w.WriteString(string.Empty);
            w.WriteString("a");
            w.WriteUInt64(1);
            w.WriteUInt8(5);
        });

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 1, CodecTestHarness.None));
    }

    [Test]
    public async Task WriteColumn_DenseLowCardinalityColumn_RoundTripsWithoutRebuilding()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        var dictionary = (IColumn<string>)DecodedColumns.Of("c", "String", string.Empty, "x", "y");
        using var dense = new LowCardinalityColumn<string>("c", "LowCardinality(String)", dictionary, new[] { 1, 2, 1 }, rowCount: 3, pooledKeys: false);

        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, dense, 0, dense.RowCount, prefix: true);
        using IColumn read = await ReadWithPrefixAsync(codec, bytes, "LowCardinality(String)", dense.RowCount);

        Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(new[] { "x", "y", "x" }));
    }

    [Test]
    public void CanWrite_DictionaryThatTheInnerCodecDoesNotWrite_IsFalse()
    {
        // The codec writes the dictionary through the String codec, which writes a decoded String column only.
        IColumnCodec codec = Resolve("LowCardinality(String)");
        using IColumn decoded = DecodedColumns.Of("c", "LowCardinality(String)", "a", "b");
        using var callerDictionary = new LowCardinalityColumn<string>(
            "c", "LowCardinality(String)", new ArrayColumn<string>("c", "String", new[] { string.Empty, "a" }), new[] { 1 }, rowCount: 1, pooledKeys: false);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(decoded), Is.True);
            Assert.That(codec.CanWrite(callerDictionary), Is.False, "the dictionary is an ArrayColumn<string>");
        });
    }

    [TestCase("LowCardinality(UInt8, String)")]
    [TestCase("LowCardinality()")]
    [TestCase("LowCardinality(Nullable(Nullable(String)))")]
    public void Create_WrongArgumentCount_ThrowsFormat(string type)
        => Assert.Throws<FormatException>(() => Resolve(type));

    [Test]
    public void Create_NullableInner_ResolvesToNullableSurfaceType()
        => Assert.Multiple(() =>
        {
            Assert.That(Resolve("LowCardinality(Nullable(String))").ElementType, Is.EqualTo(typeof(string)));
            Assert.That(Resolve("LowCardinality(Nullable(UInt32))").ElementType, Is.EqualTo(typeof(uint?)));
        });

    [Test]
    public async Task ReadColumn_UInt64Keys_Decodes()
    {
        // A crafted stream with key-width code 3 (8-byte keys); the writer never selects this width, but a decoder
        // must handle it. dict ["", "a", "b"], keys [2, 1] → ["b", "a"].
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            w.WriteUInt64(0x600 | 3);
            w.WriteUInt64(3);
            w.WriteString(string.Empty);
            w.WriteString("a");
            w.WriteString("b");
            w.WriteUInt64(2);
            w.WriteUInt64(2);
            w.WriteUInt64(1);
        });

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 2, CodecTestHarness.None);
        Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(new[] { "b", "a" }));
    }

    [Test]
    public void ReadColumn_DictionarySizeExceedsInt32_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] bytes = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(0, 8), 0x600); // valid metadata
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(8, 8), (ulong)int.MaxValue + 1); // dict_size overflows int

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 1, CodecTestHarness.None));
    }

    [Test]
    public async Task WriteColumn_FloatValuesEqualByClrButDifferentOnWire_KeepsBothBitPatterns()
    {
        const string type = "LowCardinality(Float32)";
        IColumnCodec codec = Resolve(type);
        var column = new ArrayColumn<float>("c", type, new[] { 0f, -0f });

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, column, 0, column.RowCount);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", type, 2, CodecTestHarness.None);
        var lowCardinality = (ILowCardinalityColumn<float>)read;
        var typed = (IColumn<float>)read;

        Assert.Multiple(() =>
        {
            Assert.That(lowCardinality.Dictionary.RowCount, Is.EqualTo(2));
            Assert.That(BitConverter.SingleToInt32Bits(typed[0]), Is.EqualTo(BitConverter.SingleToInt32Bits(0f)));
            Assert.That(BitConverter.SingleToInt32Bits(typed[1]), Is.EqualTo(BitConverter.SingleToInt32Bits(-0f)));
        });
    }

    [Test]
    public async Task WriteColumn_NullableIPv4WithRepeatedAndDefaultValues_PreservesNullAndDeduplicates()
    {
        const string type = "LowCardinality(Nullable(IPv4))";
        IColumnCodec codec = Resolve(type);
        IPAddress address = IPAddress.Parse("192.0.2.1");
        var expected = new[] { IPAddress.Any, address, null, address };
        var column = new ArrayColumn<IPAddress>("c", type, expected);

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, column, 0, column.RowCount);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", type, expected.Length, CodecTestHarness.None);
        var lowCardinality = (ILowCardinalityColumn<IPAddress>)read;

        Assert.Multiple(() =>
        {
            Assert.That(lowCardinality.Dictionary.RowCount, Is.EqualTo(3));
            Assert.That(((IColumn<IPAddress>)read).Values.ToArray(), Is.EqualTo(expected));
        });
    }

    [Test]
    public async Task WriteThenRead_NullableFixedString_DeduplicatesByContentAndKeepsNulls()
    {
        IColumnCodec codec = Resolve("LowCardinality(Nullable(FixedString(4)))");
        var expected = new[]
        {
            new byte[] { 1, 2, 3, 4 },
            null,
            new byte[] { 1, 2, 3, 4 },
            new byte[] { 9, 9, 9, 9 },
        };
        var column = new ArrayColumn<byte[]>("c", "LowCardinality(Nullable(FixedString(4)))", expected);

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, column, 0, column.RowCount);

        // dict = [NULL, default(0000), {1,2,3,4}, {9,9,9,9}] → the two equal values collapse to one slot: dict_size 4.
        Assert.That(BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8, 8)), Is.EqualTo(4UL));

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "LowCardinality(Nullable(FixedString(4)))", expected.Length, CodecTestHarness.None);
        Assert.That(((IColumn<byte[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task WriteColumn_NullableDenseColumn_RoundTripsWithoutRebuilding()
    {
        // A dense nullable column (dictionary + keys, key 0 = NULL) is the wire's own layout and re-emits directly.
        IColumnCodec codec = Resolve("LowCardinality(Nullable(String))");
        var dictionary = (IColumn<string>)DecodedColumns.Of("c", "String", string.Empty, string.Empty, "x", "y");
        using var dense = new NullableLowCardinalityReferenceColumn<string>(
            "c", "LowCardinality(Nullable(String))", dictionary, new[] { 2, 0, 3, 1 }, rowCount: 4, pooledKeys: false);

        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, dense, 0, dense.RowCount, prefix: true);
        using IColumn read = await ReadWithPrefixAsync(codec, bytes, "LowCardinality(Nullable(String))", dense.RowCount);

        Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(new[] { "x", null, "y", string.Empty }));
    }

    [Test]
    public async Task WriteColumn_NullableValueDenseColumn_RoundTripsWithoutRebuilding()
    {
        // The value-inner dense column (uint dictionary + keys, key 0 = NULL) is the zero-copy write source.
        // dict [0, 0, 7, 42], keys [2, 0, 3, 1] → [7, NULL, 42, 0].
        IColumnCodec codec = Resolve("LowCardinality(Nullable(UInt32))");
        using var dictionary = PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 0, 0, 7, 42 });
        using var dense = new NullableLowCardinalityValueColumn<uint>(
            "c", "LowCardinality(Nullable(UInt32))", dictionary, new[] { 2, 0, 3, 1 }, rowCount: 4, pooledKeys: false);

        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, dense, 0, dense.RowCount, prefix: true);
        using IColumn read = await ReadWithPrefixAsync(codec, bytes, "LowCardinality(Nullable(UInt32))", dense.RowCount);
        Assert.That(((IColumn<uint?>)read).Values.ToArray(), Is.EqualTo(new uint?[] { 7, null, 42, 0 }));
    }

    [Test]
    public void Indexer_RowPastRowCount_ThrowsRatherThanReadingStaleKeys()
    {
        // The keys buffer is normally a pooled array longer than the column, and a stale key left in its tail by a
        // previous read is a perfectly valid dictionary index — so reading a row past RowCount through the raw buffer
        // returned a real value from the dictionary instead of failing. Here row 0 is the whole column; the trailing
        // 2 is the stale tail. All three shapes keep their own copy of the indexer, hence all three here.
        using var dictionary = new ArrayColumn<string>("c", "String", new[] { string.Empty, "x", "y" });
        using var bare = new LowCardinalityColumn<string>("c", "LowCardinality(String)", dictionary, new[] { 1, 2 }, rowCount: 1, pooledKeys: false);
        using var nullableReference = new NullableLowCardinalityReferenceColumn<string>(
            "c", "LowCardinality(Nullable(String))", dictionary, new[] { 1, 2 }, rowCount: 1, pooledKeys: false);
        using var valueDictionary = new ArrayColumn<uint>("c", "UInt32", new uint[] { 0, 7, 9 });
        using var nullableValue = new NullableLowCardinalityValueColumn<uint>(
            "c", "LowCardinality(Nullable(UInt32))", valueDictionary, new[] { 1, 2 }, rowCount: 1, pooledKeys: false);

        Assert.Multiple(() =>
        {
            Assert.That(bare[0], Is.EqualTo("x"));
            Assert.That(() => bare[1], Throws.InstanceOf<IndexOutOfRangeException>());
            Assert.That(nullableReference[0], Is.EqualTo("x"));
            Assert.That(() => nullableReference[1], Throws.InstanceOf<IndexOutOfRangeException>());
            Assert.That(nullableValue[0], Is.EqualTo(7u));
            Assert.That(() => nullableValue[1], Throws.InstanceOf<IndexOutOfRangeException>());
        });
    }

    [Test]
    public void Constructor_KeysShorterThanRowCount_Throws()
    {
        // rowCount is load-bearing here — the dictionary holds one entry per distinct value, and the keys are pooled
        // — so it is validated rather than derived.
        using var dictionary = new ArrayColumn<string>("c", "String", new[] { string.Empty, "x" });

        Assert.That(
            () => new LowCardinalityColumn<string>("c", "LowCardinality(String)", dictionary, new[] { 1 }, rowCount: 2, pooledKeys: false),
            Throws.ArgumentException.With.Message.Contains("fewer than"));
    }

    [Test]
    public void CanWrite_NullableInnerAndDecodedColumnWithoutTheNullSlot_IsFalse()
    {
        // The dictionary of LowCardinality(T) has one reserved slot. LowCardinality(Nullable(T)) reads key 0 as NULL,
        // so its codec writes only a decoded column of its own type.
        IColumnCodec reference = Resolve("LowCardinality(Nullable(String))");
        IColumnCodec value = Resolve("LowCardinality(Nullable(UInt32))");
        using IColumn nullableStrings = DecodedColumns.Of("c", "LowCardinality(Nullable(String))", "a", null);
        using IColumn strings = DecodedColumns.Of("c", "LowCardinality(String)", "a");
        using IColumn nullableNumbers = DecodedColumns.Of("c", "LowCardinality(Nullable(UInt32))", new uint?[] { 7, null });
        using IColumn numbers = DecodedColumns.Of("c", "LowCardinality(UInt32)", new uint[] { 7 });

        Assert.Multiple(() =>
        {
            Assert.That(reference.CanWrite(nullableStrings), Is.True);
            Assert.That(reference.CanWrite(strings), Is.False, "a decoded LowCardinality(String) column has no NULL slot");
            Assert.That(value.CanWrite(nullableNumbers), Is.True);
            Assert.That(value.CanWrite(numbers), Is.False, "a decoded LowCardinality(UInt32) column has no NULL slot");
        });
    }

    [Test]
    public void Indexer_RowsSharingADictionaryEntry_GetTheSameInstance()
    {
        // The indexer reads the dictionary through its Values, not its own indexer, so an entry that is built on
        // access is built once for the block instead of once per row. A String dictionary is the case that matters:
        // its indexer decodes UTF-8 per call, so reading a 65,536-row column row by row would otherwise allocate a
        // string per row for a dictionary holding a handful of them.
        using var dictionary = new StringColumn("c", "String", "alphabeta"u8.ToArray(), new[] { 0, 0, 5, 9 }, rowCount: 3, pooled: false);
        using var column = new LowCardinalityColumn<string>("c", "LowCardinality(String)", dictionary, new[] { 1, 2, 1 }, rowCount: 3, pooledKeys: false);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo("alpha"));
            Assert.That(column[2], Is.EqualTo("alpha"));
            Assert.That(column[0], Is.SameAs(column[2]), "both rows hold the same dictionary entry");
            Assert.That(column[1], Is.EqualTo("beta"));
        });
    }

    [Test]
    public void Indexer_MutableElementTypeSharingADictionaryEntry_AliasesRatherThanCopies()
    {
        // The consequence of the above for the one element type a caller can mutate: the byte[] of a
        // LowCardinality(FixedString(N)) is shared, so writing through one row's array is visible from another.
        // Pinned deliberately, matching what IColumn<T>.Values has always done for this shape; a reader that needs
        // its own copy has to make one.
        using var dictionary = new FixedStringColumn("c", "FixedString(2)", 2, "\0\0aabb"u8.ToArray(), rowCount: 3, pooled: false);
        using var column = new LowCardinalityColumn<byte[]>("c", "LowCardinality(FixedString(2))", dictionary, new[] { 1, 2, 1 }, rowCount: 3, pooledKeys: false);

        Assert.That(column[0], Is.SameAs(column[2]));
    }

    [Test]
    public void NullableReferenceIndexer_RowsSharingADictionaryEntry_GetTheSameInstanceAndKeepNullNull()
    {
        // The nullable-reference shape reads its dictionary the same way, and a round trip asserting equal values
        // would still pass if it regressed to rebuilding an entry per row. Key 0 is the NULL marker here (two
        // reserved slots), so the null has to survive the sharing.
        using var dictionary = new StringColumn("c", "String", "alphabeta"u8.ToArray(), new[] { 0, 0, 0, 5, 9 }, rowCount: 4, pooled: false);
        using var column = new NullableLowCardinalityReferenceColumn<string>(
            "c", "LowCardinality(Nullable(String))", dictionary, new[] { 2, 0, 3, 2 }, rowCount: 4, pooledKeys: false);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo("alpha"));
            Assert.That(column[1], Is.Null, "key 0 is the NULL marker");
            Assert.That(column[2], Is.EqualTo("beta"));
            Assert.That(column[3], Is.SameAs(column[0]), "both rows hold the same dictionary entry");
        });
    }

    // Reads the state prefix, then the body of a column of rowCount rows.
    private static async Task<IColumn> ReadWithPrefixAsync(IColumnCodec codec, byte[] bytes, string type, int rowCount)
    {
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        return await codec.ReadColumnAsync(reader, "c", type, rowCount, CodecTestHarness.None);
    }
}
