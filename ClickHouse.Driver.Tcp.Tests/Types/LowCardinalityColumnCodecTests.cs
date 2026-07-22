using System;
using System.Buffers.Binary;
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
    public async Task WriteStatePrefixAndColumn_NonNullableString_ProducesTheDocumentedBytes()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        var column = new ArrayColumn<string>("c", "LowCardinality(String)", new[] { "a", "b", "a", "c", "b" });

        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w);
            codec.WriteColumn(w, column);
        });

        // dict[0] is the reserved empty-string default; 'a','b','c' take slots 1..3; keys index them.
        byte[] expected =
        {
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // state prefix Int64 = 1
            0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // metadata UInt64 = 0x600 (key code 0)
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // dict_size = 4
            0x00,                                           // dict[0] = ""
            0x01, (byte)'a',                                // dict[1] = "a"
            0x01, (byte)'b',                                // dict[2] = "b"
            0x01, (byte)'c',                                // dict[3] = "c"
            0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // keys_count = 5
            0x01, 0x02, 0x01, 0x03, 0x02,                   // keys (UInt8): 1, 2, 1, 3, 2
        };

        CollectionAssert.AreEqual(expected, bytes);
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_StringRoundTrips()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        var expected = new[] { "a", "b", "a", "c", "b" };
        var column = new ArrayColumn<string>("c", "LowCardinality(String)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "LowCardinality(String)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(read.RowCount, Is.EqualTo(5));
            Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(expected));
        });
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_FixedWidthInnerRoundTrips()
    {
        IColumnCodec codec = Resolve("LowCardinality(UInt32)");
        var expected = new uint[] { 7, 7, 42, 7, 42 };
        var column = new ArrayColumn<uint>("c", "LowCardinality(UInt32)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "LowCardinality(UInt32)", column.RowCount);

        Assert.That(((IColumn<uint>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task WriteColumn_ValueEqualToInnerDefault_ReusesTheReservedSlotZero()
    {
        // The reserved dict[0] holds the inner default (""), so an actual empty-string row maps to key 0 rather
        // than adding a duplicate slot — and still round-trips as an empty string.
        IColumnCodec codec = Resolve("LowCardinality(String)");
        var expected = new[] { string.Empty, "a", string.Empty };
        var column = new ArrayColumn<string>("c", "LowCardinality(String)", expected);

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));

        // metadata (8) + dict_size (8) + dict ["" , "a"] (1 + 2) + keys_count (8) + keys [0,1,0] (3) = 30 bytes.
        Assert.That(BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8, 8)), Is.EqualTo(2UL), "dict has the default plus 'a'");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 3, CodecTestHarness.None);
        Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task WriteThenRead_DictionaryPast255Entries_PromotesToUInt16Keys()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        var values = new string[300];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = "v" + i; // 300 distinct values → dict_size 301 → 2-byte keys
        }

        var column = new ArrayColumn<string>("c", "LowCardinality(String)", values);
        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));

        ulong metadata = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(0, 8));
        Assert.That(metadata & 0xFF, Is.EqualTo(1UL), "301 dictionary entries need a 2-byte key");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", values.Length, CodecTestHarness.None);
        Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(values));
    }

    [Test]
    public async Task WriteColumn_EmptySlice_WritesNothing()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        var column = new ArrayColumn<string>("c", "LowCardinality(String)", Array.Empty<string>());

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column, 0, 0));
        Assert.That(bytes, Is.Empty, "a zero-length low-cardinality slice writes no body");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 0, CodecTestHarness.None);
        Assert.That(read.RowCount, Is.Zero);
    }

    [Test]
    public async Task WriteThenReadStatePrefix_RoundTripsTheVersionMarker()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteStatePrefix(w));

        Assert.That(bytes, Is.EqualTo(new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }));

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        Assert.DoesNotThrowAsync(async () => await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None));
    }

    [Test]
    public void ReadStatePrefix_UnknownVersion_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] bytes = { 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }; // version 2

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None));
    }

    [Test]
    public void ReadColumn_GlobalDictionaryBitSet_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] metadata = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(metadata, 0x600 | (1UL << 8)); // NeedGlobalDictionaryBit

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(metadata);
        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 1, CodecTestHarness.None));
    }

    [Test]
    public void ReadColumn_AdditionalKeysBitMissing_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] metadata = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(metadata, 1UL << 10); // NeedUpdateDictionary only, no HasAdditionalKeys

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(metadata);
        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 1, CodecTestHarness.None));
    }

    [Test]
    public void ReadColumn_UnknownKeyWidthCode_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        byte[] metadata = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(metadata, 0x600 | 4); // key code 4 is undefined

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(metadata);
        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 1, CodecTestHarness.None));
    }

    [Test]
    public async Task ReadColumn_KeysCountDisagreesWithBlock_Throws()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        var column = new ArrayColumn<string>("c", "LowCardinality(String)", new[] { "a", "b" });
        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column)); // keys_count = 2

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 3, CodecTestHarness.None));
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
        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 1, CodecTestHarness.None));
    }

    [Test]
    public async Task WriteColumn_DenseLowCardinalityColumn_RoundTripsWithoutRebuilding()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        using var dictionary = new ArrayColumn<string>("c", "String", new[] { string.Empty, "x", "y" });
        using var dense = new LowCardinalityColumn<string>("c", "LowCardinality(String)", dictionary, new[] { 1, 2, 1 }, rowCount: 3, pooledKeys: false);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, dense, "LowCardinality(String)", dense.RowCount);

        Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(new[] { "x", "y", "x" }));
    }

    [Test]
    public void CanWrite_AcceptsInnerElementTypeOnly()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<string>("c", "LowCardinality(String)", Array.Empty<string>())), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<int>("c", "LowCardinality(String)", Array.Empty<int>())), Is.False);
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
    public void NullPlaceholder_DelegatesToInner()
        => Assert.That(Resolve("LowCardinality(String)").NullPlaceholder, Is.EqualTo(string.Empty));

    [Test]
    public async Task WriteThenRead_DictionaryPast65534Entries_PromotesToUInt32Keys()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        var values = new string[65_600]; // > ushort.MaxValue distinct values → dict_size forces 4-byte keys
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = "v" + i;
        }

        var column = new ArrayColumn<string>("c", "LowCardinality(String)", values);
        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));

        Assert.That(BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(0, 8)) & 0xFF, Is.EqualTo(2UL), "more than 65534 entries need a 4-byte key");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", values.Length, CodecTestHarness.None);
        Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(values));
    }

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
        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", 1, CodecTestHarness.None));
    }

    [Test]
    public async Task WriteColumn_FixedStringWithEqualValues_DeduplicatesByContent()
    {
        // byte[] defaults to reference equality; the codec must use structural equality so two distinct-but-equal
        // FixedString values collapse to one dictionary slot (dict = ["", {1,2,3,4}] → dict_size 2), not two.
        IColumnCodec codec = Resolve("LowCardinality(FixedString(4))");
        var column = new ArrayColumn<byte[]>("c", "LowCardinality(FixedString(4))", new[]
        {
            new byte[] { 1, 2, 3, 4 },
            new byte[] { 1, 2, 3, 4 },
            new byte[] { 9, 9, 9, 9 },
        });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));

        Assert.That(BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8, 8)), Is.EqualTo(3UL), "the two equal values share one slot: default + {1,2,3,4} + {9,9,9,9}");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "LowCardinality(FixedString(4))", 3, CodecTestHarness.None);
        Assert.That(((IColumn<byte[]>)read).Values.ToArray(), Is.EqualTo(new[] { new byte[] { 1, 2, 3, 4 }, new byte[] { 1, 2, 3, 4 }, new byte[] { 9, 9, 9, 9 } }));
    }

    [Test]
    public void MeasureRowBytes_DenseColumn_PricesKeyPlusInnerValue()
    {
        IColumnCodec codec = Resolve("LowCardinality(String)");
        using var dictionary = new ArrayColumn<string>("c", "String", new[] { string.Empty, "abc" });
        using var dense = new LowCardinalityColumn<string>("c", "LowCardinality(String)", dictionary, new[] { 1 }, rowCount: 1, pooledKeys: false);

        // 8 bytes for the widest key, plus the inner String's encoding of "abc" (1-byte length prefix + 3 bytes).
        Assert.That(codec.MeasureRowBytes(dense, 0), Is.EqualTo(8 + 1 + 3));
    }

    [Test]
    public void NullPlaceholder_NullableInner_IsNull()
        => Assert.That(Resolve("LowCardinality(Nullable(String))").NullPlaceholder, Is.Null);

    [Test]
    public async Task WriteColumn_NullableString_ProducesTheDocumentedBytes()
    {
        // The dictionary is written as bare String (no null-map); two slots are reserved — dict[0] the NULL marker,
        // dict[1] the "" default — so a NULL row's key is 0 and a present "" reuses slot 1. Values ['a', NULL, '', 'b'].
        IColumnCodec codec = Resolve("LowCardinality(Nullable(String))");
        var column = new ArrayColumn<string>("c", "LowCardinality(Nullable(String))", new[] { "a", null, string.Empty, "b" });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));

        byte[] expected =
        {
            0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // metadata UInt64 = 0x600 (key code 0)
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // dict_size = 4
            0x00,                                           // dict[0] = "" → NULL marker
            0x00,                                           // dict[1] = "" → inner default
            0x01, (byte)'a',                                // dict[2] = "a"
            0x01, (byte)'b',                                // dict[3] = "b"
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // keys_count = 4
            0x02, 0x00, 0x01, 0x03,                         // keys (UInt8): 2, 0, 1, 3
        };

        CollectionAssert.AreEqual(expected, bytes);
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_NullableStringRoundTrips()
    {
        // A present "" must read back as "" (present), not NULL — it points at the reserved default slot 1, not slot 0.
        IColumnCodec codec = Resolve("LowCardinality(Nullable(String))");
        var expected = new[] { "a", null, string.Empty, "b", "a", null };
        var column = new ArrayColumn<string>("c", "LowCardinality(Nullable(String))", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "LowCardinality(Nullable(String))", column.RowCount);

        Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_NullableValueTypeRoundTrips()
    {
        // A value-type inner surfaces uint?; a present 0 (the inner default) round-trips as 0, distinct from NULL.
        IColumnCodec codec = Resolve("LowCardinality(Nullable(UInt32))");
        var expected = new uint?[] { 7, null, 0, 7, null, 42 };
        var column = new ArrayColumn<uint?>("c", "LowCardinality(Nullable(UInt32))", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "LowCardinality(Nullable(UInt32))", column.RowCount);

        Assert.That(((IColumn<uint?>)read).Values.ToArray(), Is.EqualTo(expected));
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

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));

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
        using var dictionary = new ArrayColumn<string>("c", "String", new[] { string.Empty, string.Empty, "x", "y" });
        using var dense = new NullableLowCardinalityReferenceColumn<string>(
            "c", "LowCardinality(Nullable(String))", dictionary, new[] { 2, 0, 3, 1 }, rowCount: 4, pooledKeys: false);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, dense, "LowCardinality(Nullable(String))", dense.RowCount);

        Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(new[] { "x", null, "y", string.Empty }));
    }

    [Test]
    public async Task WriteColumn_NullableValueDenseColumn_RoundTripsAndMeasuresWithoutRebuilding()
    {
        // The value-inner dense column (uint dictionary + keys, key 0 = NULL) is the zero-copy write source and is
        // priced straight off the dictionary. dict [0, 0, 7, 42], keys [2, 0, 3, 1] → [7, NULL, 42, 0].
        IColumnCodec codec = Resolve("LowCardinality(Nullable(UInt32))");
        using var dictionary = PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 0, 0, 7, 42 });
        using var dense = new NullableLowCardinalityValueColumn<uint>(
            "c", "LowCardinality(Nullable(UInt32))", dictionary, new[] { 2, 0, 3, 1 }, rowCount: 4, pooledKeys: false);

        Assert.That(codec.MeasureRowBytes(dense, 0), Is.EqualTo(8 + 4), "8-byte widest key plus the fixed UInt32 width");

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, dense, "LowCardinality(Nullable(UInt32))", dense.RowCount);
        Assert.That(((IColumn<uint?>)read).Values.ToArray(), Is.EqualTo(new uint?[] { 7, null, 42, 0 }));
    }

    [Test]
    public async Task WriteColumn_NullableEmptySlice_WritesNothing()
    {
        IColumnCodec codec = Resolve("LowCardinality(Nullable(String))");
        var column = new ArrayColumn<string>("c", "LowCardinality(Nullable(String))", Array.Empty<string>());

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column, 0, 0));
        Assert.That(bytes, Is.Empty, "a zero-length low-cardinality slice writes no body");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "LowCardinality(Nullable(String))", 0, CodecTestHarness.None);
        Assert.That(read.RowCount, Is.Zero);
    }

    [Test]
    public void CanWrite_NullableInner_AcceptsNullableElementTypeOnly()
    {
        Assert.Multiple(() =>
        {
            IColumnCodec reference = Resolve("LowCardinality(Nullable(String))");
            Assert.That(reference.CanWrite(new ArrayColumn<string>("c", "LowCardinality(Nullable(String))", Array.Empty<string>())), Is.True);

            IColumnCodec value = Resolve("LowCardinality(Nullable(UInt32))");
            Assert.That(value.CanWrite(new ArrayColumn<uint?>("c", "LowCardinality(Nullable(UInt32))", Array.Empty<uint?>())), Is.True);
            Assert.That(value.CanWrite(new ArrayColumn<uint>("c", "LowCardinality(Nullable(UInt32))", Array.Empty<uint>())), Is.False, "the bare (non-nullable) element type is not accepted");
        });
    }
}
