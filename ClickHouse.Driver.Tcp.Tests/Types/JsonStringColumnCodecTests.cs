using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class JsonStringColumnCodecTests
{
    private const string Json = "JSON";

    // Three JSON values as their compact text. Verified against a ClickHouse 26.6 SELECT ... FORMAT Native with
    // output_format_native_write_json_as_string = 1.
    private static readonly byte[] DocumentedBytes =
    {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // state prefix: serialization version = 1 (String)
        0x07, 0x7B, 0x22, 0x61, 0x22, 0x3A, 0x31, 0x7D, // len = 7,  {"a":1}
        0x02, 0x7B, 0x7D,                               // len = 2,  {}
        0x0A, 0x7B, 0x22, 0x62, 0x22, 0x3A, 0x22, 0x68, // len = 10, {"b":"hi"}
        0x69, 0x22, 0x7D,
    };

    private static readonly string[] DocumentedValues = { "{\"a\":1}", "{}", "{\"b\":\"hi\"}" };

    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    [Test]
    public async Task WriteStatePrefixAndColumn_DocumentedExample_ProducesTheDocumentedBytes()
    {
        IColumnCodec codec = Resolve(Json);
        var column = new ArrayColumn<string>("j", Json, DocumentedValues);

        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, column);
            codec.WriteColumn(w, column);
        });

        CollectionAssert.AreEqual(DocumentedBytes, bytes);
    }

    [Test]
    public async Task ReadColumn_DocumentedBytes_ReconstructsTheJsonText()
    {
        IColumnCodec codec = Resolve(Json);

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn column = await codec.ReadColumnAsync(reader, "j", Json, 3, CodecTestHarness.None);

        Assert.That(column.RowCount, Is.EqualTo(3));
        Assert.That(column.GetValue(0), Is.EqualTo("{\"a\":1}"));
        Assert.That(column.GetValue(1), Is.EqualTo("{}"));
        Assert.That(column.GetValue(2), Is.EqualTo("{\"b\":\"hi\"}"));
    }

    // The version heads the prefix precisely so a client that implements one encoding can detect the others rather
    // than mis-read them. 0 = V1, 2 = V2, 3 = FLATTENED, 4 = V3 — all per-path encodings. Which one a server sends
    // when the text setting is off depends on the negotiated revision: at this client's 54460 it is 0, because V2
    // needs 54473.
    [TestCase(0UL)]
    [TestCase(2UL)]
    [TestCase(3UL)]
    [TestCase(4UL)]
    public async Task ReadStatePrefix_APerPathVersion_ThrowsNamingTheSettingThatSelectsText(ulong version)
    {
        IColumnCodec codec = Resolve(Json);
        byte[] bytes = await CodecTestHarness.WriteAsync(w => w.WriteUInt64(version));

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        var exception = Assert.ThrowsAsync<ClickHouseTcpProtocolException>(
            async () => await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None));

        Assert.That(exception.Message, Does.Contain($"version {version}"));
        Assert.That(exception.Message, Does.Contain("output_format_native_write_json_as_string=1"));
    }

    // Every JSON spelling is the same String column on the wire — the typed paths, the max_dynamic_* hints and the
    // SKIP clauses only tell the server how to store the parsed value. The quoted regex is the form most likely to
    // break a naive parse: it holds a comma and a parenthesis that must not split the argument list.
    [TestCase("JSON")]
    [TestCase("JSON(a UInt32, b String)")]
    [TestCase("JSON(max_dynamic_paths=8)")]
    [TestCase("JSON(max_dynamic_types=4, max_dynamic_paths=8)")]
    [TestCase("JSON(a Array(UInt32))")]
    [TestCase("JSON(`b.c` String)")]
    [TestCase("JSON(SKIP z)")]
    [TestCase("JSON(a UInt32, SKIP REGEXP '^tmp(x,y)')")]
    public void Resolve_AnyJsonTypeString_ResolvesToTheTextCodecKeepingTheTypeName(string type)
    {
        IColumnCodec codec = Resolve(type);

        Assert.That(codec.ElementType, Is.EqualTo(typeof(string)));
        Assert.That(codec.TypeName, Is.EqualTo(type));
    }

    [Test]
    public void CanWrite_StringColumn_ReturnsTrue()
    {
        IColumnCodec codec = Resolve(Json);

        Assert.That(codec.CanWrite(new ArrayColumn<string>("j", Json, DocumentedValues)), Is.True);
    }

    [Test]
    public void CanWrite_NonStringColumn_ReturnsFalse()
    {
        IColumnCodec codec = Resolve(Json);

        Assert.That(codec.CanWrite(PrimitiveColumn<uint>.FromValues("j", Json, new uint[] { 1 })), Is.False);
    }

    // Unlike every other codec's placeholder, this one is real input: see the Nullable(JSON) test below.
    [Test]
    public void NullPlaceholder_IsTheEmptyJsonObject()
    {
        IColumnCodec codec = Resolve(Json);

        Assert.That(codec.NullPlaceholder, Is.EqualTo("{}"));
        Assert.That(codec.NullPlaceholderAs(typeof(string)), Is.EqualTo("{}"));
    }

    [Test]
    public void NullPlaceholderAs_UnsupportedWriteType_Throws()
    {
        IColumnCodec codec = Resolve(Json);

        Assert.Throws<NotSupportedException>(() => codec.NullPlaceholderAs(typeof(int)));
    }

    // A JSON value is parsed by the server rather than stored verbatim, and a Nullable column's values stream is
    // parsed at every position — the null ones included. So the placeholder standing in for a NULL row has to be
    // parseable JSON: the empty string a String column would write is rejected as INCORRECT_DATA. Verified against
    // a ClickHouse 26.6 SELECT ... FORMAT Native, which writes "{}" in that position too.
    [Test]
    public async Task WriteStatePrefixAndColumn_NullableJson_WritesAnEmptyObjectWhereTheRowIsNull()
    {
        IColumnCodec codec = Resolve("Nullable(JSON)");
        var column = new ArrayColumn<string>("j", "Nullable(JSON)", new[] { "{\"a\":1}", null });

        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, column);
            codec.WriteColumn(w, column);
        });

        CollectionAssert.AreEqual(
            new byte[]
            {
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // the JSON version, ahead of the null map
                0x00, 0x01,                                     // null map: row 0 present, row 1 NULL
                0x07, 0x7B, 0x22, 0x61, 0x22, 0x3A, 0x31, 0x7D, // row 0: {"a":1}
                0x02, 0x7B, 0x7D,                               // row 1: the "{}" placeholder
            },
            bytes);
    }

    // JSON carries a state prefix, so it must not be treated as a flat leaf inner (ISpanWritableCodec) by a
    // concatenating composite: the version belongs on the wire once, ahead of the array's own offsets. Writing it
    // per row — or not at all — desynchronizes the block instead of failing cleanly. Verified against a
    // ClickHouse 26.6 SELECT ... FORMAT Native.
    [Test]
    public async Task WriteStatePrefixAndColumn_ArrayOfJson_WritesTheVersionOnceAheadOfTheOffsets()
    {
        IColumnCodec codec = Resolve("Array(JSON)");
        var column = new ArrayColumn<string[]>("j", "Array(JSON)", new[]
        {
            new[] { "{\"a\":1}", "{\"b\":\"hi\"}" },
            Array.Empty<string>(),
        });

        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, column);
            codec.WriteColumn(w, column);
        });

        CollectionAssert.AreEqual(
            new byte[]
            {
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // the JSON version, once, before the offsets
                0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offsets[0] = 2
                0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offsets[1] = 2 (second row empty)
                0x07, 0x7B, 0x22, 0x61, 0x22, 0x3A, 0x31, 0x7D, // {"a":1}
                0x0A, 0x7B, 0x22, 0x62, 0x22, 0x3A, 0x22, 0x68, // {"b":"hi"}
                0x69, 0x22, 0x7D,
            },
            bytes);
    }

    // A slice starting past row 0 is what every insert above DefaultMaxRowsPerBlock writes, and a start of 0 proves
    // nothing about it — see AGENTS.local.md. The version is written once for the slice, not once per row, and only
    // the slice's own rows follow it. Covers the ergonomic column; the dense read-back is the next test.
    [Test]
    public async Task WriteStatePrefixAndColumn_SliceAfterEarlierRows_WritesTheVersionOnceAndOnlyTheSliceRows()
    {
        IColumnCodec codec = Resolve(Json);
        var column = new ArrayColumn<string>("j", Json, new[] { "{}", "{\"a\":1}", "{\"b\":\"hi\"}", "{\"c\":2}" });

        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, column, 1, 2);
            codec.WriteColumn(w, column, 1, 2);
        });

        CollectionAssert.AreEqual(
            new byte[]
            {
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // the version, once for the slice
                0x07, 0x7B, 0x22, 0x61, 0x22, 0x3A, 0x31, 0x7D, // row 1: {"a":1}
                0x0A, 0x7B, 0x22, 0x62, 0x22, 0x3A, 0x22, 0x68, // row 2: {"b":"hi"}
                0x69, 0x22, 0x7D,                               // rows 0 and 3 are outside the slice
            },
            bytes);
    }

    // The same slice taken from the dense read-back column, which is a different write source (a StringColumn's
    // blob plus offsets) from the ergonomic string[] above and computes the slice its own way.
    [Test]
    public async Task WriteStatePrefixAndColumn_DenseColumnSliceAfterEarlierRows_WritesOnlyTheSliceRows()
    {
        IColumnCodec codec = Resolve(Json);

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn dense = await codec.ReadColumnAsync(reader, "j", Json, 3, CodecTestHarness.None);

        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, dense, 1, 2);
            codec.WriteColumn(w, dense, 1, 2);
        });

        CollectionAssert.AreEqual(
            new byte[]
            {
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // the version
                0x02, 0x7B, 0x7D,                               // row 1: {}
                0x0A, 0x7B, 0x22, 0x62, 0x22, 0x3A, 0x22, 0x68, // row 2: {"b":"hi"}
                0x69, 0x22, 0x7D,                               // row 0 is outside the slice
            },
            bytes);
    }

    // Slicing an Array(JSON) is where a dropped rebase shows: the wire offsets are cumulative from the start of the
    // block, so a slice must restart them at 0 rather than carry the whole column's running total. Row 0 holds one
    // element, so an unrebased write would emit 3 and 3 here instead of 2 and 2 and mis-frame every row.
    [Test]
    public async Task WriteStatePrefixAndColumn_ArrayOfJsonSlice_RebasesTheOffsetsToTheSliceStart()
    {
        IColumnCodec codec = Resolve("Array(JSON)");
        var column = new ArrayColumn<string[]>("j", "Array(JSON)", new[]
        {
            new[] { "{\"a\":1}" },
            new[] { "{\"b\":\"hi\"}", "{\"c\":2}" },
            Array.Empty<string>(),
        });

        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, column, 1, 2);
            codec.WriteColumn(w, column, 1, 2);
        });

        CollectionAssert.AreEqual(
            new byte[]
            {
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // the JSON version, once, before the offsets
                0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offsets[0] = 2, rebased (not 3)
                0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offsets[1] = 2 (third row empty)
                0x0A, 0x7B, 0x22, 0x62, 0x22, 0x3A, 0x22, 0x68, // {"b":"hi"}
                0x69, 0x22, 0x7D,
                0x07, 0x7B, 0x22, 0x63, 0x22, 0x3A, 0x32, 0x7D, // {"c":2}
            },
            bytes);
    }

    // A zero-row block carries no state prefix at all (the block layer skips the prefix phase), so the codec is
    // asked only for an empty column.
    [Test]
    public async Task ReadColumnAsync_ZeroRows_ReturnsAnEmptyColumn()
    {
        IColumnCodec codec = Resolve(Json);

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(Array.Empty<byte>());
        using IColumn column = await codec.ReadColumnAsync(reader, "j", Json, 0, CodecTestHarness.None);

        Assert.That(column.RowCount, Is.Zero);
    }
}
