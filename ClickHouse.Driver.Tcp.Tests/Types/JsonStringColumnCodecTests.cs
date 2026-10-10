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

    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

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

    // A slice that starts after row 0 of the column that a query reads (a StringColumn: one blob and its offsets). The
    // codec writes the version once, then only the rows of the slice.
    [Test]
    public async Task WriteStatePrefixAndColumn_DenseColumnSliceAfterEarlierRows_WritesOnlyTheSliceRows()
    {
        IColumnCodec codec = Resolve(Json);

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn dense = await codec.ReadColumnAsync(reader, "j", Json, 3, CodecTestHarness.None);

        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, dense, 1, 2, prefix: true);

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
