using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class StringColumnCodecTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task WriteColumn_SingleValue_IsVarUIntLengthThenBytes()
    {
        byte[] bytes = await WriteAsync(w => StringColumnCodec.Instance.WriteColumn(w, new ArrayColumn<string>("c", "String", new[] { "hello" })));
        CollectionAssert.AreEqual(new byte[] { 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F }, bytes);
    }

    [Test]
    public async Task RoundTrip_EmptyUnicodeAndEmbeddedNul_Preserved()
    {
        var values = new[] { string.Empty, "hello", "héllo✓", "a\0b", new string('x', 500) };

        byte[] bytes = await WriteAsync(w => StringColumnCodec.Instance.WriteColumn(w, new ArrayColumn<string>("c", "String", values)));
        using var reader = ReaderOver(bytes);
        using var column = (IColumn<string>)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", values.Length, None);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumn()
    {
        using var reader = ReaderOver(Array.Empty<byte>());
        using var column = (IColumn<string>)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", 0, None);
        Assert.That(column.RowCount, Is.EqualTo(0));
    }

    [Test]
    public async Task ReadColumn_NonUtf8Bytes_ExposesRawBytesAndHonoursChosenEncoding()
    {
        // A row that is not valid UTF-8: 'A', 0xFF, 'B'. Wire is the VarUInt length (3) then those bytes.
        byte[] wire = { 0x03, 0x41, 0xFF, 0x42 };
        using var reader = ReaderOver(wire);
        using var column = (StringColumn)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", 1, None);

        Assert.Multiple(() =>
        {
            CollectionAssert.AreEqual(new byte[] { 0x41, 0xFF, 0x42 }, column.GetBytes(0).ToArray());
            Assert.That(column.GetString(0, Encoding.Latin1), Is.EqualTo("AÿB"));
            Assert.That(column[0], Is.EqualTo("A�B")); // the default UTF-8 view replaces the invalid byte
        });
    }

    [Test]
    public async Task ReadColumn_MultipleRows_GetBytesSlicesEachRow()
    {
        var values = new[] { string.Empty, "a", "bcd", "héllo" };

        byte[] bytes = await WriteAsync(w => StringColumnCodec.Instance.WriteColumn(w, new ArrayColumn<string>("c", "String", values)));
        using var reader = ReaderOver(bytes);
        using var column = (StringColumn)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", values.Length, None);

        Assert.Multiple(() =>
        {
            Assert.That(column.GetBytes(0).Length, Is.EqualTo(0));
            Assert.That(column.GetBytes(2).ToArray(), Is.EqualTo(new byte[] { (byte)'b', (byte)'c', (byte)'d' }));
            Assert.That(column.GetString(3, Encoding.UTF8), Is.EqualTo("héllo"));
            CollectionAssert.AreEqual(values, column.Values.ToArray());
        });
    }

    [Test]
    public async Task ReadColumn_IndexOrGetBytesBeyondRowCount_Throws()
    {
        // The read path rents blob/offsets from the pool, so the backing arrays are typically larger than the
        // row count. Access beyond RowCount must still fail fast rather than return a stale pooled slot — both
        // before the UTF-8 cache is built and after it is materialized by touching Values.
        var values = new[] { "a", "bcd" };
        byte[] bytes = await WriteAsync(w => StringColumnCodec.Instance.WriteColumn(w, new ArrayColumn<string>("c", "String", values)));
        using var reader = ReaderOver(bytes);
        using var column = (StringColumn)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", values.Length, None);

        Assert.Multiple(() =>
        {
            Assert.Throws<IndexOutOfRangeException>(() => _ = column.GetBytes(values.Length).Length);
            Assert.Throws<IndexOutOfRangeException>(() => _ = column[values.Length]);
            _ = column.Values.Length; // materialize the cache, then re-check the indexer
            Assert.Throws<IndexOutOfRangeException>(() => _ = column[values.Length]);
        });
    }

    [Test]
    public async Task GetString_NullEncoding_ThrowsArgumentNull()
    {
        byte[] wire = { 0x01, 0x41 };
        using var reader = ReaderOver(wire);
        using var column = (StringColumn)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", 1, None);

        Assert.Throws<ArgumentNullException>(() => column.GetString(0, null));
    }

    [Test]
    public void CanWrite_AcceptsStringOrByteColumn_RejectsOthers()
    {
        IColumnCodec codec = StringColumnCodec.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<string>("c", "String", new[] { "x" })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<byte[]>("c", "String", new[] { new byte[] { 1 } })), Is.True);
            Assert.That(codec.CanWrite(PrimitiveColumn<int>.FromValues("c", "Int32", new[] { 1 })), Is.False);
            Assert.That(codec.CanWriteElementType(typeof(byte[])), Is.True);
            Assert.That(codec.NullPlaceholderAs(typeof(byte[])), Is.EqualTo(Array.Empty<byte>()));
            Assert.Throws<NotSupportedException>(() => codec.NullPlaceholderAs(typeof(int)));
        });
    }

    /// <summary>
    /// Bytes are a write shape String takes directly, but not one it can express as its canonical <c>string</c>,
    /// which is what <c>LowCardinality</c> deduplicates. Saying so here is what makes that combination refuse
    /// before the write rather than fault once the body is under way — the codec accepts the type, the wrapper
    /// does not.
    /// </summary>
    [Test]
    public void CanCanonicalizeWriteType_Bytes_IsRefusedEvenThoughTheyCanBeWritten()
    {
        IColumnCodec codec = StringColumnCodec.Instance;
        IColumnCodec lowCardinality = ColumnCodecRegistry.Default.Resolve("LowCardinality(String)", ResolveContext.ForWrite);
        IColumnCodec nullableLowCardinality = ColumnCodecRegistry.Default.Resolve("LowCardinality(Nullable(String))", ResolveContext.ForWrite);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWriteElementType(typeof(byte[])), Is.True);
            Assert.That(codec.CanCanonicalizeWriteType(typeof(byte[])), Is.False);
            Assert.That(codec.CanCanonicalizeWriteType(typeof(string)), Is.True);

            Assert.That(lowCardinality.CanWriteElementType(typeof(string)), Is.True, "text still goes through");
            Assert.That(lowCardinality.CanWriteElementType(typeof(byte[])), Is.False);
            Assert.That(lowCardinality.CanWrite(new ArrayColumn<byte[]>("c", null, new[] { new byte[] { 0xFF } })), Is.False);
            Assert.That(nullableLowCardinality.CanWriteElementType(typeof(byte[])), Is.False);
            Assert.Throws<NotSupportedException>(() => lowCardinality.NullPlaceholderAs(typeof(byte[])));
        });
    }

    /// <summary>
    /// The layout <see cref="IStringColumn"/> exposes has to be sliced to the rows, not to the pooled buffers the
    /// read path rents — a blob is normally longer than the data, and an offsets array longer than the row count.
    /// </summary>
    [Test]
    public async Task ReadColumn_TheBlobAndOffsets_AreSlicedToTheRowsRatherThanThePooledBuffers()
    {
        var values = new[] { "a", string.Empty, "bcd" };
        byte[] bytes = await WriteAsync(w => StringColumnCodec.Instance.WriteColumn(w, new ArrayColumn<string>("c", "String", values)));
        using var reader = ReaderOver(bytes);
        using var column = (IStringColumn)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", values.Length, None);

        Assert.Multiple(() =>
        {
            Assert.That(column.Offsets.ToArray(), Is.EqualTo(new[] { 0, 1, 1, 4 }), "one entry per row plus the leading 0");
            Assert.That(column.Bytes.ToArray(), Is.EqualTo(new byte[] { (byte)'a', (byte)'b', (byte)'c', (byte)'d' }));
            Assert.That(column.Bytes.Length, Is.EqualTo(column.Offsets[column.RowCount]));
            Assert.That(column.GetBytes(1).Length, Is.EqualTo(0), "an empty row is two equal offsets");
        });
    }

    [Test]
    public async Task WriteColumn_ByteColumn_StoresTheBytesVerbatim()
    {
        // The bytes are not text, so a null row is refused rather than written as anything, and 0xFF 0xFE goes out
        // as itself: routing it through the string surface would spell it U+FFFD U+FFFD instead.
        var rows = new[] { new byte[] { 0x41 }, new byte[] { 0xFF, 0xFE }, Array.Empty<byte>() };

        byte[] bytes = await WriteAsync(w => StringColumnCodec.Instance.WriteColumn(w, new ArrayColumn<byte[]>("c", "String", rows)));
        using var reader = ReaderOver(bytes);
        using var column = (IStringColumn)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", rows.Length, None);

        var withNull = new ArrayColumn<byte[]>("c", "String", new[] { new byte[] { 0x41 }, null });
        ArgumentException thrown = Assert.ThrowsAsync<ArgumentException>(
            () => WriteAsync(w => StringColumnCodec.Instance.WriteColumn(w, withNull)));

        Assert.Multiple(() =>
        {
            Assert.That(bytes, Is.EqualTo(new byte[] { 0x01, 0x41, 0x02, 0xFF, 0xFE, 0x00 }), "each row is a VarUInt length then its bytes");
            Assert.That(column.GetBytes(1).ToArray(), Is.EqualTo(new byte[] { 0xFF, 0xFE }));
            Assert.That(thrown.Message, Does.Contain("null value (at row 1)"));
        });
    }

    /// <summary>
    /// Re-emitting a column this client decoded must write the bytes it read, not the UTF-8 of the text they
    /// decoded to. Otherwise a read followed by an insert — which every dense re-insert does — replaces any byte
    /// UTF-8 cannot spell with U+FFFD, and the row that comes back is not the row that went in.
    /// </summary>
    [Test]
    public async Task WriteColumn_DecodedColumnWithNonUtf8Bytes_ReEmitsTheSameBytes()
    {
        byte[] wire = { 0x02, 0xFF, 0xFE };
        using var reader = ReaderOver(wire);
        using var decoded = (IStringColumn)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", 1, None);

        byte[] reEmitted = await WriteAsync(w => StringColumnCodec.Instance.WriteColumn(w, decoded));

        Assert.Multiple(() =>
        {
            Assert.That(reEmitted, Is.EqualTo(wire));
            Assert.That(decoded[0], Is.EqualTo("��"), "which is what the text surface makes of those bytes");
        });
    }

    private static async Task<byte[]> WriteAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));
}
