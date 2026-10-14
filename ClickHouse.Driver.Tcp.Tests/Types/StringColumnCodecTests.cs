using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class StringColumnCodecTests
{
    private static readonly CancellationToken None = CancellationToken.None;

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

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(StringColumnCodec.Instance, new ArrayColumn<string>("c", "String", values), 0, values.Length);
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
        byte[] bytes = await CodecTestHarness.WriteSliceAsync(StringColumnCodec.Instance, new ArrayColumn<string>("c", "String", values), 0, values.Length);
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

    /// <summary>
    /// The layout <see cref="IStringColumn"/> exposes has to be sliced to the rows, not to the pooled buffers the
    /// read path rents — a blob is normally longer than the data, and an offsets array longer than the row count.
    /// </summary>
    [Test]
    public async Task ReadColumn_TheBlobAndOffsets_AreSlicedToTheRowsRatherThanThePooledBuffers()
    {
        var values = new[] { "a", string.Empty, "bcd" };
        byte[] bytes = await CodecTestHarness.WriteSliceAsync(StringColumnCodec.Instance, new ArrayColumn<string>("c", "String", values), 0, values.Length);
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

    /// <summary>
    /// Verifies that dense re-emission preserves invalid UTF-8 bytes.
    /// </summary>
    [Test]
    public async Task WriteColumn_DecodedColumnWithNonUtf8Bytes_ReEmitsTheSameBytes()
    {
        byte[] wire = { 0x02, 0xFF, 0xFE };
        using var reader = ReaderOver(wire);
        using var decoded = (IStringColumn)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", 1, None);

        byte[] reEmitted = await CodecTestHarness.WriteStoredAsync(StringColumnCodec.Instance, decoded, 0, decoded.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(reEmitted, Is.EqualTo(wire));
            Assert.That(decoded[0], Is.EqualTo("��"), "which is what the text surface makes of those bytes");
        });
    }

    /// <summary>
    /// Verifies that byte projection reads decoded storage rather than re-encoding text.
    /// </summary>
    [Test]
    public async Task ReadAs_NonUtf8Column_ReadsTheBytesRatherThanTheDamagedText()
    {
        // Two rows: 'A', 0xFF, 'B', then an empty one.
        byte[] wire = { 0x03, 0x41, 0xFF, 0x42, 0x00 };
        using var reader = ReaderOver(wire);
        using IColumn column = await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", 2, None);

        IColumn<byte[]> bytes = ReadAs<byte[]>(column);

        Assert.Multiple(() =>
        {
            Assert.That(bytes, Is.Not.SameAs(column), "byte[] is not the column's own element type, so this is a converting view");
            Assert.That(bytes[0], Is.EqualTo(new byte[] { 0x41, 0xFF, 0x42 }));
            Assert.That(bytes[1], Is.EqualTo(Array.Empty<byte>()));
            Assert.That(bytes.Values.ToArray(), Is.EqualTo(new[] { new byte[] { 0x41, 0xFF, 0x42 }, Array.Empty<byte>() }));
            Assert.That(((IColumn<string>)column)[0], Is.EqualTo("A�B"), "the reading the bytes exist to avoid");
            Assert.Throws<IndexOutOfRangeException>(() => _ = bytes[2]);
        });
    }

    [Test]
    public async Task ReadAs_ZeroRowColumn_ReadsAsAnEmptyByteColumn()
    {
        using var reader = ReaderOver(Array.Empty<byte>());
        using IColumn column = await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", 0, None);

        IColumn<byte[]> bytes = ReadAs<byte[]>(column);

        Assert.Multiple(() =>
        {
            Assert.That(bytes.RowCount, Is.EqualTo(0));
            Assert.That(bytes.Values.Length, Is.EqualTo(0));
            Assert.Throws<IndexOutOfRangeException>(() => _ = bytes[0]);
        });
    }

    /// <summary>
    /// Verifies the error when a caller-built String column has no raw-byte surface.
    /// </summary>
    [Test]
    public void ReadAs_StringColumnWithoutByteStorage_SaysWhichColumnHasNoBytes()
    {
        var text = new ArrayColumn<string>("c", "String", new[] { "a" });

        var thrown = Assert.Throws<InvalidOperationException>(() => ReadAs<byte[]>(text));

        Assert.That(
            thrown.Message,
            Is.EqualTo(
                $"Column 'c' (String) was read as {typeof(ArrayColumn<string>)}, which does not expose the wire bytes through IStringColumn, " +
                "so its values cannot be read as a byte[]. Only a String column decoded from a server response does."));
    }

    private static IColumn<T> ReadAs<T>(IColumn column)
        => ColumnCodecRegistry.Default.Projections.ReadAs<T>(column, new ResolveContext { ServerTimezone = "UTC" });

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));
}
