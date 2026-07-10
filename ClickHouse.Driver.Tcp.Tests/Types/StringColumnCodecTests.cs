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
    public void CanWrite_AcceptsStringColumn_RejectsOthers()
    {
        Assert.Multiple(() =>
        {
            Assert.That(StringColumnCodec.Instance.CanWrite(new ArrayColumn<string>("c", "String", new[] { "x" })), Is.True);
            Assert.That(StringColumnCodec.Instance.CanWrite(PrimitiveColumn<int>.FromValues("c", "Int32", new[] { 1 })), Is.False);
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
