using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class FixedStringColumnCodecTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public void Create_MissingOrNonIntegerOrNonPositiveLength_ThrowsFormat()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<FormatException>(() => Resolve("FixedString"));
            Assert.Throws<FormatException>(() => Resolve("FixedString(x)"));
            Assert.Throws<FormatException>(() => Resolve("FixedString(0)"));
            Assert.Throws<FormatException>(() => Resolve("FixedString(-4)"));
            Assert.Throws<FormatException>(() => Resolve("FixedString(4, 5)"));
        });
    }

    [Test]
    public async Task WriteColumn_ExactWidthValue_WritesBytesVerbatim()
    {
        byte[] value = { 0xDE, 0xAD, 0xBE, 0xEF };
        byte[] bytes = await WriteAsync(w => Codec(4).WriteColumn(w, new ArrayColumn<byte[]>("c", "FixedString(4)", new[] { value })));

        CollectionAssert.AreEqual(value, bytes);
    }

    [Test]
    public async Task WriteColumn_ShortAndEmptyValues_RightPadsWithZeros()
    {
        var values = new[] { new byte[] { 1, 2, 3 }, Array.Empty<byte>() };
        byte[] bytes = await WriteAsync(w => Codec(6).WriteColumn(w, new ArrayColumn<byte[]>("c", "FixedString(6)", values)));

        CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, bytes);
    }

    [Test]
    public async Task WriteColumn_WidthLargerThanZeroRun_PadsAcrossMultipleChunks()
    {
        // The write path pads with a 64-byte stack zero-run in a loop; a width well past 64 exercises the
        // multi-chunk path. A three-byte value into FixedString(200) must emit exactly 200 bytes: the value then
        // 197 zeros.
        const int width = 200;
        byte[] value = { 1, 2, 3 };
        byte[] bytes = await WriteAsync(w => Codec(width).WriteColumn(w, new ArrayColumn<byte[]>("c", $"FixedString({width})", new[] { value })));

        byte[] expected = new byte[width];
        value.CopyTo(expected, 0);
        CollectionAssert.AreEqual(expected, bytes);
    }

    [Test]
    public async Task WriteColumn_ValueLongerThanWidth_ThrowsArgument()
    {
        var column = new ArrayColumn<byte[]>("c", "FixedString(2)", new[] { new byte[] { 1, 2, 3 } });
        var ex = await CaptureAsync(w => Codec(2).WriteColumn(w, column));

        Assert.That(ex, Is.TypeOf<ArgumentException>());
    }

    [Test]
    public async Task WriteColumn_NullRow_ThrowsArgument()
    {
        var column = new ArrayColumn<byte[]>("c", "FixedString(4)", new byte[][] { null });
        var ex = await CaptureAsync(w => Codec(4).WriteColumn(w, column));

        Assert.That(ex, Is.TypeOf<ArgumentException>());
    }

    [Test]
    public async Task RoundTrip_MultipleRowsWithEmbeddedNulAndNonUtf8_PreservedAtFixedStride()
    {
        var values = new[]
        {
            new byte[] { 0, 0, 0, 0 },
            new byte[] { (byte)'A', 0x00, (byte)'B', 0xFF },
            new byte[] { 0xFF, 0xFE, 0xFD, 0xFC },
        };

        byte[] bytes = await WriteAsync(w => Codec(4).WriteColumn(w, new ArrayColumn<byte[]>("c", "FixedString(4)", values)));
        using var reader = ReaderOver(bytes);
        using var column = (FixedStringColumn)await Codec(4).ReadColumnAsync(reader, "c", "FixedString(4)", values.Length, None);

        Assert.Multiple(() =>
        {
            CollectionAssert.AreEqual(values[1], column.GetBytes(1).ToArray());
            Assert.That(column.GetString(1, Encoding.Latin1), Is.EqualTo("A\0Bÿ"));
            CollectionAssert.AreEqual(values, column.Values.ToArray());
        });
    }

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumn()
    {
        using var reader = ReaderOver(Array.Empty<byte>());
        using var column = (IColumn<byte[]>)await Codec(4).ReadColumnAsync(reader, "c", "FixedString(4)", 0, None);

        Assert.That(column.RowCount, Is.EqualTo(0));
    }

    [Test]
    public async Task ReadColumn_IndexOrGetBytesBeyondRowCount_Throws()
    {
        // The read path rents the blob from the pool, so it is typically larger than rowCount * N. Access beyond
        // RowCount must still fail fast rather than return a stale pooled slot — both before and after the cache
        // is materialized by touching Values.
        var values = new[] { new byte[] { 1, 2 }, new byte[] { 3, 4 } };
        byte[] bytes = await WriteAsync(w => Codec(2).WriteColumn(w, new ArrayColumn<byte[]>("c", "FixedString(2)", values)));
        using var reader = ReaderOver(bytes);
        using var column = (FixedStringColumn)await Codec(2).ReadColumnAsync(reader, "c", "FixedString(2)", values.Length, None);

        Assert.Multiple(() =>
        {
            Assert.Throws<IndexOutOfRangeException>(() => _ = column.GetBytes(values.Length).Length);
            Assert.Throws<IndexOutOfRangeException>(() => _ = column[values.Length]);
            _ = column.Values.Length; // materialize the cache, then re-check the indexer
            Assert.Throws<IndexOutOfRangeException>(() => _ = column[values.Length]);
        });
    }

    [Test]
    public void CanWrite_AcceptsByteArrayColumn_RejectsOthers()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Codec(4).CanWrite(new ArrayColumn<byte[]>("c", "FixedString(4)", new[] { new byte[4] })), Is.True);
            Assert.That(Codec(4).CanWrite(new ArrayColumn<string>("c", "String", new[] { "x" })), Is.False);
        });
    }

    private static IColumnCodec Codec(int size) => ColumnCodecRegistry.Default.Resolve($"FixedString({size})", ResolveContext.ForWrite);

    private static void Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, ResolveContext.ForWrite);

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

    private static async Task<Exception> CaptureAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using var writer = new ClickHouseBinaryWriter(ms);
        try
        {
            write(writer);
            await writer.FlushAsync(None);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));
}
