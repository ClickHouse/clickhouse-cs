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

    // A value of any width other than N is rejected rather than padded or truncated: padding a short value would
    // silently rewrite the caller's data and hide whatever produced the wrong width. This matches the HTTP path's
    // FixedStringType, which requires a byte[] to be exactly N bytes.
    [TestCase(0, TestName = "WriteColumn_EmptyValue_ThrowsArgument")]
    [TestCase(3, TestName = "WriteColumn_ValueShorterThanWidth_ThrowsArgument")]
    [TestCase(7, TestName = "WriteColumn_ValueLongerThanWidth_ThrowsArgument")]
    public async Task WriteColumn_ValueWidthOtherThanN_ThrowsArgument(int valueLength)
    {
        var column = new ArrayColumn<byte[]>("c", "FixedString(6)", new[] { new byte[valueLength] });
        var ex = await CaptureAsync(w => Codec(6).WriteColumn(w, column));

        Assert.That(ex, Is.TypeOf<ArgumentException>());
        Assert.That(ex.Message, Does.Contain("exactly 6 bytes"));
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
    public async Task WriteColumn_DenseColumnSubRange_BlitsOnlyThatRangeOfTheBlob()
    {
        // The dense read-back holds its rows at the wire stride, so the codec blits the range in one copy instead
        // of walking it. The insert path splits a large column into per-block ranges, so a partial range must emit
        // exactly its own rows — a stride slip would show up as neighbouring rows' bytes.
        using var dense = await DenseAsync(2, new byte[] { 1, 1 }, new byte[] { 2, 2 }, new byte[] { 3, 3 });
        byte[] bytes = await WriteAsync(w => Codec(2).WriteColumn(w, dense, start: 1, length: 2));

        CollectionAssert.AreEqual(new byte[] { 2, 2, 3, 3 }, bytes);
    }

    [Test]
    public async Task WriteColumn_DenseColumnOfDifferentWidth_ThrowsArgument()
    {
        // A FixedString(2) read-back is not a valid body for a FixedString(4) column: blitting its blob would emit
        // half the bytes the header promises and corrupt the block. The width guard must send it down the per-row
        // path, which rejects each row on width — a shape no server round-trip can produce, hence the unit test.
        using var dense = await DenseAsync(2, new byte[] { 1, 2 }, new byte[] { 3, 4 });
        var ex = await CaptureAsync(w => Codec(4).WriteColumn(w, dense));

        Assert.That(ex, Is.TypeOf<ArgumentException>());
        Assert.That(ex.Message, Does.Contain("exactly 4 bytes"));
    }

    [Test]
    public async Task GetBytes_RangeBeyondRowCount_ThrowsArgumentOutOfRange()
    {
        // The blob is rented and typically longer than rowCount * N, so an over-long range must fail fast rather
        // than blit a stale pooled region into the block.
        using var dense = await DenseAsync(2, new byte[] { 1, 2 }, new byte[] { 3, 4 });

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => _ = dense.GetBytes(0, 3).Length);
            Assert.Throws<ArgumentOutOfRangeException>(() => _ = dense.GetBytes(1, 2).Length);
            Assert.Throws<ArgumentOutOfRangeException>(() => _ = dense.GetBytes(-1, 1).Length);
            Assert.Throws<ArgumentOutOfRangeException>(() => _ = dense.GetBytes(0, -1).Length);
            CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, dense.GetBytes(0, 2).ToArray());
            Assert.That(dense.GetBytes(2, 0).Length, Is.EqualTo(0));
        });
    }

    [Test]
    public void NullPlaceholder_IsWidthZeroBytes()
    {
        // Nullable substitutes this at a null position, and the values stream must still advance a full row there,
        // so the placeholder has to be exactly N bytes now that a short value is rejected.
        CollectionAssert.AreEqual(new byte[6], (byte[])Codec(6).NullPlaceholder);
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

    // Builds the dense, blob-backed column the read path produces — the shape the codec's bulk-blit write covers —
    // by writing the values and reading them straight back.
    private static async Task<FixedStringColumn> DenseAsync(int size, params byte[][] values)
    {
        string type = $"FixedString({size})";
        byte[] bytes = await WriteAsync(w => Codec(size).WriteColumn(w, new ArrayColumn<byte[]>("c", type, values)));
        using var reader = ReaderOver(bytes);
        return (FixedStringColumn)await Codec(size).ReadColumnAsync(reader, "c", type, values.Length, None);
    }

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
