using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
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
    public async Task RoundTrip_MultipleRowsWithEmbeddedNulAndNonUtf8_PreservedAtFixedStride()
    {
        var values = new[]
        {
            new byte[] { 0, 0, 0, 0 },
            new byte[] { (byte)'A', 0x00, (byte)'B', 0xFF },
            new byte[] { 0xFF, 0xFE, 0xFD, 0xFC },
        };

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(Codec(4), new ArrayColumn<byte[]>("c", "FixedString(4)", values), 0, values.Length);
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
        byte[] bytes = await CodecTestHarness.WriteSliceAsync(Codec(2), new ArrayColumn<byte[]>("c", "FixedString(2)", values), 0, values.Length);
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
        using FixedStringColumn dense = Dense(2, new byte[] { 1, 1 }, new byte[] { 2, 2 }, new byte[] { 3, 3 });
        byte[] bytes = await CodecTestHarness.WriteStoredAsync(Codec(2), dense, start: 1, length: 2);

        CollectionAssert.AreEqual(new byte[] { 2, 2, 3, 3 }, bytes);
    }

    [Test]
    public void CanWrite_DecodedColumnOfAnotherWidth_IsFalse()
    {
        // A FixedString(2) read-back is not a body for a FixedString(4) column: its blob holds half the bytes that the
        // header promises. So the codec does not write it, and the insert refuses each row on its width. No server
        // round trip can make this column, so a unit test covers it.
        using FixedStringColumn narrow = Dense(2, new byte[] { 1, 2 }, new byte[] { 3, 4 });
        using FixedStringColumn wide = Dense(4, new byte[] { 1, 2, 3, 4 });

        Assert.Multiple(() =>
        {
            Assert.That(Codec(4).CanWrite(wide), Is.True);
            Assert.That(Codec(4).CanWrite(narrow), Is.False);
        });

        ArgumentException thrown = Assert.ThrowsAsync<ArgumentException>(() => CodecTestHarness.WriteSliceAsync(Codec(4), narrow, 0, narrow.RowCount));
        Assert.That(thrown.Message, Does.Contain("exactly 4 bytes"));
    }

    [Test]
    public void GetBytes_RangeBeyondRowCount_ThrowsArgumentOutOfRange()
    {
        // The blob is rented and typically longer than rowCount * N, so an over-long range must fail fast rather
        // than blit a stale pooled region into the block.
        using FixedStringColumn dense = Dense(2, new byte[] { 1, 2 }, new byte[] { 3, 4 });

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
    public void ReadAs_StringOfAColumnHoldingNoBytes_SaysWhichColumnCannotBeRead()
    {
        // Caller-built columns can carry the type name without exposing FixedString byte storage.
        using var notBytes = new ArrayColumn<int>("c", "FixedString(4)", new[] { 1 });

        var thrown = Assert.Throws<InvalidOperationException>(() => ColumnCodecRegistry.Default.Projections.ReadAs<string>(notBytes, ResolveContext.ForWrite));

        Assert.That(thrown.Message, Does.Contain("'c'").And.Contain("FixedString(4)").And.Contain("does not expose IColumn<Byte[]>"));
    }

    private static IColumnCodec Codec(int size) => ColumnCodecRegistry.Default.Resolve($"FixedString({size})", ResolveContext.ForWrite);

    // The column that a query of FixedString(size) reads: one blob that holds the rows one after the other. The codec
    // writes a range of it in one copy.
    private static FixedStringColumn Dense(int size, params byte[][] values)
        => (FixedStringColumn)DecodedColumns.Of("c", $"FixedString({size})", values);

    private static void Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, ResolveContext.ForWrite);

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));
}
