using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Format;

[TestFixture]
public class BlockWriterTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // A revision high enough to include BlockInfo and the has_custom_serialization byte (>= 54454).
    private static readonly NegotiatedProtocol Negotiated = new(54476);

    [Test]
    public async Task WriteEmptyBlock_ProducesNameInfoAndZeroCounts()
    {
        byte[] bytes = await WriteAsync(w => BlockWriter.WriteEmptyBlock(w));

        // empty name, block info (field-tagged), num_columns = 0, num_rows = 0.
        CollectionAssert.AreEqual(new byte[] { 0x00, 0x01, 0x00, 0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00 }, bytes);
    }

    [Test]
    public async Task WriteDataBlockAsync_MultipleColumns_RoundTripsThroughBlockReader()
    {
        IColumn[] columns =
        {
            PrimitiveColumn<int>.FromValues("id", "Int32", new[] { 1, 2, 3 }),
            new ArrayColumn<string>("name", "String", new[] { "a", "bb", "ccc" }),
        };

        byte[] bytes = await WriteAsync(w => BlockWriter.WriteDataBlockAsync(
            w, Negotiated, columns, rowCount: 3, ColumnCodecRegistry.Default, BlockWriter.DefaultFlushThresholdBytes, None));

        using var reader = new ClickHouseBinaryReader(new MemoryStream(bytes));
        using Block block = await BlockReader.ReadBlockAsync(reader, Negotiated, ColumnCodecRegistry.Default, default, None);

        Assert.Multiple(() =>
        {
            Assert.That(block.Name, Is.Empty);
            Assert.That(block.RowCount, Is.EqualTo(3));
            Assert.That(block.ColumnCount, Is.EqualTo(2));
            Assert.That(block[0].Name, Is.EqualTo("id"));
            Assert.That(block[0].TypeName, Is.EqualTo("Int32"));
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, ((IColumn<int>)block[0]).Values.ToArray());
            Assert.That(block[1].Name, Is.EqualTo("name"));
            Assert.That(block[1].TypeName, Is.EqualTo("String"));
            CollectionAssert.AreEqual(new[] { "a", "bb", "ccc" }, ((IColumn<string>)block[1]).Values.ToArray());
        });
    }

    [Test]
    public async Task WriteDataBlockAsync_SubRange_WritesOnlyThoseRows()
    {
        // Two columns of five rows; write the middle three [1, 4) straight from their spans (no slicing).
        var id = PrimitiveColumn<int>.FromValues("id", "Int32", new[] { 10, 20, 30, 40, 50 });
        var name = new ArrayColumn<string>("name", "String", new[] { "a", "bb", "ccc", "dddd", "eeeee" });
        InsertColumn[] columns =
        {
            new("id", "Int32", ColumnCodecRegistry.Default.Resolve("Int32", ResolveContext.ForWrite), id),
            new("name", "String", ColumnCodecRegistry.Default.Resolve("String", ResolveContext.ForWrite), name),
        };

        byte[] bytes = await WriteAsync(w => BlockWriter.WriteDataBlockAsync(
            w, Negotiated, columns, start: 1, rowCount: 3, BlockWriter.DefaultFlushThresholdBytes, None));

        using var reader = new ClickHouseBinaryReader(new MemoryStream(bytes));
        using Block block = await BlockReader.ReadBlockAsync(reader, Negotiated, ColumnCodecRegistry.Default, ResolveContext.ForWrite, None);

        Assert.Multiple(() =>
        {
            Assert.That(block.RowCount, Is.EqualTo(3));
            CollectionAssert.AreEqual(new[] { 20, 30, 40 }, ((IColumn<int>)block[0]).Values.ToArray());
            CollectionAssert.AreEqual(new[] { "bb", "ccc", "dddd" }, ((IColumn<string>)block[1]).Values.ToArray());
        });
    }

    [Test]
    public async Task WriteDataBlockAsync_WhenNegotiatedLacksCustomSerialization_OmitsTheByte()
    {
        var legacy = new NegotiatedProtocol(54440); // below CUSTOM_SERIALIZATION (54454)
        IColumn[] columns = { PrimitiveColumn<byte>.FromValues("c", "UInt8", new byte[] { 7 }) };

        byte[] with = await WriteAsync(w => BlockWriter.WriteDataBlockAsync(w, Negotiated, columns, 1, ColumnCodecRegistry.Default, BlockWriter.DefaultFlushThresholdBytes, None));
        byte[] without = await WriteAsync(w => BlockWriter.WriteDataBlockAsync(w, legacy, columns, 1, ColumnCodecRegistry.Default, BlockWriter.DefaultFlushThresholdBytes, None));

        // The only difference is the single has_custom_serialization byte per column.
        Assert.That(without, Has.Length.EqualTo(with.Length - 1));
    }

    [Test]
    public async Task WriteDataBlockAsync_SmallFlushThreshold_FlushesBetweenColumnsMidBlock()
    {
        IColumn[] columns =
        {
            PrimitiveColumn<int>.FromValues("a", "Int32", new[] { 1, 2 }),
            PrimitiveColumn<int>.FromValues("b", "Int32", new[] { 3, 4 }),
        };

        using var stream = new MemoryStream();
        using var writer = new ClickHouseBinaryWriter(stream);

        // Threshold of 1 forces a flush after every column, so bytes reach the stream before the caller's own
        // message-boundary flush.
        await BlockWriter.WriteDataBlockAsync(writer, Negotiated, columns, rowCount: 2, ColumnCodecRegistry.Default, flushThresholdBytes: 1, None);

        Assert.That(stream.Length, Is.GreaterThan(0), "a small threshold should flush mid-block");
    }

    [Test]
    public async Task WriteDataBlockAsync_DefaultFlushThreshold_BuffersUntilCallerFlush()
    {
        IColumn[] columns = { PrimitiveColumn<int>.FromValues("a", "Int32", new[] { 1, 2 }) };

        using var stream = new MemoryStream();
        using var writer = new ClickHouseBinaryWriter(stream);

        await BlockWriter.WriteDataBlockAsync(writer, Negotiated, columns, rowCount: 2, ColumnCodecRegistry.Default, BlockWriter.DefaultFlushThresholdBytes, None);

        Assert.Multiple(() =>
        {
            Assert.That(stream.Length, Is.EqualTo(0), "a small block should stay buffered until the message-boundary flush");
            Assert.That(writer.BufferedBytes, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task WriteDataBlockAsync_ZeroRowsWithDictionaryBearingColumn_EmitsNoStatePrefix()
    {
        // A dictionary-bearing type (LowCardinality) is the one case where a zero-row block could plausibly still
        // owe a serialization-state prefix, since that prefix is per-column state rather than per-row data. It does
        // not: ClickHouse's own NativeWriter emits the prefix from inside writeData, which it calls only `if (rows)`,
        // and NativeReader likewise skips readData for a zero-row block. So the header block that opens every result
        // set — and the schema block the server sends for an INSERT — carry column names and types but no prefix and
        // no body. Both of our halves are gated the same way; this pins it byte-exactly, because getting it wrong on
        // either side makes the reader consume 8 bytes that were never written, desynchronizing the stream for every
        // column that follows.
        IColumn[] columns = { new ArrayColumn<string>("v", "LowCardinality(String)", Array.Empty<string>()) };

        byte[] bytes = await WriteAsync(w => BlockWriter.WriteDataBlockAsync(
            w, Negotiated, columns, rowCount: 0, ColumnCodecRegistry.Default, BlockWriter.DefaultFlushThresholdBytes, None));

        // Empty block name, block info, num_columns = 1, num_rows = 0, then the column name, the type string and the
        // has_custom_serialization byte — and nothing at all after it.
        var expected = new List<byte> { 0x00, 0x01, 0x00, 0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01, 0x00, 0x01, (byte)'v', 0x16 };
        expected.AddRange(Encoding.UTF8.GetBytes("LowCardinality(String)"));
        expected.Add(0x00);

        CollectionAssert.AreEqual(expected, bytes, "no Int64 version marker may follow the column header");

        // Read it back with a sentinel appended: the block must consume its own bytes and not one more, so the
        // sentinel has to survive intact. An over-reading reader would swallow it as a state prefix.
        const ulong sentinel = 0xDEADBEEFCAFEF00DUL;
        var framed = new List<byte>(bytes);
        framed.AddRange(BitConverter.GetBytes(sentinel));

        using var reader = new ClickHouseBinaryReader(new MemoryStream(framed.ToArray()));
        using Block block = await BlockReader.ReadBlockAsync(reader, Negotiated, ColumnCodecRegistry.Default, default, None);
        ulong trailing = await reader.ReadUInt64Async(None);

        Assert.Multiple(() =>
        {
            Assert.That(block.RowCount, Is.Zero);
            Assert.That(block.ColumnCount, Is.EqualTo(1));
            Assert.That(block[0].TypeName, Is.EqualTo("LowCardinality(String)"));
            Assert.That(block[0].RowCount, Is.Zero);
            Assert.That(trailing, Is.EqualTo(sentinel), "the reader consumed the block exactly, leaving the following bytes untouched");
        });
    }

    [Test]
    public void WriteDataBlockAsync_ColumnRowCountDisagreesWithBlock_Throws()
    {
        IColumn[] columns = { PrimitiveColumn<int>.FromValues("a", "Int32", new[] { 1, 2, 3 }) };

        using var stream = new MemoryStream();
        using var writer = new ClickHouseBinaryWriter(stream);

        Assert.ThrowsAsync<ArgumentException>(async () => await BlockWriter.WriteDataBlockAsync(
            writer, Negotiated, columns, rowCount: 2, ColumnCodecRegistry.Default, BlockWriter.DefaultFlushThresholdBytes, None));
    }

    private static async Task<byte[]> WriteAsync(Func<ClickHouseBinaryWriter, ValueTask> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            await write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    private static async Task<byte[]> WriteAsync(Action<ClickHouseBinaryWriter> write)
        => await WriteAsync(w =>
        {
            write(w);
            return default;
        });
}
