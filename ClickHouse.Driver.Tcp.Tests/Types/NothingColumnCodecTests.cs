using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class NothingColumnCodecTests
{
    [Test]
    public async Task ReadColumn_ConsumesOneBytePerRow_AndSurfacesNulls()
    {
        // Three placeholder bytes, then a sentinel the reader must still see after the column is read.
        byte[] bytes = await WriteAsync(w =>
        {
            w.WriteByte(0);
            w.WriteByte(0);
            w.WriteByte(0);
            w.WriteByte(0x7F);
        });
        using var reader = ReaderOver(bytes);

        using IColumn column = await NothingColumnCodec.Instance.ReadColumnAsync(reader, "c", "Nothing", 3, None);
        byte sentinel = await reader.ReadByteAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(3));
            Assert.That(column.GetValue(0), Is.Null);
            Assert.That(column.GetValue(2), Is.Null);
            Assert.That(sentinel, Is.EqualTo(0x7F));
        });
    }

    [Test]
    public void FixedRowByteSize_IsOneByte_AndComposesUnderNullable()
    {
        IColumnCodec nullableOfNothing = ColumnCodecRegistry.Default.Resolve("Nullable(Nothing)", default);
        Assert.Multiple(() =>
        {
            // One placeholder byte per row for the bare type; the null-map byte adds one more under Nullable.
            Assert.That(NothingColumnCodec.Instance.FixedRowByteSize, Is.EqualTo(1));
            Assert.That(nullableOfNothing.FixedRowByteSize, Is.EqualTo(2));
        });
    }

    [Test]
    public void CanWrite_IsFalse_AndWriteThrows()
    {
        var column = new ArrayColumn<object>("c", "Nothing", new object[1]);
        Assert.Multiple(() =>
        {
            Assert.That(NothingColumnCodec.Instance.CanWrite(column), Is.False);
            Assert.ThrowsAsync<NotSupportedException>(async () => await WriteAsync(w => NothingColumnCodec.Instance.WriteColumn(w, column)));
        });
    }
}
