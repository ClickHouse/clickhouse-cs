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

    // Nothing has no storage that a write can take: the codec does not write even the column that its read gives, and
    // the converter layer refuses every write of the type.
    [Test]
    public async Task CanWrite_DecodedColumn_IsFalseAndTheInsertRefusesTheColumn()
    {
        using var reader = ReaderOver(new byte[] { 0 });
        IColumnCodec codec = NothingColumnCodec.Instance;
        using IColumn decoded = await codec.ReadColumnAsync(reader, "c", "Nothing", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(decoded), Is.False);
            Assert.Throws<InvalidOperationException>(() => InsertWrite(codec, decoded, "Nothing", ResolveContext.ForWrite));
            Assert.ThrowsAsync<NotSupportedException>(() => WriteAsync(w => codec.WriteColumn(w, decoded, 0, 1, null)));
        });
    }
}
