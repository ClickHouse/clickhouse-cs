using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Format;

[TestFixture]
public class BlockInfoTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task WriteBlockInfo_Default_ProducesFieldTaggedBytes()
    {
        // field 1 + is_overflows=0, field 2 + bucket=-1 (Int32), terminator 0.
        byte[] bytes = await WriteAsync(w => BlockWriter.WriteBlockInfo(w, BlockInfo.Default));
        CollectionAssert.AreEqual(new byte[] { 0x01, 0x00, 0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0x00 }, bytes);
    }

    [Test]
    public async Task ReadBlockInfoAsync_Default_DecodesStandardValues()
    {
        byte[] bytes = await WriteAsync(w => BlockWriter.WriteBlockInfo(w, BlockInfo.Default));
        using var reader = ReaderOver(bytes);

        BlockInfo info = await BlockReader.ReadBlockInfoAsync(reader, None);

        Assert.Multiple(() =>
        {
            Assert.That(info.IsOverflows, Is.False);
            Assert.That(info.BucketNumber, Is.EqualTo(-1));
        });
    }

    [Test]
    public async Task ReadBlockInfoAsync_CustomValues_RoundTrips()
    {
        byte[] bytes = await WriteAsync(w => BlockWriter.WriteBlockInfo(w, new BlockInfo(isOverflows: true, bucketNumber: 5)));
        using var reader = ReaderOver(bytes);

        BlockInfo info = await BlockReader.ReadBlockInfoAsync(reader, None);

        Assert.Multiple(() =>
        {
            Assert.That(info.IsOverflows, Is.True);
            Assert.That(info.BucketNumber, Is.EqualTo(5));
        });
    }

    [Test]
    public async Task ReadBlockInfoAsync_UnknownFieldId_ThrowsProtocol()
    {
        byte[] bytes = await WriteAsync(w => w.WriteVarUInt(3)); // no field id 3 is defined
        using var reader = ReaderOver(bytes);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await BlockReader.ReadBlockInfoAsync(reader, None));
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
