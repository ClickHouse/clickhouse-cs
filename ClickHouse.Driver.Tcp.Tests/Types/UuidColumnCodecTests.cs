using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class UuidColumnCodecTests
{
    [Test]
    public async Task WriteColumn_ProducesClickHouseByteOrder()
    {
        // ClickHouse stores a UUID as two little-endian 64-bit halves. For 00112233-4455-6677-8899-aabbccddeeff
        // the first half is 0x0011223344556677 (LE) and the second 0x8899aabbccddeeff (LE). An ArrayColumn<Guid> is
        // the storage of UUID, so the codec writes it.
        var guid = new Guid("00112233-4455-6677-8899-aabbccddeeff");
        var column = new ArrayColumn<Guid>("c", "UUID", new[] { guid });

        byte[] bytes = await WriteStoredAsync(UuidColumnCodec.Instance, column, 0, column.RowCount);

        var expected = new byte[]
        {
            0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00, // first half, little-endian
            0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, // second half, little-endian
        };
        CollectionAssert.AreEqual(expected, bytes);
    }
}
