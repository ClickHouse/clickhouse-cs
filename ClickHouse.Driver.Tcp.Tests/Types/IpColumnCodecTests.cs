using System;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

// An ArrayColumn<IPAddress> is the storage of IPv4 and IPv6, so the codec writes it.
[TestFixture]
public class IpColumnCodecTests
{
    [Test]
    public async Task IPv4_WriteColumn_IsByteReversed()
    {
        // ClickHouse stores IPv4 as a little-endian UInt32, so 1.2.3.4 goes on the wire as 4,3,2,1.
        var column = new ArrayColumn<IPAddress>("c", "IPv4", new[] { IPAddress.Parse("1.2.3.4") });

        byte[] bytes = await WriteStoredAsync(IPv4ColumnCodec.Instance, column, 0, column.RowCount);

        CollectionAssert.AreEqual(new byte[] { 4, 3, 2, 1 }, bytes);
    }

    [Test]
    public void IPv4_WriteIPv6Address_Throws()
    {
        var column = new ArrayColumn<IPAddress>("c", "IPv4", new[] { IPAddress.Parse("::1") });
        Assert.ThrowsAsync<ArgumentException>(() => WriteStoredAsync(IPv4ColumnCodec.Instance, column, 0, column.RowCount));
    }

    [Test]
    public async Task IPv6_WriteColumn_IsNetworkOrder()
    {
        var column = new ArrayColumn<IPAddress>("c", "IPv6", new[] { IPAddress.Parse("::1") });

        byte[] bytes = await WriteStoredAsync(IPv6ColumnCodec.Instance, column, 0, column.RowCount);

        var expected = new byte[16];
        expected[15] = 1;
        CollectionAssert.AreEqual(expected, bytes);
    }
}
