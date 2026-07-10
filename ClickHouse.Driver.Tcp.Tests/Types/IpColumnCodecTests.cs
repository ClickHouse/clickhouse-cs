using System;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class IpColumnCodecTests
{
    [Test]
    public async Task IPv4_WriteColumn_IsByteReversed()
    {
        // ClickHouse stores IPv4 as a little-endian UInt32, so 1.2.3.4 goes on the wire as 4,3,2,1.
        byte[] bytes = await WriteAsync(w => IPv4ColumnCodec.Instance.WriteColumn(w, new ArrayColumn<IPAddress>("c", "IPv4", new[] { IPAddress.Parse("1.2.3.4") })));
        CollectionAssert.AreEqual(new byte[] { 4, 3, 2, 1 }, bytes);
    }

    [Test]
    public async Task IPv4_RoundTrip_PreservesAddresses()
    {
        var values = new[] { IPAddress.Parse("0.0.0.0"), IPAddress.Parse("127.0.0.1"), IPAddress.Parse("255.255.255.255") };

        using var column = (IColumn<IPAddress>)await RoundTripAsync(IPv4ColumnCodec.Instance, new ArrayColumn<IPAddress>("c", "IPv4", values), "IPv4", values.Length);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }

    [Test]
    public void IPv4_WriteIPv6Address_Throws()
    {
        var column = new ArrayColumn<IPAddress>("c", "IPv4", new[] { IPAddress.Parse("::1") });
        Assert.ThrowsAsync<ArgumentException>(async () => await WriteAsync(w => IPv4ColumnCodec.Instance.WriteColumn(w, column)));
    }

    [Test]
    public async Task IPv6_RoundTrip_PreservesAddresses()
    {
        var values = new[] { IPAddress.Parse("::"), IPAddress.Parse("::1"), IPAddress.Parse("2001:db8::1") };

        using var column = (IColumn<IPAddress>)await RoundTripAsync(IPv6ColumnCodec.Instance, new ArrayColumn<IPAddress>("c", "IPv6", values), "IPv6", values.Length);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }

    [Test]
    public async Task IPv6_WriteColumn_IsNetworkOrder()
    {
        byte[] bytes = await WriteAsync(w => IPv6ColumnCodec.Instance.WriteColumn(w, new ArrayColumn<IPAddress>("c", "IPv6", new[] { IPAddress.Parse("::1") })));
        var expected = new byte[16];
        expected[15] = 1;
        CollectionAssert.AreEqual(expected, bytes);
    }
}
