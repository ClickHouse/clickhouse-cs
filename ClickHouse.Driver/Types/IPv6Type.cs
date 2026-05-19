using System;
using System.Net;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class IPv6Type : ClickHouseType
{
    public override Type FrameworkType => typeof(IPAddress);

    public override object Read(ExtendedBinaryReader reader) => new IPAddress(reader.ReadBytes(16));

    public override string ToString() => "IPv6";

    public override bool CanWrite<T>(T value)
        => value is IPAddress ip && ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6;

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        if (value is not IPAddress address6)
        {
            address6 = IPAddress.Parse((string)(object)value);
        }

        if (address6.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
        {
            throw new ArgumentException($"Expected IPv6, got {address6.AddressFamily}");
        }

        var ipv6bytes = address6.GetAddressBytes();
        writer.Write(ipv6bytes, 0, ipv6bytes.Length);
    }
}
