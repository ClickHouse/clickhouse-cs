using System;
using System.Net;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class IPv4Type : ClickHouseType
{
    public override Type FrameworkType => typeof(IPAddress);

    public override object Read(ExtendedBinaryReader reader)
    {
        var ipv4bytes = reader.ReadBytes(4);
        Array.Reverse(ipv4bytes);
        return new IPAddress(ipv4bytes);
    }

    public override string ToString() => "IPv4";

    public override bool CanWrite(object value)
        => value is IPAddress ip && ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork;

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var address4 = value is IPAddress a ? a : IPAddress.Parse((string)value);
        if (address4.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
        {
            throw new ArgumentException($"Expected IPv4, got {address4.AddressFamily}");
        }

        var ipv4bytes = address4.GetAddressBytes();
        Array.Reverse(ipv4bytes);
        writer.Write(ipv4bytes, 0, ipv4bytes.Length);
    }
}
