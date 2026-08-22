using System;
using System.Linq;
using ClickHouse.Driver.Tcp.Compression;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Compression;

/// <summary>
/// Pins which packet bodies are framed. This list is not "every packet that carries a block": <c>Log</c> and
/// <c>ProfileEvents</c> carry blocks and go through the same block reader, yet arrive uncompressed at our
/// protocol target. Adding either here desynchronizes any query against a server with
/// <c>send_logs_level</c> set, so the list is asserted whole rather than case by case.
/// <para>
/// The cases are not a <c>TestCaseSource</c> because <c>ServerPacketType</c> is internal and so cannot appear
/// in a public test method's signature.
/// </para>
/// </summary>
[TestFixture]
public class FramedPacketsTests
{
    [Test]
    public void CarriesFramedBody_TheCompressiblePackets_AreFramed()
    {
        Assert.Multiple(() =>
        {
            Assert.That(FramedPackets.CarriesFramedBody(ServerPacketType.Data), Is.True, "Data");
            Assert.That(FramedPackets.CarriesFramedBody(ServerPacketType.Totals), Is.True, "Totals");
            Assert.That(FramedPackets.CarriesFramedBody(ServerPacketType.Extremes), Is.True, "Extremes");
        });
    }

    [Test]
    public void CarriesFramedBody_TheBlockBearingPacketsTheServerDoesNotCompress_AreNotFramed()
    {
        Assert.Multiple(() =>
        {
            Assert.That(FramedPackets.CarriesFramedBody(ServerPacketType.Log), Is.False, "Log");
            Assert.That(FramedPackets.CarriesFramedBody(ServerPacketType.ProfileEvents), Is.False, "ProfileEvents");
        });
    }

    [Test]
    public void CarriesFramedBody_EveryOtherPacket_IsNotFramed()
    {
        // A packet with no block body cannot be framed, so a new enum member must default to false rather than
        // silently join the framed set.
        ServerPacketType[] framed =
        [
            ServerPacketType.Data,
            ServerPacketType.Totals,
            ServerPacketType.Extremes,
        ];

        Assert.Multiple(() =>
        {
            foreach (ServerPacketType packet in Enum.GetValues<ServerPacketType>().Except(framed))
            {
                Assert.That(FramedPackets.CarriesFramedBody(packet), Is.False, $"{packet} must not be framed");
            }
        });
    }
}
