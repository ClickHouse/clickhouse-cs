using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

[TestFixture]
public class ClientVersionTests
{
    [TestCase("1.5.0+3f2a1c9", 1, 5, 0)]
    [TestCase("1.4.0-rc2+3f2a1c9", 1, 4, 0)]
    [TestCase("2.10.3", 2, 10, 3)]
    [TestCase("1.5", 1, 5, 0)]
    public void Parse_InformationalVersion_ReadsMajorMinorAndPatch(string informationalVersion, int major, int minor, int patch)
    {
        Assert.That(ClientVersion.Parse(informationalVersion), Is.EqualTo((major, minor, patch)));
    }

    // 0.0.0 is what a benchmark build carries, and it is the one version a server refuses for a distributed query.
    [TestCase("0.0.0-baseline")]
    [TestCase("0.0.0-pr.724+3f2a1c9")]
    [TestCase("not a version")]
    [TestCase("1")]
    [TestCase("")]
    [TestCase(null)]
    public void Parse_NoUsableVersion_ReturnsTheFallback(string informationalVersion)
    {
        Assert.That(ClientVersion.Parse(informationalVersion), Is.EqualTo(ClientVersion.Fallback));
    }

    [Test]
    public async Task ClientInfoWrite_ClientVersion_WritesMajorMinorAndPatch()
    {
        // Distinct values, so a field written as a constant or in the wrong slot shows. A dev build reports 1.0.0,
        // whose zero minor and patch cannot show that.
        var client = new ClientMetadata { Username = "default", VersionMajor = 12, VersionMinor = 34, VersionPatch = 56 };
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            ClientInfo.Write(writer, new NegotiatedProtocol(NegotiatedProtocol.ClientTcpProtocolVersion), client);
            await writer.FlushAsync(CancellationToken.None);
        }

        var wire = new WireCursor(ms.ToArray());
        Assert.That(wire.Byte(), Is.EqualTo(1), "query_kind");
        wire.String();                                  // initial_user
        wire.String();                                  // initial_query_id
        wire.String();                                  // initial_address
        wire.Skip(sizeof(long));                        // initial_query_start_time_microseconds
        Assert.That(wire.Byte(), Is.EqualTo(1), "interface");
        wire.String();                                  // os_user
        wire.String();                                  // client_hostname
        Assert.That(wire.String(), Is.EqualTo(ClientHandshakeParameters.DefaultClientName));
        ulong major = wire.VarUInt();
        ulong minor = wire.VarUInt();
        wire.VarUInt();                                 // client_tcp_protocol_version
        wire.String();                                  // quota_key
        wire.VarUInt();                                 // distributed_depth
        ulong patch = wire.VarUInt();

        Assert.That((major, minor, patch), Is.EqualTo((12UL, 34UL, 56UL)));
    }

    [Test]
    public void ClientHandshakeParameters_Defaults_ReportTheDriverVersion()
    {
        var parameters = new ClientHandshakeParameters { Username = "default" };

        Assert.Multiple(() =>
        {
            Assert.That((parameters.VersionMajor, parameters.VersionMinor, parameters.VersionPatch), Is.EqualTo(ClientVersion.Current));
            Assert.That(ClientVersion.Current, Is.Not.EqualTo((0, 0, 0)));
        });
    }

    // Reads the native protocol's primitive encodings from a byte array, in order.
    private sealed class WireCursor(byte[] bytes)
    {
        private int position;

        internal byte Byte() => bytes[position++];

        internal void Skip(int count) => position += count;

        internal ulong VarUInt()
        {
            ulong value = 0;
            for (int shift = 0; ; shift += 7)
            {
                byte b = bytes[position++];
                value |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                {
                    return value;
                }
            }
        }

        internal string String()
        {
            int length = checked((int)VarUInt());
            string value = Encoding.UTF8.GetString(bytes, position, length);
            position += length;
            return value;
        }
    }
}
