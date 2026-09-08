using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

[TestFixture]
[Category("Integration")]
public class ClickHouseTcpConnectionIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task ConnectAsync_CompletesHandshakeAndPopulatesServerInfo()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(connection.Protocol.Version, Is.EqualTo(NegotiatedProtocol.ClientTcpProtocolVersion));
            Assert.That(connection.Server.ServerName, Is.EqualTo("ClickHouse"));
            Assert.That(connection.Server.VersionMajor, Is.GreaterThan(0));
            Assert.That(connection.Server.Timezone, Is.Not.Empty);
        });
    }

    [Test]
    public async Task PingAsync_ReturnsAndConnectionStaysReady()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        await connection.PingAsync(None);
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));

        // A connection survives repeated pings — the exchange is self-contained and returns to Ready.
        await connection.PingAsync(None);
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
    }

    [Test]
    public void ConnectAsync_WithWrongPassword_ThrowsServerException()
    {
        var thrown = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
            await TcpServerFixture.ConnectAsync(None, password: "definitely-not-the-password"));

        // The server rejects the credentials during the handshake and the failure surfaces as a typed error.
        Assert.That(thrown.Code, Is.GreaterThan(0));
    }

    [Test]
    public async Task PingAsync_AfterTerminate_ThrowsObjectDisposed()
    {
        var connection = await TcpServerFixture.ConnectAsync(None);
        connection.Terminate();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await connection.PingAsync(None));
    }

    [Test]
    public void ConnectAsync_ToUnreachablePort_ThrowsSocketException()
    {
        // Port 1 is reserved and never accepts native-protocol connections.
        Assert.ThrowsAsync<SocketException>(async () =>
            await ClickHouseTcpConnection.ConnectAsync(
                TcpServerFixture.Host,
                1,
                new ClientHandshakeParameters { ClientName = "test", Username = "default" },
                None));
    }
}
