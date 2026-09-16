using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

[TestFixture]
[Category("Integration")]
[Category("Cloud")]
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
        var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(async () =>
            await TcpServerFixture.ConnectAsync(None, password: "definitely-not-the-password"));

        // The server rejects the credentials during the handshake and the failure surfaces as a typed error.
        Assert.That(thrown.RawCode, Is.GreaterThan(0));
    }

    [Test]
    public async Task PingAsync_AfterTerminate_ThrowsObjectDisposed()
    {
        var connection = await TcpServerFixture.ConnectAsync(None);
        connection.Terminate();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await connection.PingAsync(None));
    }

    [Test]
    public void ConnectAsync_ToUnreachablePort_ThrowsTransportExceptionNamingTheEndpointAndKeepingTheSocketError()
    {
        // Loopback rather than the server host, and nothing binds port 1 on either. A closed loopback port is
        // refused at once; a Cloud endpoint drops the packet, leaving the connect on the OS timeout for minutes.
        // What is under test is the client's own behaviour, which does not depend on which server it dials.
        var thrown = Assert.ThrowsAsync<ClickHouseTcpTransportException>(async () =>
            await ClickHouseTcpConnection.ConnectAsync(
                IPAddress.Loopback.ToString(),
                1,
                new ClientHandshakeParameters { Username = "default" },
                tls: null,
                None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain($"{IPAddress.Loopback}:1"));
            Assert.That(thrown.InnerException, Is.InstanceOf<SocketException>());
        });
    }
}
