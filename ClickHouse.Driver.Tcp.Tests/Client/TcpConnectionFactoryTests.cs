using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// DialTimeout has to bound connect *and* handshake, not just connect: a server that accepts the socket and then
// says nothing is exactly the hang the deadline exists for, and no real ClickHouse can be asked to behave that
// way. A listener that accepts and never replies reproduces it with no network beyond loopback.
[TestFixture]
public class TcpConnectionFactoryTests
{
    [Test]
    public async Task CreateAsync_ServerAcceptsButNeverCompletesTheHandshake_ThrowsTimeoutNamingDialTimeout()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        // Accepted and then held open, unanswered, so the client blocks reading the server Hello.
        Task<TcpClient> accepted = listener.AcceptTcpClientAsync();

        try
        {
            var factory = new TcpConnectionFactory(new ClickHouseTcpClientOptions
            {
                Host = "127.0.0.1",
                Port = port,
                DialTimeout = TimeSpan.FromMilliseconds(250),
            });

            var thrown = Assert.ThrowsAsync<TimeoutException>(async () => await factory.CreateAsync(CancellationToken.None));

            Assert.Multiple(() =>
            {
                Assert.That(thrown.Message, Does.Contain("DialTimeout"));
                Assert.That(thrown.Message, Does.Contain($"127.0.0.1:{port}"));
            });
        }
        finally
        {
            listener.Stop();
            if (accepted.IsCompletedSuccessfully)
            {
                (await accepted).Dispose();
            }
        }
    }

    [Test]
    public void CreateAsync_CallerCancels_ReportsCancellationRatherThanATimeout()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        try
        {
            var factory = new TcpConnectionFactory(new ClickHouseTcpClientOptions
            {
                Host = "127.0.0.1",
                Port = port,
                DialTimeout = TimeSpan.FromMinutes(5),
            });

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            // The caller's own cancellation must stay a cancellation: the two share a linked token, and reporting
            // a timeout here would send the caller looking for a network fault they do not have.
            Assert.CatchAsync<OperationCanceledException>(async () => await factory.CreateAsync(cts.Token));
        }
        finally
        {
            listener.Stop();
        }
    }
}
