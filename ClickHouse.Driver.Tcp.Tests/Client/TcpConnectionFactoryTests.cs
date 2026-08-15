using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

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
    public async Task IsReusable_PeerClosedTheSocketWhileIdle_IsFalseOverARealSocket()
    {
        // The branch that matters most and that a scripted stream cannot reach: the socket poll. Every other
        // IsReusable test takes the no-socket path, and a live server never hangs up mid-test on request.
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        Task<TcpClient> accepting = listener.AcceptTcpClientAsync();
        try
        {
            var factory = new TcpConnectionFactory(new ClickHouseTcpClientOptions
            {
                Host = "127.0.0.1",
                Port = port,
                DialTimeout = TimeSpan.FromSeconds(10),
            });

            ValueTask<ClickHouseTcpConnection> connecting = factory.CreateAsync(CancellationToken.None);

            // Play the server's side of the handshake, then leave the connection idle as the pool would.
            using TcpClient server = await accepting;
            byte[] hello = await FakeConnectionFactory.ServerHelloBytesAsync(CancellationToken.None);
            await server.GetStream().WriteAsync(hello);
            await server.GetStream().FlushAsync();

            using ClickHouseTcpConnection connection = await connecting;
            Assert.That(connection.IsReusable, Is.True, "a live idle connection must stay reusable");

            // An orderly close from the far end: the socket becomes readable with nothing to read.
            server.Client.Shutdown(SocketShutdown.Both);
            server.Close();

            // The FIN travels over loopback, but not instantly.
            await WaitUntilAsync(() => !connection.IsReusable);

            Assert.Multiple(() =>
            {
                Assert.That(connection.IsReusable, Is.False, "a connection the peer closed must not be handed out");
                Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready), "the probe reports, it does not terminate");
            });
        }
        finally
        {
            listener.Stop();
        }
    }

    [Test]
    public async Task DisposeAsync_OperationBlockedOnAReadThatNeverArrives_IsFreedByTheAbort()
    {
        // The claim the abort path exists for, and the one a scripted connection cannot make: an operation
        // parked on a read from a server that will never answer is released by disposal rather than left until
        // TCP gives up. A listener that completes the handshake and then goes silent is exactly that server.
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        Task<TcpClient> accepting = listener.AcceptTcpClientAsync();
        TcpClient server = null;
        try
        {
            var options = new ClickHouseTcpClientOptions
            {
                Host = "127.0.0.1",
                Port = port,
                DialTimeout = TimeSpan.FromSeconds(10),
                MaxPoolSize = 1,
                PoolTimeout = TimeSpan.FromMilliseconds(250),
            };
            var pool = new ConnectionPool(options);

            // The rent has to be in flight while the handshake is answered: it blocks reading the Hello.
            ValueTask<IConnectionLease> renting = pool.RentAsync(CancellationToken.None);
            server = await accepting;
            await server.GetStream().WriteAsync(await FakeConnectionFactory.ServerHelloBytesAsync(CancellationToken.None));
            await server.GetStream().FlushAsync();
            IConnectionLease lease = await renting;

            // Sends the query, then blocks reading a reply the listener never sends. The lease is deliberately
            // never disposed, so the drain cannot complete and disposal has to abort this.
            Task query = Task.Run(async () =>
            {
                await foreach (Block block in lease.Connection.QueryAsync("SELECT 1", cancellationToken: CancellationToken.None))
                {
                    Assert.That(block, Is.Not.Null);
                }
            });

            await WaitUntilAsync(() => lease.Connection.State == TcpConnectionState.ReadingResponse);
            Assert.That(query.IsCompleted, Is.False, "the query must still be waiting on the server");

            var elapsed = Stopwatch.StartNew();
            await pool.DisposeAsync();
            elapsed.Stop();

            // Whether the abort really released the read, as opposed to disposal merely giving up on waiting
            // for it. Task.WhenAny rather than WaitAsync, whose own timeout would otherwise look like success.
            Task finished = await Task.WhenAny(query, Task.Delay(TimeSpan.FromSeconds(10)));

            Assert.Multiple(() =>
            {
                Assert.That(finished, Is.SameAs(query), "the abort must release the operation's pending read");
                Assert.That(query.IsFaulted, Is.True, "the operation fails rather than quietly returning nothing");
                Assert.That(elapsed.Elapsed, Is.LessThan(TimeSpan.FromSeconds(10)), "disposal must not wait out the abandoned lease");
                Assert.That(lease.Connection.State, Is.EqualTo(TcpConnectionState.Terminated));
                Assert.That(lease.Connection.IsReusable, Is.False);
            });

            // The operation's own unwinding calls Terminate after the abort already set the state; that is what
            // returns the pooled buffers, and it must not fault or return them a second time.
            Assert.DoesNotThrow(lease.Connection.Terminate);
        }
        finally
        {
            server?.Dispose();
            listener.Stop();
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        for (int attempt = 0; attempt < 100 && !condition(); attempt++)
        {
            await Task.Delay(20);
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
