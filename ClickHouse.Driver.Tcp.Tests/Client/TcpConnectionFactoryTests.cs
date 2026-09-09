using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
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
    // The name the test server's certificate carries. Not the loopback address the factory dials, so the tests
    // also cover TlsServerName being what the certificate is matched against.
    private const string CertificateName = "clickhouse.factory.test.invalid";

    [OneTimeTearDown]
    public void DeleteTemporaryCertificateFiles() => TestCertificates.DeleteTemporaryFiles();

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

    [Test]
    public void Constructor_TlsCaCertificatePathThatDoesNotExist_Throws()
    {
        // The file is read once here rather than per connect, which is also what makes a typo in the path fail
        // where the caller can act on it instead of on whichever operation happens to dial first.
        var options = new ClickHouseTcpClientOptions
        {
            Host = "127.0.0.1",
            UseTls = true,
            TlsCaCertificatePath = Path.Combine(Path.GetTempPath(), $"absent-ca-{Guid.NewGuid():N}.pem"),
        };

        Assert.Throws<FileNotFoundException>(() => new TcpConnectionFactory(options));
    }

    [Test]
    public void ClickHouseTcpClient_TlsCaCertificatePathThatDoesNotExist_ThrowsAtConstruction()
    {
        // The client builds its pool, and so its factory, eagerly. This is the caller-facing half of the test
        // above: nothing defers the failure to the first query.
        var options = new ClickHouseTcpClientOptions
        {
            Host = "127.0.0.1",
            UseTls = true,
            TlsCaCertificatePath = Path.Combine(Path.GetTempPath(), $"absent-ca-{Guid.NewGuid():N}.pem"),
        };

        Assert.Throws<FileNotFoundException>(() => new ClickHouseTcpClient(options));
    }

    [Test]
    public void ClickHouseTcpClient_TlsCaCertificatePathHoldingOnlyAnIntermediate_ThrowsAtConstruction()
    {
        using X509Certificate2 root = TestCertificates.CreateAuthority();
        using X509Certificate2 intermediate = TestCertificates.CreateIntermediate(root);
        var options = new ClickHouseTcpClientOptions
        {
            Host = "127.0.0.1",
            UseTls = true,
            TlsCaCertificatePath = TestCertificates.WritePemFile(intermediate),
        };

        var thrown = Assert.Throws<ArgumentException>(() => new ClickHouseTcpClient(options));

        Assert.That(thrown.Message, Does.Contain("root certificate"));
    }

    [Test]
    public void Constructor_NoTls_DoesNotThrow()
    {
        // A plaintext factory must not touch any TLS machinery, so constructing one reads no certificate file.
        Assert.DoesNotThrow(() => new TcpConnectionFactory(new ClickHouseTcpClientOptions { Host = "127.0.0.1" }));
    }

    [Test]
    public async Task CreateAsync_UseTlsAgainstATlsServer_HandshakesInsideTheTunnel()
    {
        // The only test that covers the whole wiring — options.UseTls to BuildTlsParameters to the SslStream the
        // native handshake then runs inside. Invert the UseTls test in the factory and every other test still
        // passes, because the rest either build TlsParameters by hand or need a Cloud service.
        using X509Certificate2 authority = TestCertificates.CreateAuthority();
        using X509Certificate2 serverCertificate = TestCertificates.IssueServerCertificate(authority, CertificateName);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        Task server = RunTlsClickHouseServerAsync(listener, serverCertificate);

        try
        {
            var factory = new TcpConnectionFactory(new ClickHouseTcpClientOptions
            {
                Host = "127.0.0.1",
                Port = ((IPEndPoint)listener.LocalEndpoint).Port,
                UseTls = true,

                // The certificate names CertificateName, not the loopback address Host carries.
                TlsServerName = CertificateName,
                TlsCaCertificatePath = TestCertificates.WritePemFile(authority),
                DialTimeout = TimeSpan.FromSeconds(30),
            });

            using ClickHouseTcpConnection connection = await factory.CreateAsync(CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
                Assert.That(connection.Server.ServerName, Is.EqualTo("ClickHouse"));
                Assert.That(connection.Server.Timezone, Is.EqualTo("UTC"), "the Hello was decoded from inside the tunnel, not guessed");
            });
        }
        finally
        {
            await server.ContinueWith(static _ => { }, TaskScheduler.Default);
            listener.Stop();
        }
    }

    [Test]
    public async Task CreateAsync_TlsServerButUseTlsLeftOff_DoesNotReachReady()
    {
        // The negative half: a plaintext client against a TLS port must fail rather than appear to work. Proves
        // the previous test passes because TLS was negotiated, not because the server would take anything.
        using X509Certificate2 authority = TestCertificates.CreateAuthority();
        using X509Certificate2 serverCertificate = TestCertificates.IssueServerCertificate(authority, CertificateName);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        Task server = RunTlsClickHouseServerAsync(listener, serverCertificate);

        try
        {
            var factory = new TcpConnectionFactory(new ClickHouseTcpClientOptions
            {
                Host = "127.0.0.1",
                Port = ((IPEndPoint)listener.LocalEndpoint).Port,
                DialTimeout = TimeSpan.FromSeconds(10),
            });

            Assert.CatchAsync(async () => await factory.CreateAsync(CancellationToken.None));
        }
        finally
        {
            await server.ContinueWith(static _ => { }, TaskScheduler.Default);
            listener.Stop();
        }
    }

    // A ClickHouse server just complete enough to finish a handshake, wrapped in TLS: read the ClientHello, reply
    // with a server Hello, then absorb the Addendum the client sends next.
    private static async Task RunTlsClickHouseServerAsync(TcpListener listener, X509Certificate2 certificate)
    {
        using TcpClient accepted = await listener.AcceptTcpClientAsync();
        using var ssl = new SslStream(accepted.GetStream(), leaveInnerStreamOpen: false);
        await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificate = certificate });

        var buffer = new byte[1024];
        int clientHello = await ssl.ReadAsync(buffer);
        Assert.That(clientHello, Is.GreaterThan(0), "the client must send its Hello inside the tunnel");

        byte[] hello = await FakeConnectionFactory.ServerHelloBytesAsync(CancellationToken.None);
        await ssl.WriteAsync(hello);
        await ssl.FlushAsync();

        // The client writes its Addendum after reading Hello; read it so the client is never blocked on a full
        // send buffer, and so this task ends rather than being torn down mid-write. A short read is fine here —
        // nothing inspects the bytes.
        _ = await ssl.ReadAsync(buffer);
    }
}
