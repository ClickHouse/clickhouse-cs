using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

/// <summary>
/// TLS is a transport concern the native protocol never sees, so these run against a bare TLS echo server rather
/// than a ClickHouse server: what needs proving is which server certificates the client accepts and which it
/// refuses, not what it sends once the tunnel is up. A real ClickHouse server over TLS is covered by the Cloud
/// tests, which cannot reach these cases — no cloud service will present a self-signed certificate, one from a
/// private authority, or one issued for the wrong purpose on request.
/// </summary>
[TestFixture]
public class TlsParametersTests
{
    private const string ServerName = "clickhouse.test.invalid";

    private static readonly byte[] Payload = Encoding.UTF8.GetBytes("ping");

    [OneTimeTearDown]
    public void DeleteTemporaryCertificateFiles() => TestCertificates.DeleteTemporaryFiles();

    [Test]
    public async Task WrapAsync_CertificateIssuedByTheConfiguredAuthority_CompletesHandshake()
    {
        using X509Certificate2 authority = TestCertificates.CreateAuthority();
        using X509Certificate2 server = TestCertificates.IssueServerCertificate(authority, ServerName);

        var tls = new TlsParameters
        {
            TargetHost = ServerName,
            CaCertificates = TlsParameters.LoadCaCertificates(TestCertificates.WritePemFile(authority)),
        };

        byte[] echoed = await RoundTripThroughTlsAsync(server, tls);

        Assert.That(echoed, Is.EqualTo(Payload));
    }

    [Test]
    public void WrapAsync_SelfSignedCertificateAndValidationLeftOn_ThrowsAuthenticationException()
    {
        using X509Certificate2 server = TestCertificates.CreateSelfSignedServerCertificate(ServerName);

        var tls = new TlsParameters { TargetHost = ServerName };

        Assert.ThrowsAsync<AuthenticationException>(async () => await RoundTripThroughTlsAsync(server, tls));
    }

    [Test]
    public async Task WrapAsync_SelfSignedCertificateAndValidationTurnedOff_CompletesHandshake()
    {
        using X509Certificate2 server = TestCertificates.CreateSelfSignedServerCertificate(ServerName);

        var tls = new TlsParameters { TargetHost = ServerName, AllowInvalidCertificates = true };

        byte[] echoed = await RoundTripThroughTlsAsync(server, tls);

        Assert.That(echoed, Is.EqualTo(Payload));
    }

    [Test]
    public void WrapAsync_AuthorityTrustedButCertificateNamesAnotherHost_ThrowsAuthenticationException()
    {
        // The custom-root path re-takes the trust decision only. Naming a private authority says who to trust,
        // not that the server's identity stopped mattering, so a name mismatch must still fail.
        using X509Certificate2 authority = TestCertificates.CreateAuthority();
        using X509Certificate2 server = TestCertificates.IssueServerCertificate(authority, ServerName);

        var tls = new TlsParameters
        {
            TargetHost = "someone.else.invalid",
            CaCertificates = TlsParameters.LoadCaCertificates(TestCertificates.WritePemFile(authority)),
        };

        Assert.ThrowsAsync<AuthenticationException>(async () => await RoundTripThroughTlsAsync(server, tls));
    }

    [Test]
    public void WrapAsync_CertificateFromAnUnrelatedAuthority_ThrowsAuthenticationException()
    {
        // Trusting one private authority must not trust every private authority.
        using X509Certificate2 trusted = TestCertificates.CreateAuthority();
        using X509Certificate2 other = TestCertificates.CreateAuthority();
        using X509Certificate2 server = TestCertificates.IssueServerCertificate(other, ServerName);

        var tls = new TlsParameters
        {
            TargetHost = ServerName,
            CaCertificates = TlsParameters.LoadCaCertificates(TestCertificates.WritePemFile(trusted)),
        };

        Assert.ThrowsAsync<AuthenticationException>(async () => await RoundTripThroughTlsAsync(server, tls));
    }

    [Test]
    public void WrapAsync_CertificateUnderTheAuthorityButNotValidForServerAuthentication_ThrowsAuthenticationException()
    {
        // The rebuild replaces the platform's chain check, so it has to keep the platform's requirement that the
        // certificate is valid for server authentication. Without that, a private authority which also issues
        // client certificates lets any holder of one impersonate the server for a name it covers.
        using X509Certificate2 authority = TestCertificates.CreateAuthority();
        using X509Certificate2 server = TestCertificates.IssueServerCertificate(authority, ServerName, CertificatePurpose.ClientAuthentication);

        var tls = new TlsParameters
        {
            TargetHost = ServerName,
            CaCertificates = TlsParameters.LoadCaCertificates(TestCertificates.WritePemFile(authority)),
        };

        Assert.ThrowsAsync<AuthenticationException>(async () => await RoundTripThroughTlsAsync(server, tls));
    }

    [Test]
    public async Task WrapAsync_LeafUnderAnIntermediateWithOnlyTheRootPinned_CompletesHandshake()
    {
        // The rebuild starts from an empty store, so a chain that does not end at the pinned root in one hop can
        // only be completed from the intermediates the server sent. Delete the ExtraStore loop and this fails.
        using X509Certificate2 root = TestCertificates.CreateAuthority();
        using X509Certificate2 intermediate = TestCertificates.CreateIntermediate(root);
        using X509Certificate2 server = TestCertificates.IssueServerCertificate(intermediate, ServerName);

        var tls = new TlsParameters
        {
            TargetHost = ServerName,
            CaCertificates = TlsParameters.LoadCaCertificates(TestCertificates.WritePemFile(root)),
        };

        byte[] echoed = await RoundTripThroughTlsAsync(server, tls, intermediate);

        Assert.That(echoed, Is.EqualTo(Payload));
    }

    [Test]
    public void WrapAsync_ConfigureHookRejects_OverridesTheDeclarativeSettings()
    {
        // The hook is documented as applied last, so it must be able to override even AllowInvalidCertificates.
        using X509Certificate2 server = TestCertificates.CreateSelfSignedServerCertificate(ServerName);

        var tls = new TlsParameters
        {
            TargetHost = ServerName,
            AllowInvalidCertificates = true,
            Configure = options => options.RemoteCertificateValidationCallback = (_, _, _, _) => false,
        };

        Assert.ThrowsAsync<AuthenticationException>(async () => await RoundTripThroughTlsAsync(server, tls));
    }

    [Test]
    public async Task WrapAsync_ConfigureHook_SeesTheTargetHostAlreadySet()
    {
        using X509Certificate2 server = TestCertificates.CreateSelfSignedServerCertificate(ServerName);
        string observed = null;

        var tls = new TlsParameters
        {
            TargetHost = ServerName,
            AllowInvalidCertificates = true,
            Configure = options => observed = options.TargetHost,
        };

        await RoundTripThroughTlsAsync(server, tls);

        Assert.That(observed, Is.EqualTo(ServerName));
    }

    [Test]
    public async Task WrapAsync_ConfigureHookAsksForRevocationChecking_TheCustomRootRebuildUsesIt()
    {
        // The rebuild must not hardcode revocation off, or a caller who turned it on gets nothing and a revoked
        // certificate is accepted. Neither test certificate names a revocation list, so a rebuild that honours
        // the request cannot determine revocation status and refuses; one that ignores it accepts.
        using X509Certificate2 authority = TestCertificates.CreateAuthority();
        using X509Certificate2 server = TestCertificates.IssueServerCertificate(authority, ServerName);
        string caPath = TestCertificates.WritePemFile(authority);

        var honoured = new TlsParameters
        {
            TargetHost = ServerName,
            CaCertificates = TlsParameters.LoadCaCertificates(caPath),
            Configure = options => options.CertificateRevocationCheckMode = X509RevocationMode.Online,
        };

        Assert.ThrowsAsync<AuthenticationException>(async () => await RoundTripThroughTlsAsync(server, honoured));

        // The same chain, with the default of no revocation checking, is accepted — so the refusal above is the
        // revocation mode taking effect and not something else wrong with the certificate.
        var byDefault = new TlsParameters
        {
            TargetHost = ServerName,
            CaCertificates = TlsParameters.LoadCaCertificates(caPath),
        };

        Assert.That(await RoundTripThroughTlsAsync(server, byDefault), Is.EqualTo(Payload));
    }

    [Test]
    public async Task WrapAsync_HandshakeFails_DisposesTheInnerStream()
    {
        // WrapAsync takes ownership of the inner stream the moment it wraps it. If it did not close it on the
        // failure path, every rejected certificate would leak the transport it was negotiating over.
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();

        // Accept and drop, so the TLS handshake fails before any certificate is exchanged.
        Task server = Task.Run(async () =>
        {
            using TcpClient accepted = await listener.AcceptTcpClientAsync();
            accepted.Close();
        });

        using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        await socket.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        var inner = new DisposeTrackingStream(new NetworkStream(socket, ownsSocket: false));

        var tls = new TlsParameters { TargetHost = ServerName, AllowInvalidCertificates = true };

        Assert.That(async () => await tls.WrapAsync(inner, CancellationToken.None), Throws.Exception);
        Assert.That(inner.Disposed, Is.True);

        await server;
    }

    [Test]
    public void LoadCaCertificates_FileHoldingNoCertificate_ThrowsArgumentException()
    {
        string path = Path.Combine(Path.GetTempPath(), $"tcp-tls-empty-{Guid.NewGuid():N}.pem");
        File.WriteAllText(path, "not a certificate\n");
        TestCertificates.TrackForCleanUp(path);

        Assert.Throws<ArgumentException>(() => TlsParameters.LoadCaCertificates(path));
    }

    [Test]
    public void LoadCaCertificates_MissingFile_Throws()
    {
        // Reading happens once at client construction, so a typo in the path must surface there.
        string path = Path.Combine(Path.GetTempPath(), $"tcp-tls-absent-{Guid.NewGuid():N}.pem");

        Assert.Throws<FileNotFoundException>(() => TlsParameters.LoadCaCertificates(path));
    }

    [Test]
    public void LoadCaCertificates_PemFileHoldingTwoCertificates_LoadsBoth()
    {
        // A CA bundle commonly carries a root and the intermediate that signs for it.
        using X509Certificate2 root = TestCertificates.CreateAuthority();
        using X509Certificate2 intermediate = TestCertificates.CreateIntermediate(root);

        X509Certificate2Collection loaded = TlsParameters.LoadCaCertificates(TestCertificates.WritePemFile(root, intermediate));

        Assert.That(loaded, Has.Count.EqualTo(2));
    }

    // Runs one TLS exchange against a throwaway echo server presenting the given certificate, and returns what
    // came back. Throws whatever the client's handshake threw.
    private static async Task<byte[]> RoundTripThroughTlsAsync(
        X509Certificate2 serverCertificate,
        TlsParameters tls,
        X509Certificate2 intermediate = null)
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        Task server = RunTlsEchoServerAsync(listener, serverCertificate, intermediate);

        try
        {
            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            await socket.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);

            // WrapAsync owns the NetworkStream from here, and disposing the returned stream closes both.
            using Stream encrypted = await tls.WrapAsync(new NetworkStream(socket, ownsSocket: false), CancellationToken.None);
            await encrypted.WriteAsync(Payload);
            await encrypted.FlushAsync();

            var received = new byte[Payload.Length];
            int read = await encrypted.ReadAsync(received);
            return received.AsSpan(0, read).ToArray();
        }
        finally
        {
            // The server side fails too when the client rejects the certificate; that is expected, not a failure
            // of the test, so its exception is swallowed rather than masking the client's.
            await server.ContinueWith(static _ => { }, TaskScheduler.Default);
        }
    }

    private static async Task RunTlsEchoServerAsync(TcpListener listener, X509Certificate2 certificate, X509Certificate2 intermediate)
    {
        using TcpClient accepted = await listener.AcceptTcpClientAsync();
        using var ssl = new SslStream(accepted.GetStream(), leaveInnerStreamOpen: false);

        var options = new SslServerAuthenticationOptions();
        if (intermediate is null)
        {
            options.ServerCertificate = certificate;
        }
        else
        {
            // Sending the intermediate alongside the leaf is what lets a client that pins only the root complete
            // the chain, and is how a real server is configured.
            options.ServerCertificateContext = SslStreamCertificateContext.Create(certificate, [intermediate]);
        }

        await ssl.AuthenticateAsServerAsync(options);

        var buffer = new byte[64];
        int read = await ssl.ReadAsync(buffer);
        await ssl.WriteAsync(buffer.AsMemory(0, read));
        await ssl.FlushAsync();
    }

    // Reports whether the stream it wraps was disposed, which is how the ownership contract is observed.
    private sealed class DisposeTrackingStream(Stream inner) : Stream
    {
        internal bool Disposed { get; private set; }

        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => false;

        public override bool CanWrite => inner.CanWrite;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => inner.Flush();

        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => inner.ReadAsync(buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => inner.WriteAsync(buffer, cancellationToken);

        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
