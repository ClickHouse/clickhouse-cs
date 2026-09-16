using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// The native protocol inside a TLS tunnel, against the suite's own server. <c>TcpConnectionFactoryTests</c>
/// covers the handshake and every certificate-validation outcome, but against a listener that answers with a
/// canned Hello and nothing else, so no test in the default suite has ever run a query, a block or an insert
/// through an <c>SslStream</c>. The Cloud job runs the whole <c>Cloud</c> category over one, but only where a
/// service is configured, so this is what covers it on the standard matrix.
///
/// <para>
/// <see cref="TlsTerminatingProxy"/> supplies the tunnel, so what is under test is the client's side of it: a
/// real handshake, then results large enough to span many TLS records, an insert, and a pooled connection used
/// again. The server's own TLS implementation is not covered here, and is not the driver's to cover.
/// </para>
/// <para>
/// Not in the <c>Cloud</c> category: the proxy dials the fixture's host and port in the clear, which against a
/// Cloud service is the secure port, and the client is pointed at the proxy on loopback rather than at the
/// certificate's name. A Cloud run covers this ground anyway, being entirely inside a tunnel.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class TlsTransportIntegrationTests
{
    // The name the proxy's certificate carries, not the loopback address the client dials, so TlsServerName is
    // what the certificate is matched against.
    private const string CertificateName = "clickhouse.tls.test.invalid";

    private static readonly CancellationToken None = CancellationToken.None;

    private X509Certificate2 authority;
    private X509Certificate2 serverCertificate;
    private TlsTerminatingProxy proxy;
    private string authorityPath;

    [OneTimeSetUp]
    public void StartProxy()
    {
        authority = TestCertificates.CreateAuthority();
        serverCertificate = TestCertificates.IssueServerCertificate(authority, CertificateName);
        authorityPath = TestCertificates.WritePemFile(authority);
        proxy = new TlsTerminatingProxy(TcpServerFixture.Host, TcpServerFixture.Port, serverCertificate);
    }

    [OneTimeTearDown]
    public async Task StopProxyAsync()
    {
        await proxy.DisposeAsync();
        serverCertificate.Dispose();
        authority.Dispose();
        TestCertificates.DeleteTemporaryFiles();
    }

    /// <summary>
    /// The handshake, and that the server answering inside the tunnel is the real one: the version it reports
    /// has to be the version the same server reports over plaintext. A fake server could pass everything else.
    /// </summary>
    [Test]
    public async Task PingAsync_OverTls_HandshakesWithTheServerTheSuiteAlreadyUses()
    {
        await using ClickHouseTcpClient plaintext = TcpServerFixture.CreateClient();
        await using ClickHouseTcpClient tunnelled = CreateClient();

        // The shortest exchange that needs the tunnel to carry packets both ways.
        await tunnelled.PingAsync(None);

        object overTls = await tunnelled.ExecuteScalarAsync("SELECT version()", cancellationToken: None);
        object direct = await plaintext.ExecuteScalarAsync("SELECT version()", cancellationToken: None);

        Assert.Multiple(() =>
        {
            Assert.That(overTls, Is.InstanceOf<string>().And.Not.Empty);
            Assert.That(overTls, Is.EqualTo(direct));
        });
    }

    /// <summary>
    /// A result far larger than the 16 KB a TLS record holds, so the read path refills across record boundaries
    /// rather than finding each block whole. Compression is on by default, so the blocks are compressed as well.
    /// </summary>
    [Test]
    public async Task QueryAsync_OverTls_ReturnsEveryRowAcrossManyTlsRecords()
    {
        await using ClickHouseTcpClient client = CreateClient();

        var numbers = new List<ulong>();
        await foreach (object[] row in client.QueryAsync(
            "SELECT number FROM system.numbers LIMIT 50000", cancellationToken: None))
        {
            numbers.Add((ulong)row[0]);
        }

        Assert.Multiple(() =>
        {
            Assert.That(numbers, Has.Count.EqualTo(50000));
            Assert.That(numbers[0], Is.EqualTo(0UL));
            Assert.That(numbers[^1], Is.EqualTo(49999UL));
        });
    }

    /// <summary>
    /// The write path through the tunnel, on a pool of one and into a temporary table. A native connection is the
    /// session that holds that table, so the read-back succeeds only if the same <c>SslStream</c> connection came
    /// back out of the pool for each of the three operations.
    /// </summary>
    [Test]
    public async Task InsertRowsAsync_OverTls_RoundTripsOnTheOnePooledTlsConnection()
    {
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 1);
        string temporary = $"tcp_tls_test_{Guid.NewGuid():N}";

        await client.ExecuteAsync($"CREATE TEMPORARY TABLE {temporary} (id UInt64, name String)", cancellationToken: None);
        await client.InsertRowsAsync(
            $"INSERT INTO {temporary} (id, name) VALUES",
            Enumerable.Range(0, 1000).Select(i => new object[] { (ulong)i, $"row-{i}" }).ToArray(),
            cancellationToken: None);

        var rows = new List<(ulong Id, string Name)>();
        await foreach (object[] row in client.QueryAsync($"SELECT id, name FROM {temporary} ORDER BY id", cancellationToken: None))
        {
            rows.Add(((ulong)row[0], (string)row[1]));
        }

        // No DROP: the table goes when the session does, which is what the read-back above relies on.
        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Count.EqualTo(1000));
            Assert.That(rows[0], Is.EqualTo((0UL, "row-0")));
            Assert.That(rows[^1], Is.EqualTo((999UL, "row-999")));
        });
    }

    private ClickHouseTcpClient CreateClient(int maxPoolSize = 4)
        => new(TcpServerFixture.Options() with
        {
            Host = "127.0.0.1",
            Port = proxy.Port,
            UseTls = true,
            TlsServerName = CertificateName,
            TlsCaCertificatePath = authorityPath,
            MaxPoolSize = maxPoolSize,
        });
}
