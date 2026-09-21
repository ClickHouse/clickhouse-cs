using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Exercises queries, blocks, inserts, and pooled reuse through a real <see cref="SslStream"/>.
/// <see cref="TlsTerminatingProxy"/> terminates TLS before forwarding to the test server.
/// </summary>
[TestFixture]
[Category("Integration")]
public class TlsTransportIntegrationTests
{
    // Match the hostname in the proxy certificate, not the loopback address.
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
    /// Verifies the handshake and that the tunnel reaches the configured server.
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
    /// Verifies compressed reads spanning multiple TLS records.
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
    /// Verifies writes and pooled TLS connection reuse with a connection-scoped temporary table.
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

        // The temporary table is removed with the session.
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
