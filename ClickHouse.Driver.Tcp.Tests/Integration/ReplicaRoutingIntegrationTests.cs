using System;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Replica-aware routing against the suite's own server, through <see cref="TlsTerminatingProxy"/>. The proxy records
/// the TLS server name (SNI) of each connection and the plaintext the client sent on it, so each test can tell which
/// connection a query ran on and which name that connection asked for. The proxy certificate names only the normal
/// host, which does not cover the sticky name, so every tagged handshake here also proves the certificate check.
/// That Cloud routes by the name is covered by <see cref="ReplicaRoutingCloudIntegrationTests"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ReplicaRoutingIntegrationTests
{
    private const string CertificateName = "replicas.tls.test.invalid";

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
        proxy = new TlsTerminatingProxy(TcpServerFixture.Host, TcpServerFixture.Port, serverCertificate, recordClientBytes: true);
    }

    [OneTimeTearDown]
    public async Task StopProxyAsync()
    {
        await proxy.DisposeAsync();
        serverCertificate.Dispose();
        authority.Dispose();
        TestCertificates.DeleteTemporaryFiles();
    }

    [Test]
    public async Task ExecuteScalarAsync_ClientReplicaTag_RunsOnAConnectionThatSentTheStickyName()
    {
        await using ClickHouseTcpClient client = CreateClient(replicaTag: "wl1");
        string queryId = NewQueryId();

        object result = await client.ExecuteScalarAsync("SELECT 1", new ClickHouseTcpQueryOptions { QueryId = queryId }, None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo((byte)1));
            Assert.That(ConnectionThatSent(queryId).ServerName, Is.EqualTo(Sticky("wl1")));
        });
    }

    [Test]
    public async Task ExecuteScalarAsync_WithoutAReplicaTag_RunsOnAConnectionThatSentTheNormalName()
    {
        await using ClickHouseTcpClient client = CreateClient();
        string queryId = NewQueryId();

        await client.ExecuteScalarAsync("SELECT 1", new ClickHouseTcpQueryOptions { QueryId = queryId }, None);

        Assert.That(ConnectionThatSent(queryId).ServerName, Is.EqualTo(CertificateName));
    }

    [Test]
    public async Task ClickHouseTcpClient_ReplicaTagInTheConnectionString_RunsOnAConnectionThatSentTheStickyName()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder(TcpServerFixture.ConnectionString)
        {
            Host = "127.0.0.1",
            Port = proxy.Port,
            UseTls = true,
            TlsServerName = CertificateName,
            TlsCaCertificatePath = authorityPath,
            ReplicaTag = "fromcs",
        };
        await using var client = new ClickHouseTcpClient(builder.ConnectionString);
        string queryId = NewQueryId();

        await client.ExecuteScalarAsync("SELECT 1", new ClickHouseTcpQueryOptions { QueryId = queryId }, None);

        Assert.That(ConnectionThatSent(queryId).ServerName, Is.EqualTo(Sticky("fromcs")));
    }

    [Test]
    public async Task ExecuteScalarAsync_ClientReplicaTag_LogsTheTagWhenItOpensTheConnection()
    {
        using var logging = new CapturingLoggerFactory();
        await using var client = new ClickHouseTcpClient(ProxyOptions() with { ReplicaTag = "logged", LoggerFactory = logging });

        await client.ExecuteScalarAsync("SELECT 1", cancellationToken: None);

        CapturingLogger log = logging.Logger(ClickHouseTcpDiagnostics.ConnectionLogCategory);
        Assert.Multiple(() =>
        {
            Assert.That(log.WithEventId(2004).Single().Message, Does.Contain("with replica tag logged"));
            Assert.That(log.WithEventId(2000), Is.Empty, "a tagged dial is announced by its own event");
        });
    }

    [Test]
    public async Task DataSourceOpenSessionAsync_WithAReplicaTag_PinsAConnectionThatSentTheStickyName()
    {
        await using var dataSource = new ClickHouseTcpDataSource(ProxyOptions());
        await using IClickHouseTcpSession session = await dataSource.OpenSessionAsync(
            new ClickHouseTcpSessionOptions { ReplicaTag = "ds" },
            None);
        string queryId = NewQueryId();

        await session.ExecuteScalarAsync("SELECT 1", new ClickHouseTcpQueryOptions { QueryId = queryId }, None);

        Assert.That(ConnectionThatSent(queryId).ServerName, Is.EqualTo(Sticky("ds")));
    }

    [Test]
    public async Task OpenSessionAsync_WithAReplicaTag_KeepsSessionStateOnOneConnectionThatSentTheStickyName()
    {
        await using ClickHouseTcpClient client = CreateClient(replicaTag: "dflt");
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(
            new ClickHouseTcpSessionOptions { ReplicaTag = "sess" },
            None);
        string temporary = $"tcp_replica_routing_session_{Guid.NewGuid():N}";
        string create = NewQueryId();
        string insert = NewQueryId();
        string select = NewQueryId();
        string elsewhere = NewQueryId();

        await session.ExecuteAsync($"CREATE TEMPORARY TABLE {temporary} (id UInt64)", new ClickHouseTcpQueryOptions { QueryId = create }, None);

        await session.InsertRowsAsync(
            $"INSERT INTO {temporary} (id) VALUES",
            [[1UL], [2UL], [3UL]],
            new ClickHouseTcpInsertOptions { QueryId = insert },
            None);

        // The client keeps its own tag on its other connections while the session holds its connection.
        await client.ExecuteScalarAsync("SELECT 1", new ClickHouseTcpQueryOptions { QueryId = elsewhere }, None);

        object count = await session.ExecuteScalarAsync($"SELECT count() FROM {temporary}", new ClickHouseTcpQueryOptions { QueryId = select }, None);

        ProxiedConnection pinned = ConnectionThatSent(create);
        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(3UL), "the temporary table lives on the session's connection");
            Assert.That(pinned.ServerName, Is.EqualTo(Sticky("sess")));
            Assert.That(ConnectionThatSent(insert), Is.SameAs(pinned));
            Assert.That(ConnectionThatSent(select), Is.SameAs(pinned));
            Assert.That(ConnectionThatSent(elsewhere).ServerName, Is.EqualTo(Sticky("dflt")));
        });
    }

    [Test]
    public async Task OpenSessionAsync_WithoutAReplicaTag_RunsOnAConnectionWithTheClientTag()
    {
        await using ClickHouseTcpClient client = CreateClient(replicaTag: "dflt");
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(new ClickHouseTcpSessionOptions(), None);
        string queryId = NewQueryId();

        await session.ExecuteScalarAsync("SELECT 1", new ClickHouseTcpQueryOptions { QueryId = queryId }, None);

        Assert.That(ConnectionThatSent(queryId).ServerName, Is.EqualTo(Sticky("dflt")));
    }

    private static string NewQueryId() => $"replica-routing-{Guid.NewGuid():N}";

    private static string Sticky(string tag) => $"{tag}.sticky.{CertificateName}";

    // The one connection whose plaintext holds the marker. A query id travels uncompressed in the Query packet.
    private ProxiedConnection ConnectionThatSent(string marker)
    {
        ProxiedConnection[] matches = proxy.Connections.Where(connection => connection.Sent(marker)).ToArray();
        Assert.That(matches, Has.Length.EqualTo(1), $"exactly one proxied connection must carry '{marker}'");
        return matches[0];
    }

    private ClickHouseTcpClient CreateClient(string replicaTag = null)
        => new(ProxyOptions() with { ReplicaTag = replicaTag });

    private ClickHouseTcpClientOptions ProxyOptions()
        => TcpServerFixture.Options() with
        {
            Host = "127.0.0.1",
            Port = proxy.Port,
            UseTls = true,
            TlsServerName = CertificateName,
            TlsCaCertificatePath = authorityPath,
        };
}
