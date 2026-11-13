using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// The client version the driver reports in its handshake and in the ClientInfo of every query. A server refuses to
/// send a distributed query on when the client that started it reports version 0.0.0, so a query that reaches another
/// replica over a connection of its own is the test that matters.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public class ClientVersionIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task ExecuteScalarAsync_QueryThatReadsEveryCloudReplica_Succeeds()
    {
        if (!TcpServerFixture.IsCloud)
        {
            Assert.Ignore("Only a Cloud service has replicas that a query reaches over connections of their own.");
        }

        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        object replicas = await client.ExecuteScalarAsync(
            "SELECT count() FROM clusterAllReplicas(default, system.one)",
            cancellationToken: None);

        Assert.That(replicas, Is.GreaterThanOrEqualTo(1UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_RemoteQueryToTheSameServer_Succeeds()
    {
        if (TcpServerFixture.IsCloud)
        {
            Assert.Ignore("A Cloud service does not accept remote() to its own local port.");
        }

        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        // The server's own plaintext native port, which is not the port the suite dialled when a proxy or TLS is in
        // between.
        object port = await client.ExecuteScalarAsync("SELECT tcpPort()", cancellationToken: None);
        var parameters = new ClickHouseTcpParameterCollection
        {
            new ClickHouseTcpParameter("address", $"localhost:{port}"),
            new ClickHouseTcpParameter("user", TcpServerFixture.Username),
            new ClickHouseTcpParameter("password", TcpServerFixture.Password),
        };

        // prefer_localhost_replica = 0 makes the server open a connection to itself instead of reading locally, so
        // the query takes the same path as a query to another replica.
        object rows = await client.ExecuteScalarAsync(
            "SELECT count() FROM remote({address:String}, system.one, {user:String}, {password:String}) SETTINGS prefer_localhost_replica = 0",
            new ClickHouseTcpQueryOptions { Parameters = parameters },
            None);

        Assert.That(rows, Is.EqualTo(1UL));
    }

    [Test]
    public async Task ExecuteAsync_AnyQuery_RecordsTheDriverVersionAsTheClientVersion()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string queryId = Guid.NewGuid().ToString();

        await client.ExecuteAsync("SELECT 1", new ClickHouseTcpQueryOptions { QueryId = queryId }, None);

        object reported = await QueryLog.ScalarAsync(
            client,
            $"SELECT format('{{}}.{{}}.{{}}', client_version_major, client_version_minor, client_version_patch) FROM {QueryLog.Table} WHERE query_id = '{queryId}' AND type = 'QueryFinish'");

        (int major, int minor, int patch) = ClientVersion.Current;
        Assert.That(reported, Is.EqualTo($"{major}.{minor}.{patch}"));
    }
}
