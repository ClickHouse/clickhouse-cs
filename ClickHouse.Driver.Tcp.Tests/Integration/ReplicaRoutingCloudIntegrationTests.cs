using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Replica-aware routing on a ClickHouse Cloud service. These need a service with more than one replica and with
/// replica-aware routing for the native protocol turned on, which the CI Cloud service has. Elsewhere the fixture is
/// ignored. <see cref="ReplicaRoutingIntegrationTests"/> covers what the client sends without Cloud.
/// </summary>
/// <remarks>
/// <para>
/// Each probe that must reach the load balancer afresh uses a new client, so it opens a new connection. A probe
/// reports <c>hostName()</c>, which names the replica that ran it.
/// </para>
/// <para>
/// The positive tests show that one tag reaches one replica. They pass on a service with one replica too, so the
/// negative tests show that the service has several replicas and that the tag chooses among them. Without them, a
/// positive result could come from a service that sends everything to one replica.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public class ReplicaRoutingCloudIntegrationTests
{
    // With two replicas and an even spread, all of this many probes land on one replica by chance with a probability
    // of 2^-15, about 3 in 100,000.
    private const int SpreadProbes = 16;

    private const int SameTagProbes = 8;

    private static readonly CancellationToken None = CancellationToken.None;

    [OneTimeSetUp]
    public void RequireCloud()
    {
        if (!TcpServerFixture.IsCloud)
        {
            Assert.Ignore("Replica-aware routing is a ClickHouse Cloud feature, so these tests run only in a Cloud run.");
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_SameReplicaTagOverNewConnections_ReachesOneReplica()
    {
        var replicas = new HashSet<string>();
        for (int i = 0; i < SameTagProbes; i++)
        {
            replicas.Add(await ReplicaOverANewConnectionAsync("chcs-same-tag"));
        }

        Assert.That(replicas, Has.Count.EqualTo(1), $"every connection with one tag must reach one replica, but they reached {string.Join(", ", replicas)}");
    }

    [Test]
    public async Task ExecuteScalarAsync_ReplicaTagInTheClientOptions_ReachesTheReplicaOfThatTag()
    {
        string perQuery = await ReplicaOverANewConnectionAsync("chcs-client-default");

        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { ReplicaTag = "chcs-client-default" });
        object byDefault = await client.ExecuteScalarAsync("SELECT hostName()", cancellationToken: None);

        Assert.That(byDefault, Is.EqualTo(perQuery));
    }

    [Test]
    public async Task OpenSessionAsync_SameReplicaTag_ReachesTheReplicaOfThatTag()
    {
        const string tag = "chcs-session";
        string expected = await ReplicaOverANewConnectionAsync(tag);

        // Two sessions held at once have two connections, so this is two more trips through the load balancer.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession first = await client.OpenSessionAsync(new ClickHouseTcpSessionOptions { ReplicaTag = tag }, None);
        await using IClickHouseTcpSession second = await client.OpenSessionAsync(new ClickHouseTcpSessionOptions { ReplicaTag = tag }, None);

        object firstReplica = await first.ExecuteScalarAsync("SELECT hostName()", cancellationToken: None);
        object secondReplica = await second.ExecuteScalarAsync("SELECT hostName()", cancellationToken: None);

        Assert.Multiple(() =>
        {
            Assert.That(firstReplica, Is.EqualTo(expected));
            Assert.That(secondReplica, Is.EqualTo(expected));
        });
    }

    [Test]
    public async Task ExecuteScalarAsync_SeveralTagsThroughOneSmallPool_EachTagKeepsItsReplica()
    {
        // More tags than pool slots, so the pool closes connections of one tag to open connections of another, and
        // each round reaches every tag over a new connection.
        string[] tags = Enumerable.Range(0, 6).Select(i => $"chcs-pool-{i}").ToArray();
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { MaxPoolSize = 2 });
        var replicasByTag = tags.ToDictionary(tag => tag, _ => new HashSet<string>());

        for (int round = 0; round < 3; round++)
        {
            foreach (string tag in tags)
            {
                object replica = await client.ExecuteScalarAsync(
                    "SELECT hostName()",
                    new ClickHouseTcpQueryOptions { ReplicaTag = tag },
                    None);
                replicasByTag[tag].Add((string)replica);
            }
        }

        Assert.Multiple(() =>
        {
            foreach ((string tag, HashSet<string> replicas) in replicasByTag)
            {
                Assert.That(replicas, Has.Count.EqualTo(1), $"tag '{tag}' reached {string.Join(", ", replicas)}");
            }
        });
    }

    [Test]
    public async Task ExecuteScalarAsync_NoReplicaTagOverNewConnections_ReachesMoreThanOneReplica()
    {
        // Without a tag the load balancer spreads connections. This shows the service has several replicas, so the
        // tests above that see one replica per tag are not seeing a service with only one.
        var replicas = new HashSet<string>();
        for (int i = 0; i < SpreadProbes; i++)
        {
            replicas.Add(await ReplicaOverANewConnectionAsync(replicaTag: null));
        }

        Assert.That(replicas, Has.Count.GreaterThan(1), $"{SpreadProbes} connections without a tag all reached {string.Join(", ", replicas)}");
    }

    [Test]
    public async Task ExecuteScalarAsync_DifferentReplicaTags_ReachMoreThanOneReplica()
    {
        // The tag chooses among the replicas, so different tags must not all map to one. Fixed tags, so the outcome
        // is the same on every run while the replica count does not change. That the service honours a tag at all
        // follows from the same-tag test against the test without a tag.
        var replicas = new HashSet<string>();
        for (int i = 0; i < SpreadProbes; i++)
        {
            replicas.Add(await ReplicaOverANewConnectionAsync($"chcs-spread-{i}"));
        }

        Assert.That(replicas, Has.Count.GreaterThan(1), $"{SpreadProbes} different tags all reached {string.Join(", ", replicas)}");
    }

    private static async Task<string> ReplicaOverANewConnectionAsync(string replicaTag)
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        object replica = await client.ExecuteScalarAsync(
            "SELECT hostName()",
            new ClickHouseTcpQueryOptions { ReplicaTag = replicaTag },
            None);
        return (string)replica;
    }
}
