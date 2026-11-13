using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// How the pool handles the client's replica tag and a session's own tag, over connections that need no server. That
// a tagged connection really sends the sticky TLS server name is covered by TlsParametersTests and
// ReplicaRoutingIntegrationTests.
[TestFixture]
public class ConnectionPoolReplicaTagTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // A tag needs TLS and a DNS host name. The fake factory opens no socket, so neither is used beyond validation.
    private static ClickHouseTcpClientOptions Options(int maxPoolSize = 4, string replicaTag = null)
        => new()
        {
            Host = "service.example",
            UseTls = true,
            MaxPoolSize = maxPoolSize,
            ReplicaTag = replicaTag,
        };

    [TestCase(null)]
    [TestCase("")]
    [TestCase("dflt")]
    public async Task RentAsync_NoTagOrTheClientTag_OpensWithTheClientTagAndReusesTheConnection(string tag)
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(replicaTag: "dflt"), factory, new ControlledTimeProvider());

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            first = lease.Connection;
        }

        await using IConnectionLease second = await pool.RentAsync(tag, None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Connection, Is.SameAs(first));
            Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "dflt" }));
        });
    }

    [Test]
    public async Task RentAsync_AnotherTag_OpensANewConnectionAndLeavesTheIdleOneAlone()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        ClickHouseTcpConnection idle;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            idle = lease.Connection;
        }

        await using IConnectionLease session = await pool.RentAsync("a", None);

        Assert.Multiple(() =>
        {
            Assert.That(session.Connection, Is.Not.SameAs(idle));
            Assert.That(factory.CreatedTags, Is.EqualTo(new[] { null, "a" }));
            Assert.That(pool.IdleCount, Is.EqualTo(1), "there is room under MaxPoolSize, so nothing is closed");
            Assert.That(idle.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task Return_ConnectionWithAnotherTag_ClosesItRatherThanPoolingIt()
    {
        // A session closes its connection before it returns it, so this is the pool's own check.
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        ClickHouseTcpConnection tagged;
        await using (IConnectionLease lease = await pool.RentAsync("a", None))
        {
            tagged = lease.Connection;
        }

        await using IConnectionLease next = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(tagged.State, Is.EqualTo(TcpConnectionState.Terminated));
            Assert.That(next.Connection, Is.Not.SameAs(tagged));
            Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "a", null }));
        });
    }

    [Test]
    public async Task RentAsync_AnotherTagOnAPoolFullOfIdleConnections_ClosesTheLongestIdleFirst()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: 2), factory, new ControlledTimeProvider());

        IConnectionLease first = await pool.RentAsync(None);
        IConnectionLease second = await pool.RentAsync(None);
        await first.DisposeAsync();
        await second.DisposeAsync();

        await using IConnectionLease session = await pool.RentAsync("a", None);

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreatedTags, Is.EqualTo(new[] { null, null, "a" }));
            Assert.That(factory.Created[0].State, Is.EqualTo(TcpConnectionState.Terminated), "idle longest, so it is closed");
            Assert.That(factory.Created[1].State, Is.EqualTo(TcpConnectionState.Ready), "one closed connection makes room");
            Assert.That(pool.IdleCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RentAsync_AnotherTagWithEveryConnectionLeased_WaitsForASlotRatherThanOpeningPastTheCap()
    {
        var factory = new FakeConnectionFactory();
        ClickHouseTcpClientOptions options = Options(maxPoolSize: 1) with { PoolTimeout = TimeSpan.FromMilliseconds(50) };
        await using var pool = new ConnectionPool(options, factory, new ControlledTimeProvider());

        await using IConnectionLease leased = await pool.RentAsync(None);

        Assert.ThrowsAsync<TimeoutException>(async () => await pool.RentAsync("a", None));
        Assert.That(factory.CreateCount, Is.EqualTo(1));
    }

    [Test]
    public async Task RentAsync_ConcurrentCheckoutsWithAndWithoutSessionTags_StayWithinMaxPoolSize()
    {
        // Half the checkouts use the client's tag and reuse idle connections. The other half have a session tag and
        // must close an idle connection before they dial when the pool is full. A connection counts as open from the
        // moment its dial starts until its socket is closed, so a dial before the close would show here.
        const int MaxPoolSize = 4;
        for (int round = 0; round < 20; round++)
        {
            await CheckOutConcurrentlyAsync(MaxPoolSize, round);
        }
    }

    [Test]
    public async Task Sweep_TopsUpToTheFloor_WithTheClientTag()
    {
        var factory = new FakeConnectionFactory();
        ClickHouseTcpClientOptions options = Options(replicaTag: "dflt") with { MinPoolSize = 1 };
        await using var pool = new ConnectionPool(options, factory, new ControlledTimeProvider());

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        await using IConnectionLease lease = await pool.RentAsync(None);

        Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "dflt" }), "the checkout reuses the top-up's connection");
    }

    [Test]
    public async Task RentAsync_InvalidTag_ThrowsArgumentExceptionAndKeepsTheSlot()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: 1), factory, new ControlledTimeProvider());

        var thrown = Assert.ThrowsAsync<ArgumentException>(async () => await pool.RentAsync("not a label", None));

        // Refused before a permit is taken, so the only slot is still free.
        await using IConnectionLease lease = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(thrown.ParamName, Is.EqualTo(nameof(ClickHouseTcpSessionOptions.ReplicaTag)));
            Assert.That(factory.CreatedTags, Is.EqualTo(new string[] { null }));
        });
    }

    [Test]
    public async Task RentAsync_TagOnAClientWithoutTls_ThrowsArgumentException()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(new ClickHouseTcpClientOptions(), factory, new ControlledTimeProvider());

        var thrown = Assert.ThrowsAsync<ArgumentException>(async () => await pool.RentAsync("a", None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain(nameof(ClickHouseTcpClientOptions.UseTls)));
            Assert.That(factory.CreateCount, Is.Zero);
        });
    }

    [Test]
    public async Task OpenSessionAsync_WithATag_PinsAConnectionOpenedWithThatTag()
    {
        var factory = new FakeConnectionFactory();
        ClickHouseTcpClientOptions options = Options(replicaTag: "dflt");
        await using var client = new ClickHouseTcpClient(new ConnectionPool(options, factory, new ControlledTimeProvider()), options);

        await using (await client.OpenSessionAsync(new ClickHouseTcpSessionOptions { ReplicaTag = "user-42" }))
        {
        }

        await using (await client.OpenSessionAsync())
        {
        }

        Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "user-42", "dflt" }), "a session without a tag takes the client tag");
    }

    private static async Task CheckOutConcurrentlyAsync(int maxPoolSize, int round)
    {
        var factory = new FakeConnectionFactory();
        int open = 0;
        int peak = 0;
        factory.BeforeCreate = _ =>
        {
            int now = Interlocked.Increment(ref open);
            int seen;
            while (now > (seen = Volatile.Read(ref peak)) && Interlocked.CompareExchange(ref peak, now, seen) != seen)
            {
            }

            return Task.CompletedTask;
        };
        factory.OnClose = () => Interlocked.Decrement(ref open);
        await using var pool = new ConnectionPool(Options(maxPoolSize: maxPoolSize), factory, new ControlledTimeProvider());

        var live = new HashSet<ClickHouseTcpConnection>();
        var clientConnections = new HashSet<ClickHouseTcpConnection>();
        var sessionConnections = new HashSet<ClickHouseTcpConnection>();
        var failures = new List<string>();

        await Task.WhenAll(Enumerable.Range(0, 32).Select(worker => Task.Run(async () =>
        {
            for (int i = 0; i < 25; i++)
            {
                string tag = (worker + i) % 2 == 0 ? null : $"t{(worker + i) % 7}";
                IConnectionLease lease = await pool.RentAsync(tag, None);
                lock (live)
                {
                    if (!live.Add(lease.Connection))
                    {
                        failures.Add("two leases held the same connection at once");
                    }

                    if (tag is null)
                    {
                        clientConnections.Add(lease.Connection);
                    }
                    else if (!sessionConnections.Add(lease.Connection))
                    {
                        failures.Add("a connection with a session tag was handed out twice");
                    }
                }

                try
                {
                    await Task.Yield();
                }
                finally
                {
                    lock (live)
                    {
                        live.Remove(lease.Connection);
                    }

                    await lease.DisposeAsync();
                }
            }
        })));

        IReadOnlyList<ClickHouseTcpConnection> created = factory.Created;
        IReadOnlyList<string> tags = factory.CreatedTags;
        string[] clientTags = clientConnections.Select(connection => tags[IndexOf(created, connection)]).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty, $"round {round}");
            Assert.That(peak, Is.LessThanOrEqualTo(maxPoolSize), $"round {round}: open connections");
            Assert.That(clientTags, Is.All.Null, $"round {round}: a checkout with the client tag got a session's connection");
        });
    }

    private static int IndexOf(IReadOnlyList<ClickHouseTcpConnection> connections, ClickHouseTcpConnection connection)
    {
        for (int i = 0; i < connections.Count; i++)
        {
            if (ReferenceEquals(connections[i], connection))
            {
                return i;
            }
        }

        return -1;
    }
}
