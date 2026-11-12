using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// How the pool and a session handle replica tags, over connections that need no server. That a tagged connection
// really sends the sticky TLS server name is covered by TlsParametersTests and ReplicaRoutingIntegrationTests.
[TestFixture]
public class ConnectionPoolReplicaTagTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // A tag needs TLS and a DNS host name. The fake factory opens no socket, so neither is used beyond validation.
    private static ClickHouseTcpClientOptions Options(
        int maxPoolSize = 4,
        string replicaTag = null,
        ClickHouseTcpPoolReusePolicy reusePolicy = ClickHouseTcpPoolReusePolicy.Lifo)
        => new()
        {
            Host = "service.example",
            UseTls = true,
            MaxPoolSize = maxPoolSize,
            ReplicaTag = replicaTag,
            PoolReusePolicy = reusePolicy,
        };

    [Test]
    public async Task RentAsync_SameTagAfterReturn_ReusesThatConnection()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync("a", None))
        {
            first = lease.Connection;
        }

        await using IConnectionLease second = await pool.RentAsync("a", None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Connection, Is.SameAs(first));
            Assert.That(second.ReplicaTag, Is.EqualTo("a"));
            Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "a" }));
        });
    }

    [Test]
    public async Task RentAsync_AnotherTagWhileAConnectionIsIdle_OpensAConnectionWithTheOtherTag()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        ClickHouseTcpConnection tagged;
        await using (IConnectionLease lease = await pool.RentAsync("a", None))
        {
            tagged = lease.Connection;
        }

        await using IConnectionLease other = await pool.RentAsync("b", None);

        Assert.Multiple(() =>
        {
            Assert.That(other.Connection, Is.Not.SameAs(tagged));
            Assert.That(other.ReplicaTag, Is.EqualTo("b"));
            Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "a", "b" }));
            Assert.That(pool.IdleCount, Is.EqualTo(1), "the connection with tag 'a' stays pooled for its own tag");
            Assert.That(tagged.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task RentAsync_NoTagAndNoDefault_DoesNotReuseATaggedConnection()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        await using (await pool.RentAsync("a", None))
        {
        }

        await using IConnectionLease untagged = await pool.RentAsync(replicaTag: null, None);

        Assert.Multiple(() =>
        {
            Assert.That(untagged.ReplicaTag, Is.Null);
            Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "a", null }));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    public async Task RentAsync_NoTagWithAClientDefault_UsesTheDefaultTag(string tag)
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(replicaTag: "dflt"), factory, new ControlledTimeProvider());

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync(tag, None))
        {
            first = lease.Connection;
        }

        // Naming the default explicitly is the same tag, so it reuses the connection.
        await using IConnectionLease named = await pool.RentAsync("dflt", None);

        Assert.Multiple(() =>
        {
            Assert.That(named.Connection, Is.SameAs(first));
            Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "dflt" }));
        });
    }

    [Test]
    public async Task RentAsync_PoolFullOfIdleConnectionsWithOtherTags_ClosesTheLongestIdleToStayWithinMaxPoolSize()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: 2), factory, new ControlledTimeProvider());

        IConnectionLease a = await pool.RentAsync("a", None);
        IConnectionLease b = await pool.RentAsync("b", None);
        await a.DisposeAsync();
        await b.DisposeAsync();

        await using IConnectionLease c = await pool.RentAsync("c", None);

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "a", "b", "c" }));
            Assert.That(factory.Created[0].State, Is.EqualTo(TcpConnectionState.Terminated), "'a' was idle longest, so it is closed");
            Assert.That(factory.Created[1].State, Is.EqualTo(TcpConnectionState.Ready), "one closed connection makes room");
            Assert.That(pool.IdleCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RentAsync_ConcurrentCheckoutsOnAFullPool_ClosesOnlyTheConnectionTheNewTagNeeds()
    {
        // The pool is full of idle connections tagged y0 to y3. Four checkouts start at once: three ask for y1 to y3,
        // one for the new tag z. Only z needs room, so exactly one connection, the longest idle, is closed and one is
        // opened, in whatever order the checkouts run. Repeated, because the order differs between rounds.
        for (int round = 0; round < 300; round++)
        {
            var factory = new FakeConnectionFactory();
            await using var pool = new ConnectionPool(Options(maxPoolSize: 4), factory, new ControlledTimeProvider());
            IConnectionLease[] warm =
            [
                await pool.RentAsync("y0", None),
                await pool.RentAsync("y1", None),
                await pool.RentAsync("y2", None),
                await pool.RentAsync("y3", None),
            ];
            foreach (IConnectionLease lease in warm)
            {
                await lease.DisposeAsync();
            }

            var go = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            string[] tags = ["z", "y1", "y2", "y3"];
            Task<IConnectionLease>[] checkouts = tags
                .Select(tag => Task.Run(async () =>
                {
                    await go.Task;
                    return await pool.RentAsync(tag, None);
                }))
                .ToArray();
            go.SetResult();
            IConnectionLease[] leases = await Task.WhenAll(checkouts);

            try
            {
                Assert.Multiple(() =>
                {
                    Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "y0", "y1", "y2", "y3", "z" }), $"round {round}: only z dials");
                    Assert.That(factory.Created[0].State, Is.EqualTo(TcpConnectionState.Terminated), $"round {round}: y0 makes room");
                    Assert.That(factory.Created.Skip(1).Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Ready), $"round {round}");
                    Assert.That(leases.Select(lease => lease.ReplicaTag), Is.EqualTo(tags));
                });
            }
            finally
            {
                foreach (IConnectionLease lease in leases)
                {
                    await lease.DisposeAsync();
                }
            }
        }
    }

    [Test]
    public async Task RentAsync_ManyConcurrentCheckoutsWithManyTags_NeverHasMoreThanMaxPoolSizeConnectionsOpen()
    {
        // More tags than slots, so most checkouts must close a connection of another tag before they dial. A
        // connection counts as open from the moment its dial starts until its socket is closed, so a checkout that
        // dialed before the connection it replaced was closed would show here.
        const int MaxPoolSize = 4;
        for (int round = 0; round < 20; round++)
        {
            await CheckOutConcurrentlyWithManyTagsAsync(MaxPoolSize, round);
        }
    }

    [Test]
    public async Task RentAsync_RoomUnderMaxPoolSize_KeepsIdleConnectionsWithOtherTags()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: 2), factory, new ControlledTimeProvider());

        await using (await pool.RentAsync("a", None))
        {
        }

        await using IConnectionLease b = await pool.RentAsync("b", None);

        Assert.Multiple(() =>
        {
            Assert.That(factory.Created[0].State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(pool.IdleCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RentAsync_EveryConnectionLeasedWithAnotherTag_WaitsForAPermitRatherThanOpeningPastTheCap()
    {
        // Eviction only reaches idle connections. A full pool of leased ones still queues the caller.
        var factory = new FakeConnectionFactory();
        ClickHouseTcpClientOptions options = Options(maxPoolSize: 1) with { PoolTimeout = TimeSpan.FromMilliseconds(50) };
        await using var pool = new ConnectionPool(options, factory, new ControlledTimeProvider());

        await using IConnectionLease a = await pool.RentAsync("a", None);

        Assert.ThrowsAsync<TimeoutException>(async () => await pool.RentAsync("b", None));
        Assert.That(factory.CreateCount, Is.EqualTo(1));
    }

    // LIFO looks at the most recently returned connection first and FIFO at the least recently returned one, so each
    // case returns the connection with the other tag where its policy looks first, and the scan has to skip it.
    [TestCase(ClickHouseTcpPoolReusePolicy.Lifo, new[] { "a", "a", "b" }, 1)]
    [TestCase(ClickHouseTcpPoolReusePolicy.Fifo, new[] { "b", "a", "a" }, 0)]
    public async Task RentAsync_AnotherTagWhereThePolicyLooksFirst_SkipsItAndFollowsThePolicyAmongTheTag(
        ClickHouseTcpPoolReusePolicy policy,
        string[] returnOrder,
        int expected)
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(reusePolicy: policy), factory, new ControlledTimeProvider());

        var leases = new IConnectionLease[returnOrder.Length];
        for (int i = 0; i < returnOrder.Length; i++)
        {
            leases[i] = await pool.RentAsync(returnOrder[i], None);
        }

        ClickHouseTcpConnection[] tagged = leases.Where(lease => lease.ReplicaTag == "a").Select(lease => lease.Connection).ToArray();
        foreach (IConnectionLease lease in leases)
        {
            await lease.DisposeAsync();
        }

        await using IConnectionLease next = await pool.RentAsync("a", None);

        Assert.Multiple(() =>
        {
            Assert.That(next.Connection, Is.SameAs(tagged[expected]));
            Assert.That(factory.CreateCount, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task Sweep_TopsUpToTheFloor_WithTheClientDefaultTag()
    {
        var factory = new FakeConnectionFactory();
        ClickHouseTcpClientOptions options = Options(replicaTag: "dflt") with { MinPoolSize = 1 };
        await using var pool = new ConnectionPool(options, factory, new ControlledTimeProvider());

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        await using IConnectionLease lease = await pool.RentAsync(replicaTag: null, None);

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "dflt" }), "the checkout reuses the top-up's connection");
            Assert.That(lease.ReplicaTag, Is.EqualTo("dflt"));
        });
    }

    [Test]
    public async Task RentAsync_InvalidTag_ThrowsArgumentExceptionAndKeepsTheSlot()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: 1), factory, new ControlledTimeProvider());

        var thrown = Assert.ThrowsAsync<ArgumentException>(async () => await pool.RentAsync("not a label", None));

        // Refused before a permit is taken, so the only slot is still free.
        await using IConnectionLease lease = await pool.RentAsync(replicaTag: null, None);

        Assert.Multiple(() =>
        {
            Assert.That(thrown.ParamName, Is.EqualTo(nameof(ClickHouseTcpQueryOptions.ReplicaTag)));
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

        Assert.That(factory.CreatedTags, Is.EqualTo(new[] { "user-42", "dflt" }), "a session without a tag takes the client default");
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("a")]
    public async Task PinnedRentAsync_NoTagOrTheSessionsTag_LendsTheConnection(string tag)
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());
        await using var pinned = new PinnedConnectionSource(await pool.RentAsync("a", None));

        await using IConnectionLease lease = await pinned.RentAsync(tag, None);

        Assert.That(lease.ReplicaTag, Is.EqualTo("a"));
    }

    [TestCase("a", "b")]
    [TestCase(null, "b")]
    public async Task PinnedRentAsync_AnotherTag_ThrowsAndTheSessionStaysUsable(string sessionTag, string operationTag)
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());
        await using var pinned = new PinnedConnectionSource(await pool.RentAsync(sessionTag, None));

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(async () => await pinned.RentAsync(operationTag, None));

        // Refused before the session is marked busy, so the next operation runs.
        await using IConnectionLease next = await pinned.RentAsync(replicaTag: null, None);

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain($"'{operationTag}'"));
            Assert.That(thrown.Message, Does.Contain(nameof(ClickHouseTcpSessionOptions)));
            Assert.That(pinned.IsOpen, Is.True);
            Assert.That(factory.CreateCount, Is.EqualTo(1));
        });
    }

    private static async Task CheckOutConcurrentlyWithManyTagsAsync(int maxPoolSize, int round)
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
        var failures = new List<string>();

        await Task.WhenAll(Enumerable.Range(0, 32).Select(worker => Task.Run(async () =>
        {
            for (int i = 0; i < 25; i++)
            {
                string tag = $"t{(worker + i) % 7}";
                IConnectionLease lease = await pool.RentAsync(tag, None);
                lock (live)
                {
                    if (!live.Add(lease.Connection))
                    {
                        failures.Add("two leases held the same connection at once");
                    }

                    if (lease.ReplicaTag != tag)
                    {
                        failures.Add($"asked for {tag}, got {lease.ReplicaTag}");
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

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty, $"round {round}");
            Assert.That(peak, Is.LessThanOrEqualTo(maxPoolSize), $"round {round}: open connections");
            Assert.That(factory.CreateCount, Is.GreaterThan(maxPoolSize), "the tags outnumber the slots, so connections were replaced");
        });
    }
}
