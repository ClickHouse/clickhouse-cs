using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// The pool's own behaviour, over connections that need no server: reuse, retirement, queueing and drain are all
// decided by the pool alone, and a scripted connection reaches the same Ready/Terminated states a real one does.
// A controlled clock ages connections without the test waiting, and the sweep is called directly rather than
// racing its timer. Pooling over a live server is covered by ConnectionPoolIntegrationTests.
[TestFixture]
public class ConnectionPoolTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static ClickHouseTcpClientOptions Options(
        int maxPoolSize = 4,
        int minPoolSize = 0,
        TimeSpan? poolTimeout = null,
        TimeSpan? maxConnectionLifetime = null,
        TimeSpan? idleTimeout = null,
        ClickHouseTcpPoolReusePolicy reusePolicy = ClickHouseTcpPoolReusePolicy.Lifo)
        => new()
        {
            MaxPoolSize = maxPoolSize,
            MinPoolSize = minPoolSize,
            PoolTimeout = poolTimeout ?? TimeSpan.FromSeconds(30),
            MaxConnectionLifetime = maxConnectionLifetime ?? TimeSpan.FromMinutes(30),
            IdleTimeout = idleTimeout ?? TimeSpan.FromMinutes(5),
            PoolReusePolicy = reusePolicy,
        };

    [Test]
    public async Task RentAsync_FirstRent_OpensOneConnection()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        await using IConnectionLease lease = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreateCount, Is.EqualTo(1));
            Assert.That(lease.Connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task RentAsync_AfterALeaseIsReturned_ReusesThatConnection()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            first = lease.Connection;
        }

        await using IConnectionLease second = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Connection, Is.SameAs(first));
            Assert.That(factory.CreateCount, Is.EqualTo(1), "a returned connection must not be re-opened");
        });
    }

    [Test]
    public async Task RentAsync_ConnectionTerminatedByItsOperation_IsDiscardedAndReplaced()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            // What a failed or abandoned operation leaves behind.
            lease.Connection.Terminate();
        }

        Assert.That(pool.IdleCount, Is.Zero, "a terminated connection must never re-enter the idle set");

        await using IConnectionLease replacement = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreateCount, Is.EqualTo(2));
            Assert.That(replacement.Connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task RentAsync_ConnectionTerminatedWhileIdle_IsDiscardedAtTheNextCheckout()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            first = lease.Connection;
        }

        // The server dropped it while it sat in the pool.
        first.Terminate();

        await using IConnectionLease second = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Connection, Is.Not.SameAs(first));
            Assert.That(factory.CreateCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task RentAsync_UpToMaxPoolSizeCallers_AllGetDistinctConnections()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: 3), factory, new ControlledTimeProvider());

        IConnectionLease[] leases =
        [
            await pool.RentAsync(None),
            await pool.RentAsync(None),
            await pool.RentAsync(None),
        ];

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(factory.CreateCount, Is.EqualTo(3));
                Assert.That(leases.Select(l => l.Connection).Distinct().Count(), Is.EqualTo(3));
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

    [Test]
    public async Task RentAsync_BeyondMaxPoolSize_WaitsAndIsServedWhenALeaseReturns()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: 1), factory, new ControlledTimeProvider());

        IConnectionLease held = await pool.RentAsync(None);
        ValueTask<IConnectionLease> queued = pool.RentAsync(None);

        Assert.That(queued.IsCompleted, Is.False, "the cap must make the second caller wait");

        await held.DisposeAsync();

        await using IConnectionLease served = await queued;
        Assert.Multiple(() =>
        {
            Assert.That(served.Connection, Is.SameAs(held.Connection));
            Assert.That(factory.CreateCount, Is.EqualTo(1), "the waiter takes the freed connection, it does not open one");
        });
    }

    [Test]
    public async Task RentAsync_PoolExhaustedForLongerThanPoolTimeout_ThrowsTimeoutNamingTheKnobs()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 1, poolTimeout: TimeSpan.FromMilliseconds(150)), factory, new ControlledTimeProvider());

        await using IConnectionLease held = await pool.RentAsync(None);

        var thrown = Assert.ThrowsAsync<TimeoutException>(async () => await pool.RentAsync(None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain("MaxPoolSize"));
            Assert.That(thrown.Message, Does.Contain("PoolTimeout"));
            Assert.That(thrown.Message, Does.Contain("All 1 connections are in use"));
        });
    }

    [Test]
    public async Task RentAsync_WaiterCancelled_LeavesTheSlotUsableForTheNextCaller()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: 1), factory, new ControlledTimeProvider());

        IConnectionLease held = await pool.RentAsync(None);
        using var cts = new CancellationTokenSource();
        ValueTask<IConnectionLease> cancelled = pool.RentAsync(cts.Token);
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () => await cancelled);

        await held.DisposeAsync();

        // The cancelled waiter must not have consumed the slot it never got.
        await using IConnectionLease next = await pool.RentAsync(None);
        Assert.That(next.Connection, Is.SameAs(held.Connection));
    }

    [Test]
    public async Task RentAsync_FactoryThrows_ReleasesTheSlotAndSurfacesTheFailure()
    {
        var factory = new FakeConnectionFactory { FailNextWith = new TimeoutException("connect timed out") };
        await using var pool = new ConnectionPool(Options(maxPoolSize: 1), factory, new ControlledTimeProvider());

        Assert.ThrowsAsync<TimeoutException>(async () => await pool.RentAsync(None));

        // The slot the failed attempt held has to come back, or the pool is permanently a connection short.
        await using IConnectionLease recovered = await pool.RentAsync(None);
        Assert.That(recovered.Connection.State, Is.EqualTo(TcpConnectionState.Ready));
    }

    [Test]
    public async Task RentAsync_LifoPolicy_HandsOutTheMostRecentlyReturnedConnection()
    {
        ClickHouseTcpConnection[] order = await ReturnTwoThenRentAsync(ClickHouseTcpPoolReusePolicy.Lifo);

        Assert.That(order[2], Is.SameAs(order[1]));
    }

    [Test]
    public async Task RentAsync_FifoPolicy_HandsOutTheLeastRecentlyReturnedConnection()
    {
        ClickHouseTcpConnection[] order = await ReturnTwoThenRentAsync(ClickHouseTcpPoolReusePolicy.Fifo);

        Assert.That(order[2], Is.SameAs(order[0]));
    }

    [Test]
    public async Task RentAsync_IdleConnectionPastItsLifetime_IsClosedAndReplaced()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxConnectionLifetime: TimeSpan.FromMinutes(10)), factory, clock);

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            first = lease.Connection;
        }

        clock.Advance(TimeSpan.FromMinutes(11));

        await using IConnectionLease second = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Connection, Is.Not.SameAs(first));
            Assert.That(first.State, Is.EqualTo(TcpConnectionState.Terminated), "the retired connection must be closed, not just dropped");
        });
    }

    [Test]
    public async Task RentAsync_IdleConnectionJustInsideItsLifetime_IsStillHandedOut()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxConnectionLifetime: TimeSpan.FromMinutes(10)), factory, clock);

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            first = lease.Connection;
        }

        // The limit is the whole limit: a connection is reusable right up to it, and an operation started here
        // simply carries it past — the pool never interrupts a running query.
        clock.Advance(TimeSpan.FromMinutes(10) - TimeSpan.FromSeconds(1));

        await using IConnectionLease second = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Connection, Is.SameAs(first));
            Assert.That(factory.CreateCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RentAsync_LifetimeDisabled_KeepsReusingTheSameConnection()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxConnectionLifetime: TimeSpan.Zero, idleTimeout: TimeSpan.Zero), factory, clock);

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            first = lease.Connection;
        }

        clock.Advance(TimeSpan.FromDays(7));

        await using IConnectionLease second = await pool.RentAsync(None);
        Assert.That(second.Connection, Is.SameAs(first));
    }

    [Test]
    public async Task Return_ConnectionAlreadyPastItsLifetime_IsClosedRatherThanPooled()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxConnectionLifetime: TimeSpan.FromMinutes(10)), factory, clock);

        IConnectionLease lease = await pool.RentAsync(None);
        clock.Advance(TimeSpan.FromMinutes(11));
        await lease.DisposeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pool.IdleCount, Is.Zero);
            Assert.That(lease.Connection.State, Is.EqualTo(TcpConnectionState.Terminated));
        });
    }

    [Test]
    public async Task Lease_DisposedTwice_ReturnsTheConnectionOnlyOnce()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: 1), factory, new ControlledTimeProvider());

        IConnectionLease lease = await pool.RentAsync(None);
        await lease.DisposeAsync();
        await lease.DisposeAsync();

        Assert.That(pool.IdleCount, Is.EqualTo(1), "a double dispose must not pool the connection twice");

        // A second release of the slot would let two callers hold the one connection at once.
        await using IConnectionLease held = await pool.RentAsync(None);
        ValueTask<IConnectionLease> queued = pool.RentAsync(None);
        Assert.That(queued.IsCompleted, Is.False);

        await held.DisposeAsync();
        await using IConnectionLease drained = await queued;
        Assert.That(drained, Is.Not.Null);
    }

    [Test]
    public async Task PooledConnection_Checkouts_CountUsagesAndCarryTheNegotiatedProtocol()
    {
        var clock = new ControlledTimeProvider();
        using ClickHouseTcpConnection connection = await FakeConnectionFactory.CreateReadyAsync();
        var pooled = new PooledConnection(connection, clock);

        Assert.That(pooled.UsageCount, Is.Zero);
        pooled.OnRented();
        pooled.OnRented();

        Assert.Multiple(() =>
        {
            Assert.That(pooled.UsageCount, Is.EqualTo(2));

            // The scripted server offers a higher revision than the client speaks, so the negotiated one is ours.
            Assert.That(pooled.ProtocolVersion, Is.EqualTo(NegotiatedProtocol.ClientTcpProtocolVersion));
        });
    }

    [Test]
    public async Task PooledConnection_IdleClock_RestartsWhenTheConnectionIsReturned()
    {
        var clock = new ControlledTimeProvider();
        using ClickHouseTcpConnection connection = await FakeConnectionFactory.CreateReadyAsync();
        var pooled = new PooledConnection(connection, clock);

        clock.Advance(TimeSpan.FromMinutes(3));
        Assert.That(pooled.IdleFor, Is.EqualTo(TimeSpan.FromMinutes(3)));

        pooled.OnReturned();

        Assert.Multiple(() =>
        {
            Assert.That(pooled.IdleFor, Is.EqualTo(TimeSpan.Zero), "a returned connection starts its idle clock over");
            Assert.That(pooled.Age, Is.EqualTo(TimeSpan.FromMinutes(3)), "age is measured from opening and does not restart");
        });
    }

    [Test]
    public async Task Sweep_IdleConnectionsPastTheIdleTimeout_AreClosedDownToMinPoolSize()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 1, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        await ReturnConcurrentLeasesAsync(pool, 3);
        clock.Advance(TimeSpan.FromMinutes(6));

        pool.Sweep();

        Assert.Multiple(() =>
        {
            Assert.That(pool.IdleCount, Is.EqualTo(1), "the floor keeps one connection warm");
            Assert.That(factory.Created.Count(c => c.State == TcpConnectionState.Terminated), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Sweep_IdleConnectionsPastTheirLifetime_AreClosedEvenBelowMinPoolSize()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 3, maxConnectionLifetime: TimeSpan.FromMinutes(10)), factory, clock);

        await ReturnConcurrentLeasesAsync(pool, 3);
        clock.Advance(TimeSpan.FromMinutes(11));

        pool.Sweep();

        Assert.That(pool.IdleCount, Is.Zero, "age is a correctness limit, so the floor must not hold an over-age connection");
    }

    [Test]
    public async Task Sweep_ConnectionsWithinTheirLimits_AreLeftAlone()
    {
        var clock = new ControlledTimeProvider();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 3, idleTimeout: TimeSpan.FromMinutes(5), maxConnectionLifetime: TimeSpan.FromMinutes(30)),
            new FakeConnectionFactory(),
            clock);

        await ReturnConcurrentLeasesAsync(pool, 3);
        clock.Advance(TimeSpan.FromMinutes(4));

        pool.Sweep();

        Assert.That(pool.IdleCount, Is.EqualTo(3));
    }

    [Test]
    public async Task Sweep_IdleTimeoutDisabled_KeepsEveryIdleConnection()
    {
        var clock = new ControlledTimeProvider();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 3, idleTimeout: TimeSpan.Zero, maxConnectionLifetime: TimeSpan.FromHours(10)),
            new FakeConnectionFactory(),
            clock);

        await ReturnConcurrentLeasesAsync(pool, 3);
        clock.Advance(TimeSpan.FromHours(1));

        pool.Sweep();

        Assert.That(pool.IdleCount, Is.EqualTo(3));
    }

    [Test]
    public async Task Sweep_TrimmingToTheFloor_KeepsTheMostRecentlyUsedConnections()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 3, minPoolSize: 1, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        // Returned oldest-first, then aged so all three are past the idle timeout.
        IConnectionLease[] leases = [await pool.RentAsync(None), await pool.RentAsync(None), await pool.RentAsync(None)];
        foreach (IConnectionLease lease in leases)
        {
            await lease.DisposeAsync();
            clock.Advance(TimeSpan.FromMinutes(1));
        }

        clock.Advance(TimeSpan.FromMinutes(5));
        pool.Sweep();

        await using IConnectionLease survivor = await pool.RentAsync(None);
        Assert.That(survivor.Connection, Is.SameAs(leases[2].Connection), "the coldest connections go first");
    }

    [Test]
    public async Task SweepQuietly_ClosingAConnectionThrows_SwallowsItRatherThanFaultingTheTimerThread()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory { ClosingThrows = true };
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 1, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        await ReturnConcurrentLeasesAsync(pool, 1);
        clock.Advance(TimeSpan.FromMinutes(6));

        // The guard is load-bearing, so prove there is really something to guard: the sweep itself throws here,
        // and on the timer's thread-pool thread that would be an unhandled exception, not a failed sweep.
        Assert.Throws<IOException>(pool.Sweep);

        await ReturnConcurrentLeasesAsync(pool, 1);
        clock.Advance(TimeSpan.FromMinutes(6));

        Assert.DoesNotThrow(pool.SweepQuietly);
    }

    [Test]
    public async Task Sweep_AfterDispose_DoesNothing()
    {
        var pool = new ConnectionPool(Options(), new FakeConnectionFactory(), new ControlledTimeProvider());
        await pool.DisposeAsync();

        Assert.DoesNotThrow(pool.Sweep);
    }

    [TestCase(0, 0, 0, TestName = "SweepInterval_NoLimitsSet_IsZeroSoNoSweepRuns")]
    [TestCase(0, 1800, 30, TestName = "SweepInterval_LongLimit_IsCappedAtThirtySeconds")]
    [TestCase(40, 0, 10, TestName = "SweepInterval_ModerateLimit_IsAQuarterOfIt")]
    [TestCase(2, 0, 1, TestName = "SweepInterval_ShortLimit_IsFlooredAtOneSecond")]
    [TestCase(3600, 60, 15, TestName = "SweepInterval_TwoLimits_FollowsTheShorterOne")]
    public void SweepInterval_ForTheLimitsInForce_IsClampedToTheExpectedPeriod(int idleSeconds, int lifetimeSeconds, int expectedSeconds)
    {
        var options = new ClickHouseTcpClientOptions
        {
            IdleTimeout = TimeSpan.FromSeconds(idleSeconds),
            MaxConnectionLifetime = TimeSpan.FromSeconds(lifetimeSeconds),
        };

        Assert.That(ConnectionPool.SweepInterval(options), Is.EqualTo(TimeSpan.FromSeconds(expectedSeconds)));
    }

    [Test]
    public async Task DisposeAsync_ClosesEveryIdleConnection()
    {
        var factory = new FakeConnectionFactory();
        var pool = new ConnectionPool(Options(maxPoolSize: 3), factory, new ControlledTimeProvider());
        await ReturnConcurrentLeasesAsync(pool, 3);

        await pool.DisposeAsync();

        Assert.That(factory.Created.Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task DisposeAsync_CalledTwice_IsNoOp()
    {
        var pool = new ConnectionPool(Options(), new FakeConnectionFactory(), new ControlledTimeProvider());
        await pool.DisposeAsync();

        Assert.DoesNotThrowAsync(async () => await pool.DisposeAsync());
    }

    [Test]
    public async Task RentAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var pool = new ConnectionPool(Options(), new FakeConnectionFactory(), new ControlledTimeProvider());
        await pool.DisposeAsync();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await pool.RentAsync(None));
    }

    [Test]
    public void RentAsync_PreCancelledToken_ThrowsWithoutOpeningAConnection()
    {
        var factory = new FakeConnectionFactory();
        var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () => await pool.RentAsync(cts.Token));
        Assert.That(factory.CreateCount, Is.Zero);
    }

    [Test]
    public async Task DisposeAsync_WithALeaseStillOut_WaitsForItAndThenClosesTheConnection()
    {
        var factory = new FakeConnectionFactory();
        var pool = new ConnectionPool(Options(maxPoolSize: 1), factory, new ControlledTimeProvider());
        IConnectionLease lease = await pool.RentAsync(None);

        ValueTask disposal = pool.DisposeAsync();
        Assert.That(disposal.IsCompleted, Is.False, "disposal must wait for the operation still running");

        await lease.DisposeAsync();
        await disposal;

        Assert.That(lease.Connection.State, Is.EqualTo(TcpConnectionState.Terminated), "a connection returned to a disposed pool is closed, not kept");
    }

    [Test]
    public async Task DisposeAsync_WithALeaseNeverReturned_GivesUpAfterPoolTimeout()
    {
        var factory = new FakeConnectionFactory();
        var pool = new ConnectionPool(
            Options(maxPoolSize: 1, poolTimeout: TimeSpan.FromMilliseconds(150)), factory, new ControlledTimeProvider());

        // Rented and deliberately never disposed: the caller abandoned the operation.
        await pool.RentAsync(None);

        var elapsed = Stopwatch.StartNew();
        await pool.DisposeAsync();
        elapsed.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(elapsed.Elapsed, Is.LessThan(TimeSpan.FromSeconds(10)), "disposal must not hang on an abandoned lease");

            // Nothing else can reach that connection, so leaving it open would leak the socket for good.
            Assert.That(factory.Created[0].State, Is.EqualTo(TcpConnectionState.Terminated));
        });
    }

    [Test]
    public async Task RentAsync_ManyCallersAtOnce_NeverExceedsMaxPoolSizeAndNeverSharesAConnection()
    {
        // The properties the design leans on hardest — the cap, permit accounting, and one connection to one
        // caller — are the ones a single-threaded test cannot reach. 200 checkouts over 32 tasks contend hard
        // enough on the semaphore and the idle list to expose an accounting slip.
        const int MaxPoolSize = 4;
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: MaxPoolSize), factory, new ControlledTimeProvider());

        var live = new HashSet<ClickHouseTcpConnection>();
        var failures = new List<string>();
        int concurrent = 0;
        int peak = 0;

        await Task.WhenAll(Enumerable.Range(0, 32).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 25; i++)
            {
                IConnectionLease lease = await pool.RentAsync(None);

                // Registered for exactly the window the lease is held: after the rent, and released immediately
                // before the dispose that gives the connection back.
                int now = Interlocked.Increment(ref concurrent);
                lock (live)
                {
                    peak = Math.Max(peak, now);
                    if (!live.Add(lease.Connection))
                    {
                        failures.Add("two leases held the same connection at once");
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

                    Interlocked.Decrement(ref concurrent);
                    await lease.DisposeAsync();
                }
            }
        })));

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty);
            Assert.That(peak, Is.LessThanOrEqualTo(MaxPoolSize), "more callers held a connection at once than the cap allows");
            Assert.That(factory.CreateCount, Is.LessThanOrEqualTo(MaxPoolSize), "open connections must never exceed the cap either");
            Assert.That(pool.IdleCount, Is.LessThanOrEqualTo(MaxPoolSize));
        });

        // Every slot must have come back: holding the full cap at once would block if even one permit leaked.
        var all = new List<IConnectionLease>();
        try
        {
            for (int i = 0; i < MaxPoolSize; i++)
            {
                all.Add(await pool.RentAsync(None));
            }
        }
        finally
        {
            foreach (IConnectionLease lease in all)
            {
                await lease.DisposeAsync();
            }
        }

        Assert.That(all, Has.Count.EqualTo(MaxPoolSize));
    }

    private static async Task<ClickHouseTcpConnection[]> ReturnTwoThenRentAsync(ClickHouseTcpPoolReusePolicy policy)
    {
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 2, reusePolicy: policy), new FakeConnectionFactory(), new ControlledTimeProvider());

        // Held together so two distinct connections exist, then returned in a known order.
        IConnectionLease first = await pool.RentAsync(None);
        IConnectionLease second = await pool.RentAsync(None);
        await first.DisposeAsync();
        await second.DisposeAsync();

        await using IConnectionLease next = await pool.RentAsync(None);
        return [first.Connection, second.Connection, next.Connection];
    }

    /// <summary>Rents <paramref name="count"/> connections at once, so each is distinct, then returns them all.</summary>
    private static async Task ReturnConcurrentLeasesAsync(ConnectionPool pool, int count)
    {
        var leases = new List<IConnectionLease>(count);
        for (int i = 0; i < count; i++)
        {
            leases.Add(await pool.RentAsync(None));
        }

        foreach (IConnectionLease lease in leases)
        {
            await lease.DisposeAsync();
        }
    }
}
