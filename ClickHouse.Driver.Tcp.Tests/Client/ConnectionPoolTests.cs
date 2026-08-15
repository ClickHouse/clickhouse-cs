using System;
using System.Collections.Generic;
using System.Diagnostics;
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
    public async Task RentAsync_IdleConnectionExactlyAtItsLifetime_IsRetired()
    {
        // The boundary the >= comparison decides. A controlled clock can land on it exactly, which no wall clock
        // could, so it is worth pinning rather than leaving to the two tests either side of it.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxConnectionLifetime: TimeSpan.FromMinutes(10)), factory, clock);

        await using (await pool.RentAsync(None))
        {
        }

        clock.Advance(TimeSpan.FromMinutes(10));

        await using IConnectionLease second = await pool.RentAsync(None);
        Assert.That(factory.CreateCount, Is.EqualTo(2));
    }

    [Test]
    public async Task Return_ClosingTheConnectionThrows_StillReleasesTheSlotAndDoesNotFailTheCaller()
    {
        // A teardown failure must not escape into the operation that was just finishing, nor cost the pool a
        // slot — which would shrink it by one for the rest of the process.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory { ClosingThrows = true };
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 1, maxConnectionLifetime: TimeSpan.FromMinutes(10)), factory, clock);

        IConnectionLease lease = await pool.RentAsync(None);
        clock.Advance(TimeSpan.FromMinutes(11));

        Assert.DoesNotThrowAsync(async () => await lease.DisposeAsync());

        // The slot came back, so the next checkout does not wait for the pool timeout.
        await using IConnectionLease next = await pool.RentAsync(None);
        Assert.That(next.Connection.State, Is.EqualTo(TcpConnectionState.Ready));
    }

    [Test]
    public async Task DisposeAsync_ClosingAConnectionThrows_StillClosesTheRest()
    {
        // TakeAllIdle empties the set before closing, so an exception escaping the loop would strand every
        // connection after the failing one with nothing able to reach them.
        var factory = new FakeConnectionFactory { ClosingThrows = true };
        var pool = new ConnectionPool(Options(maxPoolSize: 3), factory, new ControlledTimeProvider());
        await ReturnConcurrentLeasesAsync(pool, 3);

        Assert.DoesNotThrowAsync(async () => await pool.DisposeAsync());

        Assert.That(factory.Created.Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task RentAsync_StaleIdleConnectionFailsToClose_StillServesTheCaller()
    {
        // The discard happens on an unrelated caller's checkout path, so a teardown failure there would fail an
        // operation that has nothing to do with the dead connection.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory { ClosingThrows = true };
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 2, maxConnectionLifetime: TimeSpan.FromMinutes(10)), factory, clock);

        await using (await pool.RentAsync(None))
        {
        }

        clock.Advance(TimeSpan.FromMinutes(11));

        IConnectionLease replacement = null;
        Assert.DoesNotThrowAsync(async () => replacement = await pool.RentAsync(None));
        await using (replacement)
        {
            Assert.That(replacement.Connection.State, Is.EqualTo(TcpConnectionState.Ready));
        }
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
    public async Task Sweep_IdleConnectionsPastTheirLifetime_AreRetiredDespiteTheFloorAndReplaced()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 3, maxConnectionLifetime: TimeSpan.FromMinutes(10)), factory, clock);

        await ReturnConcurrentLeasesAsync(pool, 3);
        ClickHouseTcpConnection[] original = [.. factory.Created];
        clock.Advance(TimeSpan.FromMinutes(11));

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.Multiple(() =>
        {
            // Age is a correctness limit: the floor must not hold an over-age connection open.
            Assert.That(original.Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Terminated));

            // But the floor is still a floor, so the sweep opens fresh ones to replace them.
            Assert.That(pool.IdleCount, Is.EqualTo(3));
            Assert.That(factory.CreateCount, Is.EqualTo(6));
        });
    }

    [Test]
    public async Task Sweep_PoolBelowTheFloor_OpensConnectionsUpToIt()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 2), factory, new ControlledTimeProvider());

        // Nothing has been asked of the pool yet, so it holds nothing.
        Assert.That(factory.CreateCount, Is.Zero);

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.Multiple(() =>
        {
            Assert.That(pool.IdleCount, Is.EqualTo(2));
            Assert.That(factory.CreateCount, Is.EqualTo(2), "the floor is a floor, not a target to overshoot");
        });
    }

    [Test]
    public async Task Sweep_PoolAlreadyAtTheFloor_OpensNothing()
    {
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 2), factory, new ControlledTimeProvider());

        await ReturnConcurrentLeasesAsync(pool, 2);

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.That(factory.CreateCount, Is.EqualTo(2));
    }

    [Test]
    public async Task Sweep_FloorPartlyMetByConnectionsInUse_OpensOnlyTheDifference()
    {
        // MinPoolSize counts open connections, not spare ones, so one in use already covers half a floor of two.
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 2), factory, new ControlledTimeProvider());

        await using IConnectionLease held = await pool.RentAsync(None);

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreateCount, Is.EqualTo(2));
            Assert.That(pool.IdleCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Sweep_EverySlotInUse_SkipsTheTopUp()
    {
        // A saturated pool needs no warming, and the top-up must never queue ahead of a real caller for a slot.
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 2, minPoolSize: 2), factory, new ControlledTimeProvider());

        await using IConnectionLease first = await pool.RentAsync(None);
        await using IConnectionLease second = await pool.RentAsync(None);

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.That(factory.CreateCount, Is.EqualTo(2), "the floor is already met by the two in use");
    }

    [Test]
    public async Task Sweep_NoFloorConfigured_NeverOpensAnything()
    {
        // The default. A pool nobody has used must stay at zero connections.
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 0), factory, new ControlledTimeProvider());

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.Multiple(() =>
        {
            Assert.That(pool.LastRefill, Is.Null, "no floor means no top-up is even started");
            Assert.That(factory.CreateCount, Is.Zero);
        });
    }

    [Test]
    public async Task Sweep_TopUpDialFails_LeavesThePoolUsableAndRetriesOnTheNextSweep()
    {
        var factory = new FakeConnectionFactory { FailNextWith = new TimeoutException("connect timed out") };
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 2), factory, new ControlledTimeProvider());

        // Nobody is waiting on a top-up, so a failed dial has nowhere to report and must not escape.
        pool.Sweep();
        Assert.DoesNotThrowAsync(async () => await (pool.LastRefill ?? Task.CompletedTask));
        Assert.That(pool.IdleCount, Is.Zero);

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.That(pool.IdleCount, Is.EqualTo(2), "the next sweep tries again rather than giving up on the floor");
    }

    [Test]
    public async Task Sweep_TopUpAlreadyRunning_DoesNotStartASecondToFillTheSameGap()
    {
        // Sweeps fire on a timer and a top-up dials, so a second sweep can easily land while the first is still
        // opening connections. Two of them would both see the same shortfall and overshoot the floor.
        var dialing = new TaskCompletionSource();
        var factory = new FakeConnectionFactory { BeforeCreate = _ => dialing.Task };
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 2), factory, new ControlledTimeProvider());

        pool.Sweep();
        Task first = pool.LastRefill;
        Assert.That(first, Is.Not.Null, "the pool is below the floor, so a top-up must have started");

        // The first is parked mid-dial. The flag is set before the task is queued, so this is not a race.
        Assert.That(pool.StartRefillIfBelowFloor(), Is.Null);

        dialing.SetResult();
        await first;

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreateCount, Is.EqualTo(2));
            Assert.That(pool.IdleCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Sweep_AfterDispose_DoesNotTopUpTheFloor()
    {
        var factory = new FakeConnectionFactory();
        var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 2), factory, new ControlledTimeProvider());
        await pool.DisposeAsync();

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.That(factory.CreateCount, Is.Zero);
    }

    [Test]
    public async Task Sweep_IdleConnectionsPastTheIdleTimeoutWithConnectionsInUse_CountsBothAgainstTheFloor()
    {
        // Two in use plus two idle, against a floor of three: only one idle connection is surplus.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 3, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        IConnectionLease[] leases = [await pool.RentAsync(None), await pool.RentAsync(None)];
        await ReturnConcurrentLeasesAsync(pool, 2);
        clock.Advance(TimeSpan.FromMinutes(6));

        try
        {
            pool.Sweep();
            await (pool.LastRefill ?? Task.CompletedTask);

            Assert.That(pool.IdleCount, Is.EqualTo(1), "the two in use count toward the floor of three");
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
    public async Task Sweep_ClosingOneConnectionThrows_StillReapsTheWholeBatch()
    {
        // The reaped connections leave the idle set before any is closed, so an exception escaping the close
        // loop would strand the rest with nothing able to reach them. The sweep runs on a timer, so it would
        // also be an unhandled exception on a thread-pool thread rather than a failed sweep.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory { ClosingThrows = true };
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 3, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        await ReturnConcurrentLeasesAsync(pool, 3);
        clock.Advance(TimeSpan.FromMinutes(6));

        Assert.DoesNotThrow(pool.Sweep);

        Assert.Multiple(() =>
        {
            Assert.That(pool.IdleCount, Is.Zero);
            Assert.That(factory.Created.Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Terminated));
        });
    }

    [Test]
    public async Task SweepQuietly_WhateverTheSweepDoes_NeverThrowsAtTheTimer()
    {
        // The outer guard the timer calls. Each individual close already swallows, so this is defence in depth
        // for anything else a future sweep might do: a timer callback has no one to catch for it, and an escape
        // ends the process rather than the sweep.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory { ClosingThrows = true };
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 2, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        await ReturnConcurrentLeasesAsync(pool, 2);
        clock.Advance(TimeSpan.FromMinutes(6));

        Assert.DoesNotThrow(pool.SweepQuietly);
        Assert.That(pool.IdleCount, Is.Zero);
    }

    [Test]
    public async Task Sweep_AfterDispose_DoesNothing()
    {
        var pool = new ConnectionPool(Options(), new FakeConnectionFactory(), new ControlledTimeProvider());
        await pool.DisposeAsync();

        Assert.DoesNotThrow(pool.Sweep);
    }

    [Test]
    public void SweepInterval_NoLimitsButAFloorToHold_StillSweeps()
    {
        // Nothing expires, but the sweep is the only thing that tops the pool back up, so it still has to run.
        var options = new ClickHouseTcpClientOptions
        {
            IdleTimeout = TimeSpan.Zero,
            MaxConnectionLifetime = TimeSpan.Zero,
            MinPoolSize = 2,
        };

        Assert.That(ConnectionPool.SweepInterval(options), Is.EqualTo(TimeSpan.FromSeconds(30)));
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

            // Nothing else can reach that connection, so leaving it open would leak the socket for good. The
            // transport is aborted rather than fully torn down, because the operation may still hold buffers.
            Assert.That(factory.Created[0].State, Is.EqualTo(TcpConnectionState.Terminated));
            Assert.That(factory.Created[0].IsReusable, Is.False);
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

    [Test]
    public async Task RentAsync_WhileTopUpsRunConcurrently_StillNeverExceedsMaxPoolSize()
    {
        // The top-up is a third party competing for permits, so the cap now depends on it playing by the same
        // rules as a checkout. The stress case above runs without a floor and so never exercises that.
        const int MaxPoolSize = 3;
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: MaxPoolSize, minPoolSize: MaxPoolSize, poolTimeout: TimeSpan.FromMinutes(2)),
            factory,
            new ControlledTimeProvider());

        var live = new HashSet<ClickHouseTcpConnection>();
        var failures = new List<string>();
        var sweeping = Task.Run(async () =>
        {
            // Sweeps fire on a timer in production; here they are driven as fast as the renters run.
            for (int i = 0; i < 200; i++)
            {
                pool.Sweep();
                await (pool.LastRefill ?? Task.CompletedTask);
            }
        });

        await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 25; i++)
            {
                IConnectionLease lease = await pool.RentAsync(None);
                lock (live)
                {
                    if (!live.Add(lease.Connection))
                    {
                        failures.Add("a top-up handed out a connection that was already leased");
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

        await sweeping;

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty);
            Assert.That(factory.CreateCount, Is.LessThanOrEqualTo(MaxPoolSize), "the floor must not open past the cap");
            Assert.That(pool.IdleCount, Is.LessThanOrEqualTo(MaxPoolSize));
        });
    }

    [Test]
    public async Task Sweep_TopUpReassessesWhileCheckoutsAreDialing_CountsThemAndStopsAtTheCap()
    {
        // The stress test above overlaps a top-up with checkouts only by luck, since a scripted dial finishes
        // almost at once. Here every dial is held open by hand, so the interleaving that overshoots is the one
        // that runs: the top-up finishes one connection and looks again while two checkouts are still dialing,
        // and the connections those are about to add are in neither set for it to count.
        const int Size = 3;
        var gates = new[] { new TaskCompletionSource(), new TaskCompletionSource(), new TaskCompletionSource() };
        var started = new SemaphoreSlim(0);
        int dials = -1;
        var factory = new FakeConnectionFactory
        {
            BeforeCreate = async _ =>
            {
                int mine = Interlocked.Increment(ref dials);
                started.Release();

                // Only the first three are held. A fourth is the overshoot itself, and letting it run makes that
                // a failed assertion below rather than a hang.
                if (mine < gates.Length)
                {
                    await gates[mine].Task;
                }
            },
        };

        await using var pool = new ConnectionPool(
            Options(maxPoolSize: Size, minPoolSize: Size), factory, new ControlledTimeProvider());

        // Dialing in a known order: the top-up first, so it is the one that gets to reassess mid-round.
        pool.Sweep();
        Task refill = pool.LastRefill;
        Assert.That(refill, Is.Not.Null, "an empty pool is below the floor, so a top-up must have started");
        await DialStartedAsync();

        Task<IConnectionLease> first = Task.Run(async () => await pool.RentAsync(None));
        await DialStartedAsync();
        Task<IConnectionLease> second = Task.Run(async () => await pool.RentAsync(None));
        await DialStartedAsync();

        // Three dials are in flight and the pool itself still holds nothing. Letting the top-up's finish leaves
        // it looking at one connection against a floor of three — which is where it has to count the other two.
        gates[0].SetResult();
        await refill;

        Assert.That(factory.CreateCount, Is.EqualTo(1), "the two checkouts already dialing fill the rest of the floor");

        gates[1].SetResult();
        gates[2].SetResult();
        IConnectionLease[] leases = [await first, await second];
        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(factory.CreateCount, Is.EqualTo(Size), "more connections were opened than the cap allows");
                Assert.That(pool.IdleCount, Is.EqualTo(1), "the top-up's one connection, the checkouts holding the other two");
            });
        }
        finally
        {
            foreach (IConnectionLease lease in leases)
            {
                await lease.DisposeAsync();
            }
        }

        async Task DialStartedAsync()
            => Assert.That(await started.WaitAsync(TimeSpan.FromSeconds(30)), Is.True, "a dial should have started");
    }

    [Test]
    public async Task DisposeAsync_WhileACheckoutIsStillDialing_EndsThatDialRatherThanWaitingItOut()
    {
        // A dial in flight is the one connection disposal cannot reach any other way: its caller is not in the
        // leased set, so the abort at the end of the drain does not see it. Unlinked, it would hold its permit —
        // and, once connected, its socket — for as long as DialTimeout allows, well after the pool closed.
        var pool = new ConnectionPool(Options(), DialsForeverFactory(out SemaphoreSlim dialing), new ControlledTimeProvider());
        Task<IConnectionLease> renting = Task.Run(async () => await pool.RentAsync(None));
        Assert.That(await dialing.WaitAsync(TimeSpan.FromSeconds(30)), Is.True, "the dial should be in flight");

        var elapsed = Stopwatch.StartNew();
        await pool.DisposeAsync();
        elapsed.Stop();

        // WhenAny rather than an await, so a dial disposal failed to end fails the test instead of hanging it.
        Task finished = await Task.WhenAny(renting, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.That(finished, Is.SameAs(renting), "the caller must be released, not left in a dial nothing can reach");

        ObjectDisposedException thrown = Assert.ThrowsAsync<ObjectDisposedException>(async () => await renting);
        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain("ClickHouseTcpClient"), "the caller hears about the client, not the pool");
            Assert.That(elapsed.Elapsed, Is.LessThan(TimeSpan.FromSeconds(10)), "disposal must not wait out a dial it can cancel");
        });
    }

    [Test]
    public async Task RentAsync_CallerCancelsWhileDialing_ReportsCancellationRatherThanDisposal()
    {
        // The pool's shutdown token now rides along with the caller's, so the two have to stay distinguishable:
        // a caller who cancelled must not be told the client was disposed and go looking for a bug they do not
        // have. The slot has to come back either way.
        var factory = DialsForeverFactory(out SemaphoreSlim dialing);
        await using var pool = new ConnectionPool(Options(maxPoolSize: 1), factory, new ControlledTimeProvider());

        using var cts = new CancellationTokenSource();
        Task<IConnectionLease> renting = Task.Run(async () => await pool.RentAsync(cts.Token));
        Assert.That(await dialing.WaitAsync(TimeSpan.FromSeconds(30)), Is.True, "the dial should be in flight");

        cts.Cancel();
        Assert.CatchAsync<OperationCanceledException>(async () => await renting);

        // The pool's only slot: a checkout that gets it proves the cancelled dial gave back both its permit and
        // the capacity it had reserved.
        factory.BeforeCreate = null;
        await using IConnectionLease next = await pool.RentAsync(None);
        Assert.That(next.Connection.State, Is.EqualTo(TcpConnectionState.Ready));
    }

    [Test]
    public async Task RentAsync_PoolDisposedWhileTheDialFinishes_ClosesTheConnectionRatherThanHandingItOut()
    {
        // The other side of the checkout race: a dial that cannot be called off and lands just after the drain
        // emptied the leased set. Nothing else can reach that connection — it was never in either set — so the
        // checkout has to close it itself rather than hand out a connection from a pool that is already shut.
        FakeConnectionFactory factory = GatedDialFactory(out SemaphoreSlim dialing, out TaskCompletionSource finishDial);
        var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        Task<IConnectionLease> renting = Task.Run(async () => await pool.RentAsync(None));
        Assert.That(await dialing.WaitAsync(TimeSpan.FromSeconds(30)), Is.True, "the dial should be in flight");

        // Deliberately not awaited: disposal marks the pool disposed before its first await, and its drain then
        // waits for the permit this checkout still holds.
        Task disposing = pool.DisposeAsync().AsTask();
        finishDial.SetResult();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await renting, "a pool that is closing must not hand out a connection");
        await disposing;

        Assert.Multiple(() =>
        {
            Assert.That(factory.CreateCount, Is.EqualTo(1), "the dial did finish; what happens to its connection is the point");
            Assert.That(factory.Created[0].State, Is.EqualTo(TcpConnectionState.Terminated), "the connection has no other owner, so the checkout must close it");
        });
    }

    [Test]
    public async Task Sweep_PoolDisposedWhileATopUpDialFinishes_ClosesThatConnectionRatherThanPoolingIt()
    {
        // The same race on the top-up path, where the connection would otherwise be added to an idle set the
        // drain has already emptied — open, unreachable, and closed by nothing.
        FakeConnectionFactory factory = GatedDialFactory(out SemaphoreSlim dialing, out TaskCompletionSource finishDial);
        var pool = new ConnectionPool(Options(maxPoolSize: 2, minPoolSize: 1), factory, new ControlledTimeProvider());

        pool.Sweep();
        Task refill = pool.LastRefill;
        Assert.That(refill, Is.Not.Null, "an empty pool is below the floor, so a top-up must have started");
        Assert.That(await dialing.WaitAsync(TimeSpan.FromSeconds(30)), Is.True, "the top-up should be dialing");

        Task disposing = pool.DisposeAsync().AsTask();
        finishDial.SetResult();

        await refill;
        await disposing;

        Assert.Multiple(() =>
        {
            Assert.That(pool.IdleCount, Is.Zero, "a connection opened after the drain must not join the idle set");
            Assert.That(factory.Created[0].State, Is.EqualTo(TcpConnectionState.Terminated), "the top-up must close what it can no longer pool");
        });
    }

    /// <summary>
    /// A factory whose dials finish only when <paramref name="finishDial"/> is set, and that ignore cancellation
    /// on the way: these tests need a dial that runs to completion at an awkward moment, not one called off.
    /// </summary>
    private static FakeConnectionFactory GatedDialFactory(out SemaphoreSlim dialing, out TaskCompletionSource finishDial)
    {
        var started = new SemaphoreSlim(0);
        var gate = new TaskCompletionSource();
        dialing = started;
        finishDial = gate;
        return new FakeConnectionFactory
        {
            IgnoresCancellation = true,
            BeforeCreate = async _ =>
            {
                started.Release();
                await gate.Task;
            },
        };
    }

    /// <summary>A factory whose dials only ever end by cancellation, signalling as each one starts.</summary>
    private static FakeConnectionFactory DialsForeverFactory(out SemaphoreSlim dialing)
    {
        var started = new SemaphoreSlim(0);
        dialing = started;
        return new FakeConnectionFactory
        {
            BeforeCreate = async ct =>
            {
                started.Release();
                await Task.Delay(Timeout.Infinite, ct);
            },
        };
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
