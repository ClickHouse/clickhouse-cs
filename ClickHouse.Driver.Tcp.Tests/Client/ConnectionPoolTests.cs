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
        // The idle limit is off throughout the lifetime tests: both limits bar a checkout, so leaving idleness on
        // while ageing an idle connection past ten minutes would let either one explain the result.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxConnectionLifetime: TimeSpan.FromMinutes(10), idleTimeout: TimeSpan.Zero), factory, clock);

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
            Options(maxConnectionLifetime: TimeSpan.FromMinutes(10), idleTimeout: TimeSpan.Zero), factory, clock);

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
            Options(maxConnectionLifetime: TimeSpan.FromMinutes(10), idleTimeout: TimeSpan.Zero), factory, clock);

        await using (await pool.RentAsync(None))
        {
        }

        clock.Advance(TimeSpan.FromMinutes(10));

        await using IConnectionLease second = await pool.RentAsync(None);
        Assert.That(factory.CreateCount, Is.EqualTo(2));
    }

    [Test]
    public async Task RentAsync_IdleConnectionPastTheIdleTimeout_IsClosedAndReplaced()
    {
        // Idleness bars a checkout exactly as age does. A connection nobody used is what a proxy or load balancer
        // between client and server drops, and such a drop can arrive without a FIN — in which case IsReusable
        // still says yes and the operation sent over it stalls until TCP gives up.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            first = lease.Connection;
        }

        clock.Advance(TimeSpan.FromMinutes(6));

        await using IConnectionLease second = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Connection, Is.Not.SameAs(first));
            Assert.That(first.State, Is.EqualTo(TcpConnectionState.Terminated), "the retired connection must be closed, not just dropped");
        });
    }

    [Test]
    public async Task RentAsync_IdleConnectionJustInsideTheIdleTimeout_IsStillHandedOut()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            first = lease.Connection;
        }

        clock.Advance(TimeSpan.FromMinutes(5) - TimeSpan.FromSeconds(1));

        await using IConnectionLease second = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Connection, Is.SameAs(first));
            Assert.That(factory.CreateCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RentAsync_IdleConnectionExactlyAtTheIdleTimeout_IsRetired()
    {
        // The boundary the >= comparison decides, which only a controlled clock can land on.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        await using (await pool.RentAsync(None))
        {
        }

        clock.Advance(TimeSpan.FromMinutes(5));

        await using IConnectionLease second = await pool.RentAsync(None);
        Assert.That(factory.CreateCount, Is.EqualTo(2));
    }

    [Test]
    public async Task RentAsync_IdleTimeoutDisabled_ReusesAConnectionThatSatIdleForHours()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(idleTimeout: TimeSpan.Zero, maxConnectionLifetime: TimeSpan.FromDays(30)), factory, clock);

        ClickHouseTcpConnection first;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            first = lease.Connection;
        }

        clock.Advance(TimeSpan.FromHours(10));

        await using IConnectionLease second = await pool.RentAsync(None);
        Assert.That(second.Connection, Is.SameAs(first));
    }

    [Test]
    public async Task Return_OperationRanLongerThanTheIdleTimeout_StillPoolsTheConnection()
    {
        // The idle clock stops while a connection is checked out, and it has to: the return path applies the same
        // expiry test a checkout does, so a clock that kept running would read a long query's own duration as
        // idleness and retire the connection that had just run it successfully.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(idleTimeout: TimeSpan.FromMinutes(5), maxConnectionLifetime: TimeSpan.FromHours(1)), factory, clock);

        ClickHouseTcpConnection connection;
        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            connection = lease.Connection;

            // A half-hour analytical query, five times the idle timeout.
            clock.Advance(TimeSpan.FromMinutes(30));
        }

        Assert.Multiple(() =>
        {
            Assert.That(pool.IdleCount, Is.EqualTo(1));
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });

        await using IConnectionLease second = await pool.RentAsync(None);
        Assert.That(second.Connection, Is.SameAs(connection), "and it is still the connection handed out next");
    }

    [Test]
    public async Task RentAsync_EveryIdleConnectionPastTheIdleTimeout_DiscardsThemAllAndDialsOnce()
    {
        // The checkout's discard loop, which idleness reaches for the first time. It walks the idle set closing
        // candidates until one passes or the set is empty, so a loop that stopped at the first rejection would
        // leave the rest open and unreachable — and one that reported the rejection would fail the caller.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 3, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        await ReturnConcurrentLeasesAsync(pool, 3);
        ClickHouseTcpConnection[] original = [.. factory.Created];
        clock.Advance(TimeSpan.FromMinutes(6));

        await using IConnectionLease lease = await pool.RentAsync(None);

        Assert.Multiple(() =>
        {
            Assert.That(original.Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Terminated));
            Assert.That(lease.Connection, Is.Not.AnyOf(original.Cast<object>().ToArray()));
            Assert.That(factory.CreateCount, Is.EqualTo(4), "three discarded, one dialled — not one dial per discard");
            Assert.That(pool.IdleCount, Is.Zero);
        });
    }

    [Test]
    public async Task Sweep_RunTwiceOverAQuietPoolAtItsFloor_RotatesTheFloorRatherThanHoldingIt()
    {
        // The steady state the design accepts: since neither limit yields to MinPoolSize, a pool nobody is using
        // re-dials its floor once per IdleTimeout instead of holding the same sockets. Pinned because it is the
        // cost of the choice — MinPoolSize dials per IdleTimeout — and because a regression would show up here as
        // either a floor that decays to nothing or one that never refreshes.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 2, minPoolSize: 2, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        await ReturnConcurrentLeasesAsync(pool, 2);
        ClickHouseTcpConnection[] first = [.. factory.Created];

        clock.Advance(TimeSpan.FromMinutes(6));
        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.Multiple(() =>
        {
            Assert.That(first.Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Terminated));
            Assert.That(pool.IdleCount, Is.EqualTo(2), "the floor is restored, not decayed");
            Assert.That(factory.CreateCount, Is.EqualTo(4), "and restored at its cap, not past it");
        });

        // The replacements are themselves idle, so the next round reaps and replaces them in turn.
        ClickHouseTcpConnection[] second = [.. factory.Created.Skip(2)];
        clock.Advance(TimeSpan.FromMinutes(6));
        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.Multiple(() =>
        {
            Assert.That(second.Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Terminated));
            Assert.That(pool.IdleCount, Is.EqualTo(2));
            Assert.That(factory.CreateCount, Is.EqualTo(6), "one round of dials per idle timeout, no more");
        });
    }

    [Test]
    public async Task Return_ConnectionLeftBytesOnTheWire_IsClosedRatherThanPooled()
    {
        // The transport half of the test the return path now shares with a checkout. A connection whose last
        // operation did not consume everything the server sent is out of step: the next reply would decode the
        // leftovers. The old return path only asked for Ready, so it pooled such a connection and left the next
        // checkout to find it — closing now frees the socket sooner and keeps one predicate at both ends.
        var factory = new FakeConnectionFactory { Trailing = [0x09] };
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

        IConnectionLease lease = await pool.RentAsync(None);
        Assert.That(lease.Connection.State, Is.EqualTo(TcpConnectionState.Ready), "Ready, but out of step");
        await lease.DisposeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pool.IdleCount, Is.Zero);
            Assert.That(lease.Connection.State, Is.EqualTo(TcpConnectionState.Terminated));
        });
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
            Options(maxPoolSize: 2, maxConnectionLifetime: TimeSpan.FromMinutes(10), idleTimeout: TimeSpan.Zero),
            factory,
            clock);

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
    public async Task PooledConnection_IdleClock_DoesNotRunWhileTheConnectionIsCheckedOut()
    {
        var clock = new ControlledTimeProvider();
        using ClickHouseTcpConnection connection = await FakeConnectionFactory.CreateReadyAsync();
        var pooled = new PooledConnection(connection, clock);

        pooled.OnRented();
        clock.Advance(TimeSpan.FromMinutes(30));

        Assert.Multiple(() =>
        {
            // A connection carrying an operation is not idle, however long the operation takes. Both the sweep and
            // the return path read this, so a clock that kept running would retire a connection mid-use.
            Assert.That(pooled.IdleFor, Is.EqualTo(TimeSpan.Zero));
            Assert.That(pooled.IsPastIdleTimeout(TimeSpan.FromMinutes(5)), Is.False);
            Assert.That(pooled.Age, Is.EqualTo(TimeSpan.FromMinutes(30)), "age runs whatever the connection is doing");
        });

        pooled.OnReturned();
        clock.Advance(TimeSpan.FromMinutes(6));

        Assert.Multiple(() =>
        {
            Assert.That(pooled.IdleFor, Is.EqualTo(TimeSpan.FromMinutes(6)), "and it restarts once the connection is back");
            Assert.That(pooled.IsPastIdleTimeout(TimeSpan.FromMinutes(5)), Is.True);
            Assert.That(pooled.IsPastIdleTimeout(TimeSpan.Zero), Is.False, "zero disables the limit");
        });
    }

    [Test]
    public async Task Sweep_IdleConnectionsPastTheIdleTimeout_AreRetiredDespiteTheFloorAndReplaced()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 1, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        await ReturnConcurrentLeasesAsync(pool, 3);
        ClickHouseTcpConnection[] original = [.. factory.Created];
        clock.Advance(TimeSpan.FromMinutes(6));

        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.Multiple(() =>
        {
            // Idleness is a liveness limit as much as a resource one, so the floor must not hold an over-idle
            // connection open: a checkout would refuse it, and the socket may already be dead.
            Assert.That(original.Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Terminated));

            // But the floor is still a floor, so the sweep opens a fresh connection to keep it.
            Assert.That(pool.IdleCount, Is.EqualTo(1));
            Assert.That(factory.CreateCount, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task Sweep_IdleConnectionsPastTheirLifetime_AreRetiredDespiteTheFloorAndReplaced()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(
                maxPoolSize: 4,
                minPoolSize: 3,
                maxConnectionLifetime: TimeSpan.FromMinutes(10),
                idleTimeout: TimeSpan.Zero),
            factory,
            clock);

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
    public async Task Sweep_IdleConnectionsPastTheIdleTimeoutWithConnectionsInUse_ReplacesOnlyWhatTheFloorNeeds()
    {
        // Two in use plus two over-idle, against a floor of three. Both idle ones go, and the top-up then opens
        // one — not three — because the two in use count toward the floor just as much as an idle connection does.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 4, minPoolSize: 3, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        IConnectionLease[] leases = [await pool.RentAsync(None), await pool.RentAsync(None)];
        await ReturnConcurrentLeasesAsync(pool, 2);
        ClickHouseTcpConnection[] wereIdle = [.. factory.Created.Skip(2)];
        clock.Advance(TimeSpan.FromMinutes(6));

        try
        {
            pool.Sweep();
            await (pool.LastRefill ?? Task.CompletedTask);

            Assert.Multiple(() =>
            {
                Assert.That(wereIdle.Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Terminated));
                Assert.That(
                    leases.Select(l => l.Connection.State),
                    Is.All.EqualTo(TcpConnectionState.Ready),
                    "the sweep walks the idle set only, so a lease that is out is not its to touch");
                Assert.That(pool.IdleCount, Is.EqualTo(1));
                Assert.That(factory.CreateCount, Is.EqualTo(5), "the two in use cover two thirds of the floor");
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
    public async Task Sweep_SomeIdleConnectionsPastTheIdleTimeout_RetiresOnlyThose()
    {
        // The mixed case, which also covers the walk itself: the sweep removes entries while iterating, so one
        // that skipped a neighbour would leave an expired connection in the set or reap a live one.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 3, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        // Returned a minute apart, then aged so the first two are past the idle timeout and the third is not.
        IConnectionLease[] leases = [await pool.RentAsync(None), await pool.RentAsync(None), await pool.RentAsync(None)];
        foreach (IConnectionLease lease in leases)
        {
            await lease.DisposeAsync();
            clock.Advance(TimeSpan.FromMinutes(1));
        }

        clock.Advance(TimeSpan.FromMinutes(3));
        pool.Sweep();

        Assert.Multiple(() =>
        {
            Assert.That(leases[0].Connection.State, Is.EqualTo(TcpConnectionState.Terminated), "idle for 6 minutes");
            Assert.That(leases[1].Connection.State, Is.EqualTo(TcpConnectionState.Terminated), "idle for 5 minutes, the boundary");
            Assert.That(leases[2].Connection.State, Is.EqualTo(TcpConnectionState.Ready), "idle for 4 minutes");
            Assert.That(pool.IdleCount, Is.EqualTo(1));
        });

        await using IConnectionLease survivor = await pool.RentAsync(None);
        Assert.That(survivor.Connection, Is.SameAs(leases[2].Connection));
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
    public async Task Sweep_ReEnteredWhileItClosesAConnection_DoesNothingOnTheNestedCall()
    {
        // Two overlapping sweeps, which is what the one-at-a-time guard is for. Sweeps share one `reaped` buffer,
        // so a second sweep clearing it would break the first sweep's own iteration and strand the rest of the
        // batch. Real overlap needs a timer callback that outlives its period, which is possible because closing
        // sockets happens outside the lock; re-entering from inside a close is the same window, made deterministic.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 2, idleTimeout: TimeSpan.FromMinutes(5)), factory, clock);

        int closes = 0;
        bool reentered = false;
        factory.OnClose = () =>
        {
            closes++;
            if (!reentered)
            {
                reentered = true;
                pool.Sweep();
            }
        };

        await ReturnConcurrentLeasesAsync(pool, 2);
        clock.Advance(TimeSpan.FromMinutes(6));

        Assert.DoesNotThrow(pool.Sweep);

        Assert.Multiple(() =>
        {
            Assert.That(reentered, Is.True, "the nested sweep must really have been attempted");
            Assert.That(closes, Is.EqualTo(2), "each connection is closed once, by the outer sweep alone");
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
    public void SweepInterval_SetExplicitly_ReplacesTheDerivedPeriod()
    {
        // The derived period for these limits is 30 seconds, so the assertion fails if the override is ignored.
        var options = new ClickHouseTcpClientOptions
        {
            IdleTimeout = TimeSpan.FromMinutes(5),
            MaxConnectionLifetime = TimeSpan.FromMinutes(30),
            SweepInterval = TimeSpan.FromSeconds(3),
        };

        Assert.That(ConnectionPool.SweepInterval(options), Is.EqualTo(TimeSpan.FromSeconds(3)));
    }

    [TestCase(100, TestName = "SweepInterval_SetBelowTheDerivedFloor_IsNotRaisedToIt")]
    [TestCase(600_000, TestName = "SweepInterval_SetAboveTheDerivedCeiling_IsNotLoweredToIt")]
    public void SweepInterval_SetOutsideTheDerivedBounds_IsUsedAsGiven(int milliseconds)
    {
        // The bounds shape the derivation only. Clamping an explicit value would ignore what the caller asked for
        // and leave no way to find out, which is worse than honouring an awkward period.
        var options = new ClickHouseTcpClientOptions
        {
            IdleTimeout = TimeSpan.FromSeconds(4),
            MaxConnectionLifetime = TimeSpan.Zero,
            SweepInterval = TimeSpan.FromMilliseconds(milliseconds),
        };

        Assert.That(ConnectionPool.SweepInterval(options), Is.EqualTo(TimeSpan.FromMilliseconds(milliseconds)));
    }

    [Test]
    public void SweepInterval_SetWithNoLimitsButAFloorToHold_ReplacesTheFloorHoldingPeriod()
    {
        var options = new ClickHouseTcpClientOptions
        {
            IdleTimeout = TimeSpan.Zero,
            MaxConnectionLifetime = TimeSpan.Zero,
            MinPoolSize = 2,
            SweepInterval = TimeSpan.FromSeconds(7),
        };

        Assert.That(ConnectionPool.SweepInterval(options), Is.EqualTo(TimeSpan.FromSeconds(7)));
    }

    [Test]
    public void SweepInterval_SetButWithNothingToSweep_IsStillZeroSoNoTimerIsCreated()
    {
        // The override sets the period, not whether there is work. Nothing expires and there is no floor, so a
        // timer would wake only to find nothing — and while one runs it holds a reference to the pool, so an
        // undisposed client could never be collected.
        var options = new ClickHouseTcpClientOptions
        {
            IdleTimeout = TimeSpan.Zero,
            MaxConnectionLifetime = TimeSpan.Zero,
            MinPoolSize = 0,
            SweepInterval = TimeSpan.FromSeconds(5),
        };

        Assert.That(ConnectionPool.SweepInterval(options), Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public async Task Sweep_OnAPoolBuiltWithAnExplicitInterval_StillRetiresExpiredConnections()
    {
        // The override changes when the sweep runs, not what it does. The clock here is controlled and its timers
        // are inert, so this drives the sweep directly rather than waiting for the period.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        ClickHouseTcpClientOptions options = Options(maxPoolSize: 2, idleTimeout: TimeSpan.FromMinutes(5)) with
        {
            SweepInterval = TimeSpan.FromSeconds(2),
        };
        await using var pool = new ConnectionPool(options, factory, clock);

        await ReturnConcurrentLeasesAsync(pool, 2);
        clock.Advance(TimeSpan.FromMinutes(6));
        pool.Sweep();

        Assert.Multiple(() =>
        {
            Assert.That(pool.IdleCount, Is.Zero);
            Assert.That(factory.Created.Select(c => c.State), Is.All.EqualTo(TcpConnectionState.Terminated));
        });
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
    public async Task DisposeAsync_DisposesTheFactoryAfterEveryConnectionIsClosed()
    {
        // The factory owns what outlives a single connection — the TLS certificate authorities — so the pool has to
        // release it, and only once nothing can still be handshaking against it.
        var factory = new FakeConnectionFactory();
        bool everyConnectionWasClosed = false;
        factory.OnDispose = () => everyConnectionWasClosed = factory.Created
            .All(c => c.State == TcpConnectionState.Terminated);
        var pool = new ConnectionPool(Options(maxPoolSize: 3), factory, new ControlledTimeProvider());
        await ReturnConcurrentLeasesAsync(pool, 3);

        await pool.DisposeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(factory.Disposed, Is.True);
            Assert.That(factory.DisposeCount, Is.EqualTo(1));
            Assert.That(
                everyConnectionWasClosed,
                Is.True,
                "the connections are closed before the factory goes, not after");
        });
    }

    [Test]
    public async Task DisposeAsync_DeferredFactoryDisposalThrows_DoesNotReplaceTheDialFailure()
    {
        var dialFailure = new InvalidOperationException("dial failed");
        FakeConnectionFactory factory = GatedDialFactory(out SemaphoreSlim dialing, out TaskCompletionSource finishDial);
        factory.FailNextWith = dialFailure;
        factory.OnDispose = () => throw new InvalidOperationException("factory disposal failed");
        var pool = new ConnectionPool(
            Options(maxPoolSize: 1, poolTimeout: TimeSpan.FromMilliseconds(150)),
            factory,
            new ControlledTimeProvider());

        Task<IConnectionLease> stuck = Task.Run(async () => await pool.RentAsync(None));
        Assert.That(await dialing.WaitAsync(TimeSpan.FromSeconds(30)), Is.True, "the dial should be in the factory");
        await pool.DisposeAsync();

        finishDial.SetResult();
        InvalidOperationException thrown = Assert.ThrowsAsync<InvalidOperationException>(async () => await stuck);

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(dialFailure), "factory disposal must not replace the dial's own result");
            Assert.That(factory.DisposeCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task DisposeAsync_ADialStillRunningPastTheDrainDeadline_DisposesTheFactoryWhenTheDialFinishes()
    {
        // The dial is the one caller disposal cannot reach: it holds a permit but is not in `leased`, so the abort
        // after the drain deadline does not see it. Disposing the factory anyway would free the TLS certificate
        // authorities under a live handshake. It still has to be disposed once that handshake leaves the factory.
        FakeConnectionFactory factory = GatedDialFactory(out SemaphoreSlim dialing, out TaskCompletionSource finishDial);
        var pool = new ConnectionPool(
            Options(maxPoolSize: 1, poolTimeout: TimeSpan.FromMilliseconds(150)),
            factory,
            new ControlledTimeProvider());

        Task<IConnectionLease> stuck = Task.Run(async () => await pool.RentAsync(None));
        Assert.That(await dialing.WaitAsync(TimeSpan.FromSeconds(30)), Is.True, "the dial should be in the factory");

        await pool.DisposeAsync();

        Assert.That(
            factory.Disposed,
            Is.False,
            "the certificates must survive while the dial is still using them");

        finishDial.SetResult();
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await stuck);

        Assert.Multiple(() =>
        {
            Assert.That(factory.Disposed, Is.True, "the last dial out performs the deferred disposal");
            Assert.That(factory.DisposeCount, Is.EqualTo(1));
            Assert.That(factory.Created[0].State, Is.EqualTo(TcpConnectionState.Terminated));
        });
    }

    [Test]
    public async Task DisposeAsync_TwoDialsOutliveTheDrain_DisposesTheFactoryOnlyAfterBothFinish()
    {
        var dialing = new SemaphoreSlim(0);
        var finishFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finishSecond = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource[] finishes = [finishFirst, finishSecond];
        int dialIndex = -1;
        var factory = new FakeConnectionFactory
        {
            IgnoresCancellation = true,
            BeforeCreate = async _ =>
            {
                int index = Interlocked.Increment(ref dialIndex);
                dialing.Release();
                await finishes[index].Task;
            },
        };
        var pool = new ConnectionPool(
            Options(maxPoolSize: 2, poolTimeout: TimeSpan.FromMilliseconds(150)),
            factory,
            new ControlledTimeProvider());

        Task<IConnectionLease> first = Task.Run(async () => await pool.RentAsync(None));
        Assert.That(await dialing.WaitAsync(TimeSpan.FromSeconds(30)), Is.True, "the first dial should be active");
        Task<IConnectionLease> second = Task.Run(async () => await pool.RentAsync(None));
        Assert.That(await dialing.WaitAsync(TimeSpan.FromSeconds(30)), Is.True, "the second dial should be active");

        await pool.DisposeAsync();
        Assert.That(factory.Disposed, Is.False, "both dials still retain the factory");

        finishFirst.SetResult();
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await first);
        Assert.Multiple(() =>
        {
            Assert.That(factory.Disposed, Is.False, "the second dial still retains the factory");
            Assert.That(factory.DisposeCount, Is.Zero);
        });

        finishSecond.SetResult();
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await second);
        Assert.Multiple(() =>
        {
            Assert.That(factory.Disposed, Is.True, "the last dial out performs the deferred disposal");
            Assert.That(factory.DisposeCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task DisposeAsync_CalledTwice_IsNoOp()
    {
        var factory = new FakeConnectionFactory();
        var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());
        await pool.DisposeAsync();

        Assert.DoesNotThrowAsync(async () => await pool.DisposeAsync());
        Assert.That(factory.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task DisposeAsync_CalledConcurrently_BothCallersWaitForTheOneTeardown()
    {
        // Idempotent is not the same as instant. A second caller that returned as soon as it saw the flag would
        // report a pool whose connections are still open — and may yet be aborted by the first caller — as
        // closed, which is exactly what awaiting a disposal is supposed to rule out.
        var factory = new FakeConnectionFactory();
        var pool = new ConnectionPool(Options(maxPoolSize: 1), factory, new ControlledTimeProvider());

        // Held, so the first caller's drain cannot finish until it is given back.
        IConnectionLease held = await pool.RentAsync(None);

        Task first = pool.DisposeAsync().AsTask();
        Task second = pool.DisposeAsync().AsTask();

        Assert.Multiple(() =>
        {
            Assert.That(first.IsCompleted, Is.False, "the drain is waiting on the lease still out");
            Assert.That(second.IsCompleted, Is.False, "and the second caller has to wait for the same teardown");
        });

        await held.DisposeAsync();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.That(held.Connection.State, Is.EqualTo(TcpConnectionState.Terminated), "both callers return only once it is really closed");
    }

    [Test]
    public async Task RentAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var pool = new ConnectionPool(Options(), new FakeConnectionFactory(), new ControlledTimeProvider());
        await pool.DisposeAsync();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await pool.RentAsync(None));
    }

    [Test]
    public async Task RentAsync_DisposedAfterTheLastPreDialCheck_DoesNotEnterTheFactory()
    {
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(
                maxPoolSize: 1,
                maxConnectionLifetime: TimeSpan.FromMinutes(1),
                idleTimeout: TimeSpan.Zero),
            factory,
            clock);

        await using (await pool.RentAsync(None))
        {
        }

        clock.Advance(TimeSpan.FromMinutes(2));
        int createAttempts = 0;
        factory.BeforeCreate = _ =>
        {
            Interlocked.Increment(ref createAttempts);
            return Task.CompletedTask;
        };

        Task disposing = null;
        clock.OnNextTimestamp = () => disposing = pool.DisposeAsync().AsTask();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await pool.RentAsync(None));
        Assert.That(clock.OnNextTimestamp, Is.Null, "the checkout must have reached the post-check clock seam");
        Assert.That(disposing, Is.Not.Null, "the clock seam must have started disposal");
        await disposing;

        Assert.Multiple(() =>
        {
            Assert.That(createAttempts, Is.Zero, "the raced checkout must not enter the factory");
            Assert.That(factory.CreateCount, Is.EqualTo(1), "only the connection opened before disposal exists");
            Assert.That(factory.DisposeCount, Is.EqualTo(1));
        });
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
    public async Task DisposeAsync_WithALeaseHeldPastPoolTimeout_DisposesTheFactoryBeforeTheLeaseReturns()
    {
        var factory = new FakeConnectionFactory();
        var pool = new ConnectionPool(
            Options(maxPoolSize: 1, poolTimeout: TimeSpan.FromMilliseconds(150)), factory, new ControlledTimeProvider());

        IConnectionLease held = await pool.RentAsync(None);

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
            Assert.That(factory.Disposed, Is.True, "an established connection no longer uses the factory");
            Assert.That(factory.DisposeCount, Is.EqualTo(1));
        });

        Assert.DoesNotThrowAsync(async () => await held.DisposeAsync());
        Assert.That(
            factory.DisposeCount,
            Is.EqualTo(1),
            "returning the timed-out lease cannot dispose the factory twice");
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
    public async Task Sweep_ALeaseIsOutAndNothingIsIdle_RunsRatherThanThrowing()
    {
        // The commonest state a sweep can land in, and the one that used to break it. The idle trim was a second
        // pass whose floor test counted the connections that are out, so with the default floor of zero it passed
        // with an empty idle set — and the trim then read idle[0]. On the timer SweepQuietly swallows that, so the
        // sweep silently stopped trimming and stopped holding the floor for the rest of the pool's life. The trim
        // is now one indexed walk that cannot reach an entry that is not there, but the state is worth pinning.
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: 2), factory, new ControlledTimeProvider());

        await using IConnectionLease held = await pool.RentAsync(None);
        Assert.That(pool.IdleCount, Is.Zero, "the one connection open is the one checked out");

        Assert.DoesNotThrow(pool.Sweep);
    }

    [Test]
    public async Task Sweep_ReapingEmptiesTheIdleSetWhileALeaseIsOut_StillClosesWhatItReaped()
    {
        // The damage the throw did, rather than the throw itself: a connection reaped for age leaves the idle set
        // under the lock and is closed only after it, so anything throwing in between leaves it open with nothing
        // able to reach it. Still worth pinning now the two passes are one — the gap between the two is not.
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 2, maxConnectionLifetime: TimeSpan.FromMinutes(30), idleTimeout: TimeSpan.Zero),
            factory,
            clock);

        IConnectionLease held = await pool.RentAsync(None);
        IConnectionLease returned = await pool.RentAsync(None);
        ClickHouseTcpConnection reaped = returned.Connection;
        await returned.DisposeAsync();

        // Past the lifetime, so the reaping pass takes the only idle connection and leaves the set empty.
        clock.Advance(TimeSpan.FromMinutes(31));

        try
        {
            Assert.DoesNotThrow(pool.Sweep);

            Assert.Multiple(() =>
            {
                Assert.That(pool.IdleCount, Is.Zero);
                Assert.That(reaped.State, Is.EqualTo(TcpConnectionState.Terminated), "a reaped connection must still be closed, not dropped");
                Assert.That(held.Connection.State, Is.EqualTo(TcpConnectionState.Ready), "the lease still out is not the sweep's to touch");
            });
        }
        finally
        {
            await held.DisposeAsync();
        }
    }

    [Test]
    public async Task Sweep_CheckoutHoldingAConnectionItHasNotFiledYet_StillCountsItAgainstTheFloor()
    {
        // A checkout takes its connection out of the idle set before it records it as leased, so for that moment
        // the connection is in neither set. A top-up counting only the two sets reads the gap as spare capacity
        // and dials into it, past MaxPoolSize. The clock is the seam that reaches the moment: the checkout reads
        // it to test the connection's age, which happens inside exactly that window.
        const int Size = 2;
        var clock = new ControlledTimeProvider();
        var factory = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxPoolSize: Size, minPoolSize: Size), factory, clock);

        // Both connections idle: the pool sits exactly at its floor, which is also its cap.
        await ReturnConcurrentLeasesAsync(pool, Size);
        Assert.That(factory.CreateCount, Is.EqualTo(Size));

        Task refill = null;
        clock.OnNextTimestamp = () =>
        {
            pool.Sweep();
            refill = pool.LastRefill;
        };

        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            Assert.That(clock.OnNextTimestamp, Is.Null, "the checkout must have read the clock, or the sweep never ran");
        }

        await (refill ?? Task.CompletedTask);

        Assert.Multiple(() =>
        {
            Assert.That(refill, Is.Null, "the floor was already met by the connection the checkout was holding");
            Assert.That(factory.CreateCount, Is.EqualTo(Size), "more connections were opened than the cap allows");
        });
    }

    [Test]
    public async Task DisposeAsync_WhileACheckoutIsStillDialing_EndsThatDialRatherThanWaitingItOut()
    {
        // A dial in flight is the one connection disposal cannot reach any other way: its caller is not in the
        // leased set, so the abort at the end of the drain does not see it. Unlinked, it would hold its permit —
        // and, once connected, its socket — for as long as DialTimeout allows, well after the pool closed.
        // Disposed by the using as well as explicitly below, so an assertion that fails first cannot leave the
        // pool open with a dial parked in it. Disposal is idempotent, so the second call is a no-op.
        await using var pool = new ConnectionPool(
            Options(), DialsForeverFactory(out SemaphoreSlim dialing), new ControlledTimeProvider());
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
        // have. The permit and the reserved slot both have to come back either way.
        var factory = DialsForeverFactory(out SemaphoreSlim dialing);
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 1, minPoolSize: 1), factory, new ControlledTimeProvider());

        using var cts = new CancellationTokenSource();
        Task<IConnectionLease> renting = Task.Run(async () => await pool.RentAsync(cts.Token));
        Assert.That(await dialing.WaitAsync(TimeSpan.FromSeconds(30)), Is.True, "the dial should be in flight");

        cts.Cancel();
        Assert.CatchAsync<OperationCanceledException>(async () => await renting);

        // The floor is what observes the slot: a checkout that failed to give one back would leave the pool
        // permanently believing it is already at MinPoolSize, and no later top-up would ever run. The permit is
        // observed with it, since the top-up cannot dial without one.
        factory.BeforeCreate = null;
        pool.Sweep();
        await (pool.LastRefill ?? Task.CompletedTask);

        Assert.Multiple(() =>
        {
            Assert.That(pool.LastRefill, Is.Not.Null, "the cancelled checkout must give back the slot it reserved");
            Assert.That(pool.IdleCount, Is.EqualTo(1), "and the top-up must then be able to fill the floor");
        });
    }

    [Test]
    public async Task RentAsync_PoolDisposedWhileTheDialFinishes_ClosesTheConnectionRatherThanHandingItOut()
    {
        // The other side of the checkout race: a dial that cannot be called off and lands just after the drain
        // emptied the leased set. Nothing else can reach that connection — it was never in either set — so the
        // checkout has to close it itself rather than hand out a connection from a pool that is already shut.
        FakeConnectionFactory factory = GatedDialFactory(out SemaphoreSlim dialing, out TaskCompletionSource finishDial);
        await using var pool = new ConnectionPool(Options(), factory, new ControlledTimeProvider());

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
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 2, minPoolSize: 1), factory, new ControlledTimeProvider());

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
