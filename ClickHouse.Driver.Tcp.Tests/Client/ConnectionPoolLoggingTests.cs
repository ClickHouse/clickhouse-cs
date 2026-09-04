using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Logging;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// What the pool logs, over connections that need no server. Two of these events are reachable no other way: a
// background top-up and a sweep both swallow their exceptions by design, so without a logger a failure in either
// leaves no trace at all and no test could observe it.
[TestFixture]
public class ConnectionPoolLoggingTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private CapturingLoggerFactory factory;

    [SetUp]
    public void CreateFactory() => factory = new CapturingLoggerFactory();

    [TearDown]
    public void DisposeFactory() => factory.Dispose();

    private CapturingLogger Log => factory.Logger(ClickHouseTcpDiagnostics.PoolLogCategory);

    private ClickHouseTcpClientOptions Options(
        int maxPoolSize = 4,
        int minPoolSize = 0,
        TimeSpan? poolTimeout = null,
        TimeSpan? maxConnectionLifetime = null,
        TimeSpan? idleTimeout = null)
        => new()
        {
            MaxPoolSize = maxPoolSize,
            MinPoolSize = minPoolSize,
            PoolTimeout = poolTimeout ?? TimeSpan.FromSeconds(30),
            MaxConnectionLifetime = maxConnectionLifetime ?? TimeSpan.FromMinutes(30),
            IdleTimeout = idleTimeout ?? TimeSpan.FromMinutes(5),
            LoggerFactory = factory,
        };

    [Test]
    public async Task RentAsync_NoLoggerFactory_AsksForNoLogger()
    {
        var connections = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options() with { LoggerFactory = null }, connections, new ControlledTimeProvider());

        await using IConnectionLease lease = await pool.RentAsync(None);

        Assert.That(factory.Categories, Is.Empty, "no factory configured means nothing is created per pool");
    }

    [Test]
    public async Task RentAsync_FirstRent_LogsThatItIsOpeningOne()
    {
        var connections = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), connections, new ControlledTimeProvider());

        await using IConnectionLease lease = await pool.RentAsync(None);

        Assert.That(Log.WithEventId(3001), Is.Not.Empty, "no idle connection to reuse");
    }

    [Test]
    public async Task RentAsync_AfterALeaseIsReturned_LogsTheReuse()
    {
        var connections = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), connections, new ControlledTimeProvider());

        await using (IConnectionLease first = await pool.RentAsync(None))
        {
        }

        await using IConnectionLease second = await pool.RentAsync(None);

        LogEntry reused = Log.WithEventId(3000).Single();
        Assert.Multiple(() =>
        {
            Assert.That(reused.Level, Is.EqualTo(LogLevel.Trace), "a per-operation line belongs at Trace");

            // The count is this checkout's, so the first reuse is the connection's second operation. Reading it
            // before the checkout records itself would report the previous number.
            Assert.That(reused.Message, Does.Contain("its 2 operation"));
        });
    }

    [Test]
    public async Task Return_ConnectionNoLongerUsable_LogsTheDiscard()
    {
        var connections = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), connections, new ControlledTimeProvider());

        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
            // A terminated connection is what a failed or abandoned operation leaves behind, and the pool closes
            // it on return rather than handing it to the next caller.
            lease.Connection.Terminate();
        }

        Assert.That(Log.WithEventId(3002), Is.Not.Empty, "the discard is reported");
    }

    [Test]
    public async Task Sweep_IdleConnectionPastItsLifetime_LogsWhatItRetired()
    {
        var time = new ControlledTimeProvider();
        var connections = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(maxConnectionLifetime: TimeSpan.FromMinutes(1)), connections, time);

        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
        }

        time.Advance(TimeSpan.FromMinutes(2));
        pool.Sweep();

        LogEntry retired = Log.WithEventId(3003).Single();
        Assert.That(retired.Message, Does.Contain("Retired 1"));
    }

    [Test]
    public async Task Sweep_NothingExpired_LogsNothing()
    {
        var connections = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(Options(), connections, new ControlledTimeProvider());

        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
        }

        pool.Sweep();

        Assert.That(Log.WithEventId(3003), Is.Empty, "a sweep that retires nothing is not worth a line");
    }

    [Test]
    public async Task RentAsync_PoolExhausted_LogsAWarningBeforeThrowing()
    {
        var connections = new FakeConnectionFactory();
        await using var pool = new ConnectionPool(
            Options(maxPoolSize: 1, poolTimeout: TimeSpan.FromMilliseconds(50)),
            connections,
            new ControlledTimeProvider());

        await using IConnectionLease held = await pool.RentAsync(None);

        Assert.ThrowsAsync<TimeoutException>(async () => await pool.RentAsync(None));

        LogEntry exhausted = Log.WithEventId(3004).Single();
        Assert.Multiple(() =>
        {
            Assert.That(exhausted.Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(exhausted.Message, Does.Contain("PoolTimeout"));
        });
    }

    [Test]
    public async Task Sweep_BackgroundTopUpDialFails_LogsTheFailureNobodyElseSees()
    {
        var time = new ControlledTimeProvider();
        var connections = new FakeConnectionFactory { FailNextWith = new InvalidOperationException("dial refused") };
        await using var pool = new ConnectionPool(Options(minPoolSize: 1), connections, time);

        pool.Sweep();
        if (pool.LastRefill is not null)
        {
            await pool.LastRefill;
        }

        LogEntry failed = Log.WithEventId(3005).Single();
        Assert.Multiple(() =>
        {
            Assert.That(failed.Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(failed.Exception, Is.TypeOf<InvalidOperationException>(), "the swallowed exception reaches the log");
        });
    }

    [Test]
    public async Task DisposeAsync_OpenPool_LogsTheDrain()
    {
        var connections = new FakeConnectionFactory();
        var pool = new ConnectionPool(Options(), connections, new ControlledTimeProvider());

        await using (IConnectionLease lease = await pool.RentAsync(None))
        {
        }

        await pool.DisposeAsync();

        LogEntry draining = Log.WithEventId(3007).Single();
        Assert.That(draining.Message, Does.Contain("closing 1 idle"));
    }

    [Test]
    public async Task RentAsync_ThrowingLogger_StillHandsOverTheConnection()
    {
        // The pool logs at the points a connection is between owners: taken out of the idle list but not yet
        // leased, or out of the leased set but not yet closed. An exception from any of those calls would leave a
        // socket with nobody left to close it, so a broken logger would read as the client leaking connections.
        var connections = new FakeConnectionFactory();
        var pool = new ConnectionPool(Options() with { LoggerFactory = new ThrowingLoggerFactory() }, connections, new ControlledTimeProvider());

        await using (IConnectionLease first = await pool.RentAsync(None))
        {
        }

        await using (IConnectionLease reused = await pool.RentAsync(None))
        {
            Assert.That(connections.CreateCount, Is.EqualTo(1), "the reuse path survived its log call");
        }

        await pool.DisposeAsync();

        Assert.That(connections.Disposed, Is.True, "and teardown ran to the end");
    }
}
