using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// What only a real server shows: that concurrent operations over one client really do run at once on separate
// connections, and that their results come back uncorrupted rather than crossed. The pool's own decisions —
// reuse, retirement, queueing, drain — are covered without a server in ConnectionPoolTests.
[TestFixture]
[Category("Integration")]
public class ConnectionPoolIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static string UniqueTableName() => $"tcp_pool_test_{Guid.NewGuid():N}";

    private sealed class ValueRow
    {
        public ulong Id { get; set; }
    }

    private static ClickHouseTcpClient CreateClient(
        int maxPoolSize = 4,
        TimeSpan? poolTimeout = null,
        TimeSpan? idleTimeout = null)
        => new(TcpServerFixture.Options() with
        {
            MaxPoolSize = maxPoolSize,
            PoolTimeout = poolTimeout ?? TimeSpan.FromSeconds(30),
            IdleTimeout = idleTimeout ?? TimeSpan.FromMinutes(5),
        });

    [Test]
    public async Task QueryAsync_FourQueriesAtOnce_RunConcurrentlyRatherThanOneAfterAnother()
    {
        // Four one-second sleeps. Serialized they take at least four seconds; on four connections they take about
        // one, so the bound is wide enough to be stable and still far below the serialized cost.
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 4);

        var elapsed = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, 4).Select(async _ =>
        {
            await foreach (object[] row in client.QueryAsync("SELECT sleep(1)", cancellationToken: None))
            {
                Assert.That(row, Has.Length.EqualTo(1));
            }
        }));
        elapsed.Stop();

        Assert.That(elapsed.Elapsed, Is.LessThan(TimeSpan.FromSeconds(3.5)));
    }

    [Test]
    public async Task QueryAsync_MoreCallersThanMaxPoolSize_AllQueueAndSucceed()
    {
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 2);

        ulong[] results = await Task.WhenAll(Enumerable.Range(0, 12).Select(async i =>
        {
            ulong total = 0;
            await foreach (ValueRow row in client.QueryAsync<ValueRow>(
                $"SELECT toUInt64({i}) AS id", cancellationToken: None))
            {
                total += row.Id;
            }

            return total;
        }));

        Assert.That(results.OrderBy(v => v), Is.EqualTo(Enumerable.Range(0, 12).Select(i => (ulong)i)));
    }

    [Test]
    public async Task QueryAsync_ManyConcurrentQueries_EachSeesOnlyItsOwnResult()
    {
        // The failure this guards against is two operations sharing a connection and reading each other's blocks,
        // which shows up as a row count or a value belonging to another query.
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 4);

        int[] rowCounts = await Task.WhenAll(Enumerable.Range(1, 16).Select(async i =>
        {
            int rows = 0;
            await foreach (Block block in client.StreamAsync(
                $"SELECT number FROM system.numbers LIMIT {i * 100}", cancellationToken: None))
            {
                rows += block.RowCount;
            }

            return rows;
        }));

        Assert.That(rowCounts, Is.EqualTo(Enumerable.Range(1, 16).Select(i => i * 100)));
    }

    [Test]
    public async Task InsertRowsAsync_ConcurrentInsertsIntoOneTable_AllRowsLand()
    {
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 4);
        string table = UniqueTableName();
        await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = MergeTree ORDER BY id", cancellationToken: None);

        try
        {
            await Task.WhenAll(Enumerable.Range(0, 8).Select(batch => client.InsertRowsAsync(
                $"INSERT INTO {table} (id) VALUES",
                Enumerable.Range(batch * 50, 50).Select(i => new ValueRow { Id = (ulong)i }).ToList(),
                cancellationToken: None).AsTask()));

            ulong count = 0;
            await foreach (ValueRow row in client.QueryAsync<ValueRow>(
                $"SELECT count() AS id FROM {table}", cancellationToken: None))
            {
                count = row.Id;
            }

            Assert.That(count, Is.EqualTo(400UL));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task QueryAsync_ConnectionHeldByAnUnfinishedStream_LaterCallerTimesOut()
    {
        // The pool's exhaustion path, and the case its message calls out: an enumerator that is never advanced to
        // the end keeps its connection, so with a pool of one nothing else can run.
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 1, poolTimeout: TimeSpan.FromSeconds(1));

        IAsyncEnumerator<Block> held = client
            .StreamAsync("SELECT number FROM system.numbers LIMIT 500000", cancellationToken: None)
            .GetAsyncEnumerator(None);
        try
        {
            Assert.That(await held.MoveNextAsync(), Is.True);

            var thrown = Assert.ThrowsAsync<TimeoutException>(async () =>
                await client.ExecuteAsync("SELECT 1", cancellationToken: None));
            Assert.That(thrown.Message, Does.Contain("PoolTimeout"));
        }
        finally
        {
            await held.DisposeAsync();
        }

        // Disposing the enumerator gives the connection back, so the client is usable again.
        Assert.DoesNotThrowAsync(async () => await client.ExecuteAsync("SELECT 1", cancellationToken: None));
    }

    [Test]
    public async Task QueryAsync_AfterThePoolSatIdlePastTheIdleTimeout_RunsOnAFreshConnection()
    {
        // Retirement end to end, over a real socket and a real clock: ConnectionPoolTests drives a hand-held
        // TimeProvider whose timers do nothing, so nothing there proves an over-idle connection is retired
        // without a test calling Sweep itself. This does not say which mechanism did it — the sweep timer and the
        // checkout would both refuse that connection, and either is a correct answer — only that the caller is
        // given a working connection and not the stale one.
        //
        // A temporary table is the marker, because the server scopes one to the connection that created it: while
        // the pool reuses that connection the table is visible, and it is gone the moment the pool replaces it.
        // Five seconds, not one: the read-back below has to happen inside the window to prove the marker was ever
        // there, and the net8/net9/net10 suites run at once against one server, so a tight window would fail on a
        // scheduling stall rather than on the pool. Reuse itself is proven without any timing dependency by
        // Return_AfterEachKindOfOperation_KeepsTheSameConnection, which leaves the timeout at its 5-minute default.
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 1, idleTimeout: TimeSpan.FromSeconds(5));
        string marker = UniqueTableName();

        await client.ExecuteAsync($"CREATE TEMPORARY TABLE {marker} (id UInt64)", cancellationToken: None);
        Assert.That(
            await TemporaryTableExistsAsync(client, marker),
            Is.EqualTo(1UL),
            "the marker must exist to begin with, or the assertion below proves nothing");

        await Task.Delay(TimeSpan.FromSeconds(7));

        Assert.That(
            await TemporaryTableExistsAsync(client, marker),
            Is.EqualTo(0UL),
            "a connection left idle past the timeout must not be handed out again");
    }

    /// <summary>
    /// The other way a pooled connection dies: the server hangs up on it while the client still considers it
    /// fresh. <c>idle_connection_timeout</c> makes a real server do that on request, which is the behaviour of
    /// Cloud and of any load balancer with an idle cut, and the client's own timeouts are left at their defaults
    /// so neither can be what retires the connection. The only thing between the caller and a dead socket is the
    /// probe in <c>IsReusable</c>: with it disabled, the query below fails with a transport error saying the
    /// server closed the connection before the response was complete.
    ///
    /// <para>
    /// The <c>null</c> case is the control. The same wait against a server left at its default idle timeout keeps
    /// the connection, so what retires it in the other case is the server hanging up and not the wait.
    /// </para>
    /// </summary>
    /// <param name="serverIdleTimeout">Seconds for the server's <c>idle_connection_timeout</c>, or null to leave it.</param>
    /// <param name="markerAfterTheWait">The marker count expected after the wait: 1 if the connection was kept, 0 if it was replaced.</param>
    [TestCase("1", 0UL)]
    [TestCase(null, 1UL)]
    public async Task QueryAsync_AfterTheServerHungUpOnAnIdleConnection_RunsOnAFreshConnectionRatherThanFailing(
        string serverIdleTimeout,
        ulong markerAfterTheWait)
    {
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 1);
        ClickHouseTcpQueryOptions options = serverIdleTimeout is null
            ? null
            : new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string> { ["idle_connection_timeout"] = serverIdleTimeout },
            };

        string marker = UniqueTableName();
        await client.ExecuteAsync($"CREATE TEMPORARY TABLE {marker} (id UInt64)", options, None);
        Assert.That(
            await TemporaryTableExistsAsync(client, marker, options),
            Is.EqualTo(1UL),
            "the marker must exist to begin with, or the assertion below proves nothing");

        // Four seconds against a one-second server timeout: the server notices an idle connection on its own
        // schedule, and the suites for the other frameworks are on the same server.
        await Task.Delay(TimeSpan.FromSeconds(4));

        // Has to be asserted rather than thrown out of: the failure this defends against is the query failing,
        // and the marker count then says whether the connection was replaced or kept.
        ulong markerAfterwards = 0;
        Assert.DoesNotThrowAsync(async () => markerAfterwards = await TemporaryTableExistsAsync(client, marker, options));
        Assert.That(markerAfterwards, Is.EqualTo(markerAfterTheWait));
    }

    [Test]
    public async Task Return_AfterEachKindOfOperation_KeepsTheSameConnection()
    {
        // The return path now asks IsReusable, not just for Ready, so it polls the socket and inspects the read
        // buffer of a connection that has just finished work. If any operation leaves bytes behind, that turns
        // into a fresh connection per operation — a silent throughput loss no other test would show. The same
        // temporary table proves the connection survived: with a pool of one, it is gone if the pool replaced it.
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 1);
        string marker = UniqueTableName();

        await client.ExecuteAsync($"CREATE TEMPORARY TABLE {marker} (id UInt64)", cancellationToken: None);
        await client.InsertRowsAsync(
            $"INSERT INTO {marker} (id) VALUES",
            new List<ValueRow> { new() { Id = 1 }, new() { Id = 2 } },
            cancellationToken: None);

        ulong sum = 0;
        await foreach (ValueRow row in client.QueryAsync<ValueRow>($"SELECT sum(id) AS id FROM {marker}", cancellationToken: None))
        {
            sum = row.Id;
        }

        await foreach (Block block in client.StreamAsync($"SELECT id FROM {marker}", cancellationToken: None))
        {
            Assert.That(block.RowCount, Is.GreaterThan(0));
        }

        ulong stillThere = await TemporaryTableExistsAsync(client, marker);

        Assert.Multiple(() =>
        {
            Assert.That(sum, Is.EqualTo(3UL), "the insert and the query ran on the connection that holds the table");
            Assert.That(
                stillThere,
                Is.EqualTo(1UL),
                "execute, insert, query and stream must each leave a connection the pool can keep");
        });
    }

    /// <summary>
    /// Retiring a connection closes the socket on the server, not only in the client. <c>ConnectionPoolTests</c>
    /// proves the pool calls <c>Close</c>, but against a double whose "closed" is a flag the test itself watches;
    /// only the server's own connection count says the socket went away. A leak here is invisible until a
    /// long-running process runs the server out of <c>max_connections</c>.
    ///
    /// <para>
    /// A one-tick lifetime retires a connection the moment the operation using it ends, so every query below runs
    /// on a connection of its own. How many that really was comes from the server too: <c>system.query_log</c>
    /// records the client port each query arrived on, which is the connection's identity. Without that the
    /// connection count could be flat because nothing was ever opened rather than because everything was closed.
    /// </para>
    ///
    /// <para>
    /// The count is read over a second client held open for the whole test, which therefore contributes one
    /// connection to both readings. The tolerance is for the other framework suites, which share the server and
    /// open connections of their own; forty leaked sockets sit far outside it.
    /// </para>
    /// </summary>
    [Test]
    public async Task Retirement_AfterChurningManyConnections_LeavesNoneOpenOnTheServer()
    {
        const int churns = 40;
        const int tolerance = churns / 4;

        await using ClickHouseTcpClient observer = CreateClient(maxPoolSize: 1);
        long baseline = await ServerConnectionsAsync(observer);
        Assert.That(baseline, Is.GreaterThan(0), "the observer's own connection must be in the count, or this is not the count");

        string tag = $"tcp_churn_{Guid.NewGuid():N}";
        await using (var churning = new ClickHouseTcpClient(TcpServerFixture.Options() with
        {
            MaxPoolSize = 1,
            MaxConnectionLifetime = TimeSpan.FromTicks(1),
        }))
        {
            for (int churn = 0; churn < churns; churn++)
            {
                await churning.ExecuteAsync(
                    "SELECT 1",
                    new ClickHouseTcpQueryOptions { QueryId = $"{tag}_{churn}" },
                    None);
            }
        }

        // HAVING, so the lookup matches no row until every record is flushed: uniqExact over a partial set would
        // answer with a lower number and the retry would stop at it.
        object ports = await QueryLog.ScalarAsync(
            observer,
            $"SELECT toUInt64(uniqExact(port)) FROM system.query_log WHERE query_id LIKE '{tag}%' AND type = 'QueryStart' HAVING count() = {churns}");
        long open = await WaitForServerConnectionsAsync(observer, baseline + tolerance);

        Assert.Multiple(() =>
        {
            Assert.That(
                Convert.ToInt64(ports),
                Is.EqualTo(churns),
                "the server must have seen each query on a client port of its own");
            Assert.That(
                open - baseline,
                Is.LessThanOrEqualTo(tolerance),
                $"connections the pool retired must not stay open on the server (baseline {baseline})");
        });
    }

    [Test]
    public async Task ExecuteAsync_UnparseableSetting_ReplacesClosedConnectionBeforeNextOperation()
    {
        // A settings-list parse failure is raised before the server accepts the query. The server sends the
        // Exception packet and then closes the socket, but its FIN races the pool's immediate return and checkout.
        // Repeat the exact error/follow-up pair so the test cannot pass merely because one FIN arrived promptly.
        const int iterations = 40;
        var invalid = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["max_threads"] = "lots" },
        };
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 1);

        for (int iteration = 0; iteration < iterations; iteration++)
        {
            var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(async () =>
                await client.ExecuteAsync("SELECT 1", invalid, None));
            Assert.That(thrown.Code, Is.EqualTo(ClickHouseErrorCode.CannotParseInputAssertionFailed), $"iteration {iteration + 1}");

            Assert.DoesNotThrowAsync(
                async () => await client.ExecuteAsync("SELECT 1", cancellationToken: None),
                $"iteration {iteration + 1}: the valid statement must not inherit the closed connection");
        }
    }

    // How many native connections the server has open, this client's own among them.
    private static async Task<long> ServerConnectionsAsync(ClickHouseTcpClient client)
    {
        long open = 0;
        await foreach (ValueRow row in client.QueryAsync<ValueRow>(
            "SELECT toUInt64(value) AS id FROM system.metrics WHERE metric = 'TCPConnection'",
            cancellationToken: None))
        {
            open = (long)row.Id;
        }

        return open;
    }

    // Waits for the server's connection count to come down to a limit, and returns the last value read. A
    // close is asynchronous on the server's side, and the count is shared with the other framework suites, so
    // one reading taken right after the churn is not the answer.
    private static async Task<long> WaitForServerConnectionsAsync(ClickHouseTcpClient client, long limit)
    {
        long open = 0;
        for (int attempt = 0; attempt < 20; attempt++)
        {
            open = await ServerConnectionsAsync(client);
            if (open <= limit)
            {
                return open;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        return open;
    }

    private static async Task<ulong> TemporaryTableExistsAsync(
        ClickHouseTcpClient client,
        string name,
        ClickHouseTcpQueryOptions options = null)
    {
        ulong exists = 0;
        await foreach (ValueRow row in client.QueryAsync<ValueRow>(
            $"SELECT toUInt64(count()) AS id FROM system.tables WHERE is_temporary AND name = '{name}'",
            options,
            None))
        {
            exists = row.Id;
        }

        return exists;
    }
}
