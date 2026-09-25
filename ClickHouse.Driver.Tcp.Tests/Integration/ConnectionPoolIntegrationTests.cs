using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Covers pool behavior that requires real connections and server responses.
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public class ConnectionPoolIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    /// <summary>An operation that fails after acquiring a pool permit.</summary>
    public enum FailingOperation
    {
        /// <summary>A query rejected by the server.</summary>
        QueryTheServerRefuses,

        /// <summary>An insert with a column absent from the schema block.</summary>
        InsertOfAnUnknownColumn,

        /// <summary>An insert whose columns have different row counts.</summary>
        InsertOfRaggedColumns,
    }

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
        // Four serialized one-second queries would exceed the 3.5-second bound.
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
        // Distinct row counts detect responses crossing between concurrent operations.
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

            // The eight inserts commit on whichever replica their connection reached, so the count has to be
            // read with sequential consistency to include all of them. On a single server there is nothing to
            // wait for and the setting does nothing.
            var readAll = new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string> { ["select_sequential_consistency"] = "1" },
            };

            ulong count = 0;
            await foreach (ValueRow row in client.QueryAsync<ValueRow>(
                $"SELECT count() AS id FROM {table}", readAll, None))
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
        // An unfinished stream retains its connection and exhausts a pool of one.
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

        // Disposing the stream returns the connection.
        Assert.DoesNotThrowAsync(async () => await client.ExecuteAsync("SELECT 1", cancellationToken: None));
    }

    [Test]
    public async Task QueryAsync_AfterThePoolSatIdlePastTheIdleTimeout_RunsOnAFreshConnection()
    {
        // A temporary table identifies the connection. It disappears when the idle connection is replaced.
        // Five seconds leaves enough time to verify the marker before the concurrent test suites can delay it.
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
    /// Verifies that checkout replaces a connection closed by the server while idle.
    /// The null case verifies that waiting alone does not replace the connection.
    /// </summary>
    /// <param name="serverIdleTimeout">Seconds for the server's <c>idle_connection_timeout</c>, or null to leave it.</param>
    /// <param name="markerAfterTheWait">Whether the connection-scoped marker should remain.</param>
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

        // Poll because the server closes idle connections asynchronously. Each interval exceeds its timeout.
        ulong markerAfterwards = 0;
        int attempts = serverIdleTimeout is null ? 1 : 12;
        Assert.DoesNotThrowAsync(async () =>
        {
            for (int attempt = 0; attempt < attempts; attempt++)
            {
                await Task.Delay(TimeSpan.FromSeconds(3));
                markerAfterwards = await TemporaryTableExistsAsync(client, marker, options);
                if (markerAfterwards == markerAfterTheWait)
                {
                    return;
                }
            }
        });

        Assert.That(markerAfterwards, Is.EqualTo(markerAfterTheWait));
    }

    [Test]
    public async Task Return_AfterEachKindOfOperation_KeepsTheSameConnection()
    {
        // The temporary table proves each operation returned the same reusable connection.
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
    /// Verifies that retired connections close their server-side sockets.
    /// Each query uses a distinct connection; the observer remains in both counts. The tolerance allows for
    /// connections opened by concurrent framework suites.
    /// </summary>
    [Test]
    public async Task Retirement_AfterChurningManyConnections_LeavesNoneOpenOnTheServer()
    {
        if (TcpServerFixture.IsCloud)
        {
            // The TCPConnection metric counts one replica only, and each connection can go to any replica.
            Assert.Ignore("On Cloud the open-connection count covers one replica, not all connections of this client.");
        }

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

        // HAVING prevents QueryLog.ScalarAsync from accepting a partial result.
        object ports = await QueryLog.ScalarAsync(
            observer,
            $"SELECT toUInt64(uniqExact(port)) FROM {QueryLog.Table} WHERE query_id LIKE '{tag}%' AND type = 'QueryStart' HAVING count() = {churns}");
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
        // Repeat because the server's FIN races the pool's return and next checkout.
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

    /// <summary>
    /// Verifies that server, schema, and pre-connection failures return a pool permit.
    /// </summary>
    /// <param name="failing">The operation to repeat.</param>
    /// <param name="expected">The expected exception type.</param>
    [TestCase(FailingOperation.QueryTheServerRefuses, typeof(ClickHouseTcpServerException))]
    [TestCase(FailingOperation.InsertOfAnUnknownColumn, typeof(ArgumentException))]
    [TestCase(FailingOperation.InsertOfRaggedColumns, typeof(ArgumentException))]
    public async Task Failure_RepeatedAtAPoolOfOne_ReturnsThePermitEveryTime(FailingOperation failing, Type expected)
    {
        const int attempts = 3;

        // A leaked permit makes the next attempt fail after this timeout.
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 1, poolTimeout: TimeSpan.FromSeconds(5));
        string table = UniqueTableName();
        await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = MergeTree ORDER BY id", cancellationToken: None);
        try
        {
            for (int attempt = 1; attempt <= attempts; attempt++)
            {
                Exception thrown = Assert.CatchAsync(async () => await FailAsync(client, failing, table));
                Assert.That(thrown, Is.InstanceOf(expected), $"attempt {attempt}");
            }

            ulong rows = 0;
            await foreach (ValueRow row in client.QueryAsync<ValueRow>(
                $"SELECT toUInt64(count()) AS id FROM {table}", cancellationToken: None))
            {
                rows = row.Id;
            }

            Assert.That(rows, Is.Zero, "a refused insert must leave no row behind");
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    private static async Task FailAsync(ClickHouseTcpClient client, FailingOperation failing, string table)
    {
        switch (failing)
        {
            case FailingOperation.QueryTheServerRefuses:
                await client.ExecuteAsync($"SELECT count() FROM {table}_absent", cancellationToken: None);
                break;

            case FailingOperation.InsertOfAnUnknownColumn:
                IColumn[] unknown =
                [
                    PrimitiveColumn<ulong>.FromValues("id", "UInt64", [1, 2]),
                    PrimitiveColumn<ulong>.FromValues("absent", "UInt64", [1, 2]),
                ];
                await client.InsertAsync($"INSERT INTO {table} (id) VALUES", unknown, cancellationToken: None);
                break;

            case FailingOperation.InsertOfRaggedColumns:
                IColumn[] ragged =
                [
                    PrimitiveColumn<ulong>.FromValues("id", "UInt64", [1, 2]),
                    PrimitiveColumn<ulong>.FromValues("second", "UInt64", [1, 2, 3]),
                ];
                await client.InsertAsync($"INSERT INTO {table} (id) VALUES", ragged, cancellationToken: None);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(failing), failing, "unhandled failure");
        }
    }

    // Returns the server's open native-connection count, including this client.
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

    // Server-side connection closure is asynchronous, so wait for the count to settle.
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
