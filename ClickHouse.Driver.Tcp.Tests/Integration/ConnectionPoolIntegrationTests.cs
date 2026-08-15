using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;

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

    private static ClickHouseTcpClient CreateClient(int maxPoolSize = 4, TimeSpan? poolTimeout = null)
        => new(TcpServerFixture.Options() with
        {
            MaxPoolSize = maxPoolSize,
            PoolTimeout = poolTimeout ?? TimeSpan.FromSeconds(30),
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
    public async Task InsertAsync_ConcurrentInsertsIntoOneTable_AllRowsLand()
    {
        await using ClickHouseTcpClient client = CreateClient(maxPoolSize: 4);
        string table = UniqueTableName();
        await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = MergeTree ORDER BY id", cancellationToken: None);

        try
        {
            await Task.WhenAll(Enumerable.Range(0, 8).Select(batch => client.InsertAsync(
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

}
