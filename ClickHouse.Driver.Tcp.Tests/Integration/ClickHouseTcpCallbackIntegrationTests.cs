using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// The public callback surface, driven end to end through the client. The connection-level fan-out is covered by
// ClickHouseTcpConnectionMetadataIntegrationTests; what these add is the projection into owned rows and the fact
// that ClickHouseTcpQueryOptions.Callbacks reaches the read path at all.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpCallbackIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static async Task DrainAsync(ClickHouseTcpClient client, string sql, ClickHouseTcpQueryOptions options)
    {
        await foreach (Block block in client.StreamAsync(sql, options, None))
        {
            _ = block.RowCount;
        }
    }

    [Test]
    public async Task StreamAsync_OnProgress_ReportsIncrementsThatSumToTheRowsRead()
    {
        // The decisive test of the documented contract: each packet is an increment, so the sum over a query that
        // reads a known number of rows is that number. Were they running totals the sum would be far larger.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        var increments = new List<ClickHouseTcpProgress>();
        await DrainAsync(
            client,
            "SELECT sum(number) FROM numbers(2000000)",
            new ClickHouseTcpQueryOptions
            {
                Callbacks = new ClickHouseTcpQueryCallbacks { OnProgress = increments.Add },
            });

        ClickHouseTcpProgress total = increments.Aggregate(default(ClickHouseTcpProgress), static (sum, next) => sum + next);
        Assert.Multiple(() =>
        {
            Assert.That(increments, Has.Count.GreaterThan(1), "the server reports progress as the query runs, not once at the end");
            Assert.That(total.Rows, Is.EqualTo(2_000_000UL));
            Assert.That(total.Bytes, Is.GreaterThan(0UL));
        });
    }

    [Test]
    public async Task StreamAsync_OnProfileInfo_ReportsTheResultRowCount()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        var summaries = new List<ClickHouseTcpProfileInfo>();
        await DrainAsync(
            client,
            "SELECT number FROM numbers(10)",
            new ClickHouseTcpQueryOptions
            {
                Callbacks = new ClickHouseTcpQueryCallbacks { OnProfileInfo = summaries.Add },
            });

        Assert.Multiple(() =>
        {
            Assert.That(summaries, Is.Not.Empty);
            Assert.That(summaries[^1].Rows, Is.EqualTo(10UL));
        });
    }

    [Test]
    public async Task StreamAsync_OnServerLog_ProjectsTheServerRowsForThisQuery()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string queryId = Guid.NewGuid().ToString();

        var rows = new List<ClickHouseTcpServerLogRow>();
        await DrainAsync(
            client,
            "SELECT sum(number) FROM numbers(100000)",
            new ClickHouseTcpQueryOptions
            {
                QueryId = queryId,
                Settings = new Dictionary<string, string> { ["send_logs_level"] = "trace" },
                Callbacks = new ClickHouseTcpQueryCallbacks { OnServerLog = rows.Add },
            });

        Assert.That(rows, Is.Not.Empty, "the server streams trace-level log rows");
        Assert.Multiple(() =>
        {
            Assert.That(rows.Select(r => r.QueryId), Is.All.EqualTo(queryId), "every row belongs to the query that asked for them");
            Assert.That(rows.Select(r => r.Text), Is.All.Not.Empty);
            Assert.That(rows.Select(r => r.Level), Is.All.Not.EqualTo(ClickHouseTcpServerLogLevel.Unknown), "the priority column decodes to a known level");
            Assert.That(rows.Select(r => r.Source), Is.All.Not.Null);
            Assert.That(rows.Select(r => r.HostName), Is.All.Not.Null);
            Assert.That(rows.Select(r => r.EventTime), Is.All.GreaterThan(DateTimeOffset.UnixEpoch), "the two time columns combine into a real instant");
            Assert.That(rows.Any(r => r.ThreadId != 0), "at least one row names its thread");
        });
    }

    [Test]
    public async Task StreamAsync_OnServerLog_WithoutSendLogsLevel_ReportsNothing()
    {
        // The callback alone changes nothing on the wire: the server's default log level is effectively silent, so
        // asking for server logs is a two-part act and this is the half the client does not do for you.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        var rows = new List<ClickHouseTcpServerLogRow>();
        await DrainAsync(
            client,
            "SELECT sum(number) FROM numbers(100000)",
            new ClickHouseTcpQueryOptions
            {
                Callbacks = new ClickHouseTcpQueryCallbacks { OnServerLog = rows.Add },
            });

        Assert.That(rows, Is.Empty);
    }

    [Test]
    public async Task StreamAsync_OnProfileEvent_ProjectsNamedCounters()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        var events = new List<ClickHouseTcpProfileEvent>();
        await DrainAsync(
            client,
            "SELECT sum(number) FROM numbers(100000)",
            new ClickHouseTcpQueryOptions
            {
                Callbacks = new ClickHouseTcpQueryCallbacks { OnProfileEvent = events.Add },
            });

        Assert.That(events, Is.Not.Empty, "the server sends performance counters");
        Assert.Multiple(() =>
        {
            Assert.That(events.Select(e => e.Name), Is.All.Not.Empty);
            Assert.That(events.Select(e => e.Type), Is.All.Not.EqualTo(ClickHouseTcpProfileEventType.Unknown), "the type column decodes to a known kind");
            Assert.That(events.Select(e => e.CurrentTime), Is.All.GreaterThan(DateTimeOffset.UnixEpoch));
            Assert.That(events.Select(e => e.HostName), Is.All.Not.Null);
            Assert.That(events.Any(e => e.Name == "SelectedRows"), "a counter every SELECT reports");
        });
    }

    [Test]
    public async Task StreamAsync_OnTotals_LendsTheGrandTotalBlock()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        int blocks = 0;
        ulong grandTotal = 0;
        await DrainAsync(
            client,
            "SELECT number % 3 AS k, count() AS c FROM numbers(100) GROUP BY k WITH TOTALS",
            new ClickHouseTcpQueryOptions
            {
                Callbacks = new ClickHouseTcpQueryCallbacks
                {
                    OnTotals = block =>
                    {
                        blocks++;
                        grandTotal = ((IColumn<ulong>)block[1]).Values[0];
                    },
                },
            });

        Assert.Multiple(() =>
        {
            Assert.That(blocks, Is.EqualTo(1));
            Assert.That(grandTotal, Is.EqualTo(100UL));
        });
    }

    [Test]
    public async Task StreamAsync_OnExtremes_LendsTheMinimumAndMaximumBlock()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        ulong[] extremes = null;
        await DrainAsync(
            client,
            "SELECT number FROM numbers(10)",
            new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string> { ["extremes"] = "1" },
                Callbacks = new ClickHouseTcpQueryCallbacks
                {
                    OnExtremes = block => extremes = ((IColumn<ulong>)block[0]).Values.ToArray(),
                },
            });

        Assert.That(extremes, Is.EqualTo(new ulong[] { 0, 9 }));
    }

    [Test]
    public async Task InsertAsync_Callbacks_ReachTheInsertAndReportTheRowsInserted()
    {
        // Counters rather than progress: the server slices Progress by time, and an insert this small finishes
        // before the first slice, so it sends none at all. ProfileEvents it does send, so those are what show the
        // callbacks reached the write path. Which counters appear is not asserted — the set differs by server
        // version — only that they decode.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string table = $"tcp_callback_test_{Guid.NewGuid():N}";
        await client.ExecuteAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory", cancellationToken: None);

        try
        {
            var events = new List<ClickHouseTcpProfileEvent>();
            await client.InsertAsync(
                $"INSERT INTO {table} (id) VALUES",
                new IColumn[] { PrimitiveColumn<int>.FromValues("id", "Int32", [1, 2, 3, 4, 5]) },
                new ClickHouseTcpInsertOptions
                {
                    Callbacks = new ClickHouseTcpQueryCallbacks { OnProfileEvent = events.Add },
                },
                None);

            List<object[]> stored = await client.QueryAsync($"SELECT count() FROM {table}", cancellationToken: None).ToListAsync();

            Assert.That(events, Is.Not.Empty, "the insert path passes the callbacks through");
            Assert.Multiple(() =>
            {
                Assert.That((ulong)stored[0][0], Is.EqualTo(5UL), "observing the insert did not stop it inserting");
                Assert.That(events.Select(e => e.Name), Is.All.Not.Empty);
                Assert.That(events.Select(e => e.Type), Is.All.Not.EqualTo(ClickHouseTcpProfileEventType.Unknown));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task StreamAsync_ThrowingCallback_PropagatesAndLeavesTheClientUsable()
    {
        // The documented consequence of a throwing callback: the operation fails and the connection is terminated
        // rather than pooled. The client stays usable because the pool redials.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        var failure = new InvalidOperationException("from the callback");

        InvalidOperationException thrown = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await DrainAsync(
                client,
                "SELECT sum(number) FROM numbers(2000000)",
                new ClickHouseTcpQueryOptions
                {
                    Callbacks = new ClickHouseTcpQueryCallbacks { OnProgress = _ => throw failure },
                }));

        Assert.That(thrown, Is.SameAs(failure));

        long survived = 0;
        await foreach (Block block in client.StreamAsync("SELECT 1", cancellationToken: None))
        {
            survived = ((IColumn<byte>)block[0]).Values[0];
        }

        Assert.That(survived, Is.EqualTo(1));
    }
}
