using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// The public callback surface, driven end to end through the client: that ClickHouseTcpQueryOptions.Callbacks
// reaches the read path at all, what each packet carries, and that the Log and ProfileEvents blocks have the
// schema the callback docs promise — a caller reads those by column name, so a rename on the server has to fail
// here. ClickHouseTcpConnectionMetadataIntegrationTests covers the layer below, where a packet leaving the
// connection unusable would show.
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
    public async Task StreamAsync_OnLog_LendsBlocksCarryingTheDocumentedLogSchema()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string queryId = Guid.NewGuid().ToString();

        // Copied out inside the callback: the block is borrowed and released as soon as it returns.
        var queryIds = new List<string>();
        var texts = new List<string>();
        var sources = new List<string>();
        var priorities = new List<sbyte>();
        var threadIds = new List<ulong>();
        var instants = new List<DateTimeOffset>();

        await DrainAsync(
            client,
            "SELECT sum(number) FROM numbers(100000)",
            new ClickHouseTcpQueryOptions
            {
                QueryId = queryId,
                Settings = new Dictionary<string, string> { ["send_logs_level"] = "trace" },
                Callbacks = new ClickHouseTcpQueryCallbacks
                {
                    OnLog = block =>
                    {
                        ReadOnlySpan<uint> seconds = block.Column<uint>("event_time").Values;
                        ReadOnlySpan<uint> micros = block.Column<uint>("event_time_microseconds").Values;
                        ReadOnlySpan<sbyte> priority = block.Column<sbyte>("priority").Values;
                        ReadOnlySpan<ulong> threadId = block.Column<ulong>("thread_id").Values;
                        IColumn<string> id = block.Column<string>("query_id");
                        IColumn<string> source = block.Column<string>("source");
                        IColumn<string> text = block.Column<string>("text");

                        for (int row = 0; row < block.RowCount; row++)
                        {
                            queryIds.Add(id[row]);
                            texts.Add(text[row]);
                            sources.Add(source[row]);
                            priorities.Add(priority[row]);
                            threadIds.Add(threadId[row]);
                            instants.Add(DateTimeOffset.FromUnixTimeSeconds(seconds[row])
                                .AddTicks(micros[row] * TimeSpan.TicksPerMicrosecond));
                        }
                    },
                },
            });

        Assert.That(texts, Is.Not.Empty, "the server streams trace-level log rows");
        Assert.Multiple(() =>
        {
            Assert.That(queryIds, Is.All.EqualTo(queryId), "every row belongs to the query that asked for them");
            Assert.That(texts, Is.All.Not.Empty);
            Assert.That(sources, Is.All.Not.Null);
            Assert.That(priorities, Is.All.InRange((sbyte)1, (sbyte)9), "a Poco severity, lower being more severe");
            Assert.That(threadIds.Any(id => id != 0), "at least one row names its thread");
            Assert.That(instants, Is.All.GreaterThan(DateTimeOffset.UnixEpoch), "the two time columns combine into a real instant");
        });
    }

    [Test]
    public async Task StreamAsync_OnLog_WithoutSendLogsLevel_ReportsNothing()
    {
        // The callback alone changes nothing on the wire: the server's default log level is effectively silent, so
        // asking for server logs is a two-part act and this is the half the client does not do for you.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        int blocks = 0;
        await DrainAsync(
            client,
            "SELECT sum(number) FROM numbers(100000)",
            new ClickHouseTcpQueryOptions
            {
                Callbacks = new ClickHouseTcpQueryCallbacks { OnLog = _ => blocks++ },
            });

        Assert.That(blocks, Is.Zero);
    }

    [Test]
    public async Task StreamAsync_OnProfileEvents_LendsBlocksCarryingTheDocumentedCounterSchema()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        var names = new List<string>();
        var types = new List<sbyte>();
        var hosts = new List<string>();
        var instants = new List<DateTimeOffset>();

        await DrainAsync(
            client,
            "SELECT sum(number) FROM numbers(100000)",
            new ClickHouseTcpQueryOptions
            {
                Callbacks = new ClickHouseTcpQueryCallbacks { OnProfileEvents = block => Collect(block) },
            });

        Assert.That(names, Is.Not.Empty, "the server sends performance counters");
        Assert.Multiple(() =>
        {
            Assert.That(names, Is.All.Not.Empty);
            Assert.That(types, Is.All.InRange((sbyte)1, (sbyte)2), "1 increment, 2 gauge");
            Assert.That(hosts, Is.All.Not.Null);
            Assert.That(instants, Is.All.GreaterThan(DateTimeOffset.UnixEpoch));
            Assert.That(names, Does.Contain("SelectedRows"), "a counter every SELECT reports");
        });

        void Collect(Block block)
        {
            ReadOnlySpan<uint> currentTime = block.Column<uint>("current_time").Values;
            ReadOnlySpan<sbyte> type = block.Column<sbyte>("type").Values;
            IColumn<string> host = block.Column<string>("host_name");
            IColumn<string> name = block.Column<string>("name");

            // Bound as a span to pin the width the docs promise: a server sending another integer type throws.
            ReadOnlySpan<long> value = block.Column<long>("value").Values;

            for (int row = 0; row < block.RowCount; row++)
            {
                names.Add(name[row]);
                types.Add(type[row]);
                hosts.Add(host[row]);
                instants.Add(DateTimeOffset.FromUnixTimeSeconds(currentTime[row]));
                _ = value[row];
            }
        }
    }

    [Test]
    public async Task StreamAsync_OnTotals_LendsTheGrandTotalBlock()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        int blocks = 0;
        int rows = 0;
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
                        rows = block.RowCount;
                        grandTotal = ((IColumn<ulong>)block[1]).Values[0];
                    },
                },
            });

        Assert.Multiple(() =>
        {
            Assert.That(blocks, Is.EqualTo(1));
            Assert.That(rows, Is.EqualTo(1), "the single totals row");
            Assert.That(grandTotal, Is.EqualTo(100UL));
        });
    }

    [Test]
    public async Task StreamAsync_OnExtremes_LendsTheMinimumAndMaximumBlock()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        int blocks = 0;
        ulong[] extremes = null;
        await DrainAsync(
            client,
            "SELECT number FROM numbers(10)",
            new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string> { ["extremes"] = "1" },
                Callbacks = new ClickHouseTcpQueryCallbacks
                {
                    OnExtremes = block =>
                    {
                        blocks++;
                        extremes = ((IColumn<ulong>)block[0]).Values.ToArray();
                    },
                },
            });

        Assert.Multiple(() =>
        {
            Assert.That(blocks, Is.EqualTo(1), "exactly one Extremes block");
            Assert.That(extremes, Is.EqualTo(new ulong[] { 0, 9 }), "the minimum then the maximum");
        });
    }

    [Test]
    public async Task InsertAsync_Callbacks_ReachTheInsertAndReportTheRowsInserted()
    {
        // Counters rather than progress: the server slices Progress by time, and an insert this small finishes
        // before the first slice, so it sends none at all. ProfileEvents it does send, so those are what show the
        // callbacks reached the write path. Which counters appear is not asserted — the set differs by server
        // version — only that they decode.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        await client.ExecuteAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory", cancellationToken: None);

        try
        {
            var names = new List<string>();
            var types = new List<sbyte>();
            await client.InsertAsync(
                $"INSERT INTO {table} (id) VALUES",
                new IColumn[] { PrimitiveColumn<int>.FromValues("id", "Int32", [1, 2, 3, 4, 5]) },
                new ClickHouseTcpInsertOptions
                {
                    Callbacks = new ClickHouseTcpQueryCallbacks
                    {
                        OnProfileEvents = block =>
                        {
                            ReadOnlySpan<sbyte> type = block.Column<sbyte>("type").Values;
                            IColumn<string> name = block.Column<string>("name");
                            for (int row = 0; row < block.RowCount; row++)
                            {
                                names.Add(name[row]);
                                types.Add(type[row]);
                            }
                        },
                    },
                },
                None);

            List<object[]> stored = await client.QueryAsync($"SELECT count() FROM {table}", cancellationToken: None).ToListAsync();

            Assert.That(names, Is.Not.Empty, "the insert path passes the callbacks through");
            Assert.Multiple(() =>
            {
                Assert.That((ulong)stored[0][0], Is.EqualTo(5UL), "observing the insert did not stop it inserting");
                Assert.That(names, Is.All.Not.Empty);
                Assert.That(types, Is.All.InRange((sbyte)1, (sbyte)2));
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

    private static string UniqueTableName() => $"tcp_callback_test_{Guid.NewGuid():N}";
}
