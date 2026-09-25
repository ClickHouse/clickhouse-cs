using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Tracing against a real server. The attribute shapes are unit-tested in TcpActivityTests; what needs a server is
// the half a self-consistent client cannot check on its own — that the trace context the client encodes into
// ClientInfo is the trace id the *server* then records, which is the only thing that proves the two ids' byte
// order rather than merely proving the writer agrees with the reader.
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public class ClickHouseTcpTracingIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private List<Activity> finished;
    private ActivityListener listener;

    [SetUp]
    public void Subscribe()
    {
        finished = [];
        listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ClickHouseTcpDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> o) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> o) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => finished.Add(activity),
        };
        ActivitySource.AddActivityListener(listener);
    }

    [TearDown]
    public void Unsubscribe() => listener.Dispose();

    [Test]
    public async Task StreamAsync_WithAListener_ProducesOneSpanCarryingTheServerCounters()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        await foreach (Block block in client.StreamAsync("SELECT sum(number) FROM numbers(500000)", cancellationToken: None))
        {
            _ = block.RowCount;
        }

        Activity span = finished.Single(a => a.OperationName == "SELECT");
        Assert.Multiple(() =>
        {
            Assert.That(span.Status, Is.EqualTo(ActivityStatusCode.Ok));
            Assert.That(span.GetTagItem("db.clickhouse.read_rows"), Is.EqualTo(500_000UL), "the accumulated progress increments");
            Assert.That(span.GetTagItem("db.clickhouse.result_rows"), Is.EqualTo(1UL), "one aggregate row");
            Assert.That(span.GetTagItem("server.address"), Is.EqualTo(TcpServerFixture.Host));
            Assert.That(span.Duration, Is.GreaterThan(TimeSpan.Zero));
        });
    }

    [Test]
    public async Task InsertAsync_WithAListener_ReportsTheRowsSentAndNoReadCounters()
    {
        // The server sends no Progress packet for rows a client streams to it, at any size, so an insert's span has
        // only the count the client itself knows. Read counters of zero would claim the server reported reading
        // nothing rather than reporting nothing at all, and a zero elapsed_ns would claim it took no time.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        await client.ExecuteAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory", cancellationToken: None);

        try
        {
            finished.Clear();
            await client.InsertAsync(
                $"INSERT INTO {table} (id) VALUES",
                new IColumn[] { PrimitiveColumn<int>.FromValues("id", "Int32", [1, 2, 3]) },
                cancellationToken: None);

            Activity span = finished.Single(a => a.OperationName == "INSERT");
            Assert.Multiple(() =>
            {
                Assert.That(span.GetTagItem("db.clickhouse.written_rows"), Is.EqualTo(3UL));
                Assert.That(span.GetTagItem("db.clickhouse.read_rows"), Is.Null);
                Assert.That(span.GetTagItem("db.clickhouse.elapsed_ns"), Is.Null);
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task ExecuteAsync_InsertSelect_ReportsTheCountersTheServerSent()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = Memory", cancellationToken: None);

        try
        {
            finished.Clear();
            await client.ExecuteAsync($"INSERT INTO {table} SELECT number FROM numbers(1000)", cancellationToken: None);

            Activity span = finished.Single(a => a.OperationName == "INSERT");
            Assert.Multiple(() =>
            {
                Assert.That(span.GetTagItem("db.clickhouse.written_rows"), Is.EqualTo(1000UL));
                Assert.That(span.GetTagItem("db.clickhouse.written_bytes"), Is.Not.Null, "only the server reports these");
                Assert.That(span.GetTagItem("db.clickhouse.read_rows"), Is.EqualTo(1000UL), "the SELECT half is read as well as written");
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task StreamAsync_ServerRejectsTheStatement_MarksTheSpanFailedWithTheServerErrorCode()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        Assert.ThrowsAsync<ClickHouseTcpServerException>(async () =>
        {
            await foreach (Block block in client.StreamAsync("SELECT * FROM no_such_table_here", cancellationToken: None))
            {
                _ = block.RowCount;
            }
        });

        Activity span = finished.Single(a => a.OperationName == "SELECT");
        Assert.Multiple(() =>
        {
            Assert.That(span.Status, Is.EqualTo(ActivityStatusCode.Error));
            Assert.That(span.GetTagItem("error.type"), Is.EqualTo(typeof(ClickHouseTcpServerException).FullName));
            Assert.That(span.GetTagItem("db.response.status_code"), Is.Not.Null, "the server's error code reaches the span");
            Assert.That(span.Events.Any(e => e.Name == "exception"));
        });
    }

    [Test]
    public async Task StreamAsync_AbandonedMidResult_LeavesTheSpanWithoutAStatus()
    {
        // Breaking out of the loop is neither success nor failure, and reporting either would be a claim the
        // client cannot make about a result the caller stopped reading.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        await foreach (Block block in client.StreamAsync("SELECT number FROM numbers(5000000)", cancellationToken: None))
        {
            _ = block.RowCount;
            break;
        }

        Activity span = finished.Single(a => a.OperationName == "SELECT");
        Assert.That(span.Status, Is.EqualTo(ActivityStatusCode.Unset));
    }

    [Test]
    public async Task PingAsync_WithAListener_ProducesAPingSpan()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        await client.PingAsync(None);

        Assert.That(finished.Any(a => a.OperationName == "ping" && a.Status == ActivityStatusCode.Ok));
    }

    [Test]
    public async Task PingAsync_FirstOperationOnANewClient_ProducesAConnectSpan()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        await client.PingAsync(None);

        Activity connect = finished.Single(a => a.OperationName == "connect");
        Assert.Multiple(() =>
        {
            Assert.That(connect.Status, Is.EqualTo(ActivityStatusCode.Ok));
            Assert.That(connect.GetTagItem("server.port"), Is.EqualTo(TcpServerFixture.Port));
        });
    }

    [Test]
    public async Task StreamAsync_UnderARecordedSpan_SendsATraceContextTheServerRecordsAgainstTheSameTraceId()
    {
        // The end-to-end proof of the ClientInfo trace-context encoding. The server decodes trace_id as a UUID and
        // writes its own spans against it, so finding our trace id in system.opentelemetry_span_log means the two
        // byte-swapped halves and the little-endian span id were all read the way we wrote them. Getting the order
        // wrong yields a different, self-consistent trace id here and no matching row.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["opentelemetry_start_trace_probability"] = "1" },
        };

        // Cleared so the one SELECT span below is the probe query's, not this fixture's earlier spans.
        finished.Clear();
        await foreach (Block block in client.StreamAsync("SELECT sum(number) FROM numbers(1000)", options, None))
        {
            _ = block.RowCount;
        }

        // The span the Query packet's trace-context field was written from: it is the ambient activity inside the
        // iterator, where the packet is encoded.
        string traceId = finished.Single(a => a.OperationName == "SELECT").TraceId.ToHexString();

        ulong spans = await CountServerSpansAsync(client, traceId);
        if (spans == 0 && !await SpanLogExistsAsync(client))
        {
            // The table is created on demand, the first time the server records a span, so it can only be
            // checked for after the query above — not before it.
            Assert.Ignore("system.opentelemetry_span_log is not configured on this server.");
        }

        Assert.That(spans, Is.GreaterThan(0UL), $"the server recorded no span against trace id {traceId}");
    }

    [Test]
    public async Task StreamAsync_WithNoListener_SendsNoTraceContextAndStillRuns()
    {
        // Nothing listening means no ambient span, so the trace-context field takes its absent form. The query has
        // to run exactly as before — a regression here would break every query, not just a traced one.
        listener.Dispose();
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        ulong value = 0;
        await foreach (Block block in client.StreamAsync("SELECT sum(number) FROM numbers(10)", cancellationToken: None))
        {
            value = ((IColumn<ulong>)block[0]).Values[0];
        }

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo(45UL));
            Assert.That(finished, Is.Empty, "no listener, no spans");
        });
    }

    private static string UniqueTableName() => $"tcp_tracing_test_{Guid.NewGuid():N}";

    private static async Task<bool> SpanLogExistsAsync(ClickHouseTcpClient client)
    {
        List<object[]> rows = await client
            .QueryAsync("SELECT count() FROM system.tables WHERE database = 'system' AND name = 'opentelemetry_span_log'", cancellationToken: None)
            .ToListAsync();
        return (ulong)rows[0][0] != 0;
    }

    // A span's log row is queued independently of the query's response reaching us, so a flush issued right after
    // can miss it. Retried rather than slept on, and reported as zero only after the last attempt.
    private static async Task<ulong> CountServerSpansAsync(ClickHouseTcpClient client, string traceIdHex)
    {
        string uuid = $"{traceIdHex[..8]}-{traceIdHex[8..12]}-{traceIdHex[12..16]}-{traceIdHex[16..20]}-{traceIdHex[20..]}";
        for (int attempt = 0; attempt < 5; attempt++)
        {
            await client.ExecuteAsync("SYSTEM FLUSH LOGS", cancellationToken: None);
            if (!await SpanLogExistsAsync(client))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200), None);
                continue;
            }

            List<object[]> rows = await client
                .QueryAsync($"SELECT count() FROM system.opentelemetry_span_log WHERE trace_id = toUUID('{uuid}')", cancellationToken: None)
                .ToListAsync();
            var count = (ulong)rows[0][0];
            if (count != 0)
            {
                return count;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(200), None);
        }

        return 0;
    }
}
