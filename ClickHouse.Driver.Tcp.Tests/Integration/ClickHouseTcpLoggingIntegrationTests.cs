using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Logging;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// What the client logs while running against a real server. The pool's own messages are unit-tested over fake
// connections in ConnectionPoolLoggingTests; these cover the two categories that need a server to say anything —
// the handshake result, and an operation's outcome and counters.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpLoggingIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private CapturingLoggerFactory factory;

    [SetUp]
    public void CreateFactory() => factory = new CapturingLoggerFactory();

    [TearDown]
    public void DisposeFactory() => factory.Dispose();

    private CapturingLogger ClientLogger => factory.Logger(ClickHouseTcpDiagnostics.ClientLogCategory);

    private CapturingLogger ConnectionLogger => factory.Logger(ClickHouseTcpDiagnostics.ConnectionLogCategory);

    private ClickHouseTcpClient CreateClient()
        => new(TcpServerFixture.Options() with { LoggerFactory = factory });

    private static async Task DrainAsync(ClickHouseTcpClient client, string sql)
    {
        await foreach (Block block in client.StreamAsync(sql, cancellationToken: None))
        {
            _ = block.RowCount;
        }
    }

    [Test]
    public async Task StreamAsync_WithALoggerFactory_LogsTheHandshakeResult()
    {
        await using ClickHouseTcpClient client = CreateClient();

        await DrainAsync(client, "SELECT 1");

        Assert.Multiple(() =>
        {
            Assert.That(ConnectionLogger.WithEventId(2000), Is.Not.Empty, "the dial is announced before it is attempted");
            // Both revisions, each labelled, so a reader can tell the one in force from the one advertised.
            string opened = ConnectionLogger.WithEventId(2001).Single().Message;
            Assert.That(opened, Does.Contain($"protocol revision {NegotiatedProtocol.ClientTcpProtocolVersion} in force"));
            Assert.That(opened, Does.Match(@"server advertised \d+"));
        });
    }

    [Test]
    public async Task StreamAsync_WithALoggerFactory_LogsTheStatementAndItsCounters()
    {
        // The default cap allows a stub only, so reading the statement back out of the log has to lift it.
        await using ClickHouseTcpClient client =
            new(TcpServerFixture.Options() with { LoggerFactory = factory, StatementMaxLength = 100 });

        await DrainAsync(client, "SELECT sum(number) FROM numbers(1000)");

        LogEntry started = ClientLogger.WithEventId(1000).Single();
        LogEntry completed = ClientLogger.WithEventId(1001).Single();
        Assert.Multiple(() =>
        {
            Assert.That(started.Level, Is.EqualTo(LogLevel.Debug));
            Assert.That(started.Message, Does.Contain("SELECT sum(number) FROM numbers(1000)"));
            Assert.That(completed.Message, Does.Contain("reading 1000 rows"), "the accumulated progress increments, not the last packet");
            Assert.That(completed.Message, Does.Match(@"\d+(\.\d+)? ms"));
        });
    }

    [Test]
    public async Task StreamAsync_CallerNamedNoQueryId_LogsAGeneratedIdTheServerRecordedUnder()
    {
        // The native protocol never sends back the id a server assigns, so the only id that can be both logged
        // and looked up afterwards is one the client chose. Proving it needs both ends: the id in the log line,
        // and the same id against a row in system.query_log.
        await using ClickHouseTcpClient client = CreateClient();

        await DrainAsync(client, "SELECT sum(number) FROM numbers(1000)");

        // Both lines are read before the lookup: it runs its own queries on this client, which log 1000/1001 too.
        LogEntry started = ClientLogger.WithEventId(1000).Single();
        string completed = ClientLogger.WithEventId(1001).Single().Message;

        Match logged = Regex.Match(started.Message, @"query id ([0-9a-f-]{36})");
        Assert.That(logged.Success, Is.True, $"a generated id is logged, not an empty field: {started.Message}");

        string id = logged.Groups[1].Value;
        object recorded = await QueryLog.ScalarAsync(
            client,
            $"SELECT count() FROM system.query_log WHERE query_id = '{id}' AND type = 'QueryFinish'");

        Assert.Multiple(() =>
        {
            Assert.That(Guid.TryParse(id, out _), Is.True, "the generated id is a GUID, the shape the server uses too");
            Assert.That(recorded, Is.EqualTo(1UL), "the id the client logged is the one the server ran the query under");
            Assert.That(completed, Does.Contain(id), "the completion line carries the same id");
        });
    }

    [Test]
    public async Task StreamAsync_CallerSuppliedAQueryId_LogsThatIdRatherThanAGeneratedOne()
    {
        await using ClickHouseTcpClient client = CreateClient();
        string supplied = "test-" + Guid.NewGuid().ToString("N");

        await foreach (Block block in client.StreamAsync(
            "SELECT 1",
            new ClickHouseTcpQueryOptions { QueryId = supplied },
            cancellationToken: None))
        {
            _ = block.RowCount;
        }

        Assert.That(ClientLogger.WithEventId(1000).Single().Message, Does.Contain($"query id {supplied}"));
    }

    [Test]
    public async Task StreamAsync_ServerRejectsTheStatement_LogsTheFailureAtError()
    {
        await using ClickHouseTcpClient client = CreateClient();

        Assert.ThrowsAsync<ClickHouseTcpServerException>(async () => await DrainAsync(client, "SELECT * FROM no_such_table_here"));

        LogEntry failed = ClientLogger.WithEventId(1002).Single();
        Assert.Multiple(() =>
        {
            Assert.That(failed.Level, Is.EqualTo(LogLevel.Error));
            Assert.That(failed.Exception, Is.TypeOf<ClickHouseTcpServerException>());
            Assert.That(ClientLogger.WithEventId(1001), Is.Empty, "a failed statement is not also reported as completed");
        });
    }

    [Test]
    public async Task StreamAsync_AbandonedMidResult_LogsThatItWasAbandoned()
    {
        await using ClickHouseTcpClient client = CreateClient();

        await foreach (Block block in client.StreamAsync("SELECT number FROM numbers(5000000)", cancellationToken: None))
        {
            _ = block.RowCount;
            break;
        }

        Assert.Multiple(() =>
        {
            Assert.That(ClientLogger.WithEventId(1003), Is.Not.Empty, "an abandoned result is why a connection is discarded rather than reused");
            Assert.That(ClientLogger.WithEventId(1001), Is.Empty);
        });
    }

    [Test]
    public async Task InsertAsync_WithALoggerFactory_LogsTheInsertAsItsOwnOperation()
    {
        await using ClickHouseTcpClient client = CreateClient();
        string table = UniqueTableName();
        await client.ExecuteAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory", cancellationToken: None);

        try
        {
            await client.InsertAsync(
                $"INSERT INTO {table} (id) VALUES",
                new IColumn[] { PrimitiveColumn<int>.FromValues("id", "Int32", [1, 2, 3]) },
                cancellationToken: None);

            IEnumerable<LogEntry> inserts = ClientLogger.WithEventId(1000).Where(e => e.Message.Contains("INSERT", StringComparison.Ordinal));
            Assert.That(inserts, Is.Not.Empty, "the operation name is read from the statement");
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertAsync_WithALoggerFactory_ReportsTheRowsSentRatherThanAnEmptyRead()
    {
        // Against a real server, because the counters an insert has are not the ones a query has: the server sends
        // no Progress packet for rows streamed to it, whatever the size, so a completion line taken from the
        // Progress counters reports every insert as a read of nothing.
        await using ClickHouseTcpClient client = CreateClient();
        string table = UniqueTableName();
        await client.ExecuteAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory", cancellationToken: None);

        try
        {
            await client.InsertAsync(
                $"INSERT INTO {table} (id) VALUES",
                new IColumn[] { PrimitiveColumn<int>.FromValues("id", "Int32", [1, 2, 3]) },
                cancellationToken: None);

            Assert.Multiple(() =>
            {
                Assert.That(ClientLogger.WithEventId(1004).Single().Message, Does.Contain("writing 3 rows"));
                Assert.That(
                    ClientLogger.WithEventId(1001).Where(e => e.Message.Contains("INSERT", StringComparison.Ordinal)),
                    Is.Empty,
                    "an insert is not also reported as a completed read");
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task ExecuteAsync_InsertSelect_ReportsTheRowsTheServerCounted()
    {
        // The other half of the write line, and the half that does come from the server: an INSERT ... SELECT sends
        // no rows from the client, and the server reports what it wrote in a Progress packet. Asserted against a
        // real server because the counters an insert has are a server behaviour, not a client one.
        await using ClickHouseTcpClient client = CreateClient();
        string table = UniqueTableName();
        await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = Memory", cancellationToken: None);

        try
        {
            await client.ExecuteAsync($"INSERT INTO {table} SELECT number FROM numbers(1000)", cancellationToken: None);

            LogEntry wrote = ClientLogger.WithEventId(1004).Single();
            Assert.That(wrote.Message, Does.Contain("writing 1000 rows"));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task StreamAsync_ThrowingLogger_RunsTheStatementAnyway()
    {
        // A logger is infrastructure, so losing a log line beats losing the query. This is the opposite of the
        // rule for a caller callback, which is documented to propagate and end the operation.
        await using ClickHouseTcpClient client = new(TcpServerFixture.Options() with { LoggerFactory = new ThrowingLoggerFactory() });

        List<object[]> rows = await client.QueryAsync("SELECT 1", cancellationToken: None).ToListAsync();

        Assert.That((byte)rows[0][0], Is.EqualTo((byte)1));
    }

    private static string UniqueTableName() => $"tcp_logging_test_{Guid.NewGuid():N}";

    [Test]
    public async Task StreamAsync_LoggerFactoryBelowDebug_LogsNothingForASuccessfulStatement()
    {
        // The Debug lines are the chatty ones, and a production configuration at Information should carry none of
        // them. The Error line for a failure is not gated this way.
        factory.MinimumLevel = LogLevel.Information;
        await using ClickHouseTcpClient client = CreateClient();

        await DrainAsync(client, "SELECT 1");

        Assert.Multiple(() =>
        {
            Assert.That(ClientLogger.Entries, Is.Empty);
            Assert.That(ConnectionLogger.Entries, Is.Empty);
        });
    }
}
