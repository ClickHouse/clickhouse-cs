using System.Diagnostics;
using ClickHouse.Driver.Tcp.Diagnostic;
using ClickHouse.Driver.Tcp.Logging;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Tests.Diagnostic;

// The zero-cost contract, which nothing else pins: an operation nobody is watching must not be built at all, and
// must not even read the statement's leading keyword — that scan allocates, and it would then be charged to every
// query on an unconfigured client.
[TestFixture]
public class ClientOperationTests
{
    private static readonly ClickHouseTcpClientOptions Options = new();

    [Test]
    public void Start_NoListenerAndNoLogger_ReturnsNull()
    {
        Assert.That(ClientOperation.Start(Options, logger: null, "SELECT 1", queryId: null), Is.Null);
    }

    [Test]
    public void Start_LoggerOnly_AccumulatesTheCountersTheCompletionLineNeeds()
    {
        // The counters feed the log line as well as the span, so a logger alone has to switch the accumulator on.
        // Without it the completion line would report zero rows for every query.
        using var factory = new CapturingLoggerFactory();
        CapturingLogger logger = factory.Logger(ClickHouseTcpDiagnostics.ClientLogCategory);

        using (ClientOperation operation = ClientOperation.Start(Options, logger, "SELECT 1", queryId: null))
        {
            Assert.That(operation, Is.Not.Null);
            operation.Telemetry.OnProgress(new ClickHouseTcpProgress(4, 32, 4, 0, 0, 1));
            operation.Telemetry.OnProgress(new ClickHouseTcpProgress(6, 48, 6, 0, 0, 1));
            operation.Succeeded();
        }

        LogEntry completed = logger.WithEventId(1001)[0];
        Assert.That(completed.Message, Does.Contain("reading 10 rows"), "the increments are summed, not overwritten");
    }

    [Test]
    public void Dispose_NeitherSucceededNorFailed_ReportsTheOperationAbandoned()
    {
        using var factory = new CapturingLoggerFactory();
        CapturingLogger logger = factory.Logger(ClickHouseTcpDiagnostics.ClientLogCategory);

        using (ClientOperation operation = ClientOperation.Start(Options, logger, "SELECT 1", queryId: null))
        {
        }

        Assert.Multiple(() =>
        {
            Assert.That(logger.WithEventId(1003), Is.Not.Empty);
            Assert.That(logger.WithEventId(1001), Is.Empty);
        });
    }

    [Test]
    public void Start_QueryIdSet_PutsItOnTheLogLine()
    {
        using var factory = new CapturingLoggerFactory();
        CapturingLogger logger = factory.Logger(ClickHouseTcpDiagnostics.ClientLogCategory);

        using (ClientOperation operation = ClientOperation.Start(Options, logger, "SELECT 1", "my-query-id"))
        {
            operation.Succeeded();
        }

        Assert.Multiple(() =>
        {
            Assert.That(logger.WithEventId(1000)[0].Message, Does.Contain("my-query-id"));
            Assert.That(logger.WithEventId(1001)[0].Message, Does.Contain("my-query-id"), "the completion line carries it too, concurrent operations not being adjacent in a log");
        });
    }

    [Test]
    public void Start_StatementLongerThanTheLimit_TruncatesItInTheLogLine()
    {
        // The same knob that caps the span attribute, so one setting governs how much statement text leaves the
        // client whichever channel it leaves by.
        using var factory = new CapturingLoggerFactory();
        CapturingLogger logger = factory.Logger(ClickHouseTcpDiagnostics.ClientLogCategory);
        ClickHouseTcpClientOptions options = Options with { StatementMaxLength = 6 };

        using (ClientOperation operation = ClientOperation.Start(options, logger, "SELECT 'a very long literal'", queryId: null))
        {
        }

        Assert.That(logger.WithEventId(1000)[0].Message, Does.EndWith("SELECT"));
    }

    [Test]
    public void Start_ZeroStatementMaxLength_KeepsTheStatementOutOfTheLogEntirely()
    {
        using var factory = new CapturingLoggerFactory();
        CapturingLogger logger = factory.Logger(ClickHouseTcpDiagnostics.ClientLogCategory);
        ClickHouseTcpClientOptions options = Options with { StatementMaxLength = 0 };

        using (ClientOperation operation = ClientOperation.Start(options, logger, "SELECT 'secret'", queryId: null))
        {
        }

        Assert.That(logger.WithEventId(1000)[0].Message, Does.Not.Contain("secret"));
    }

    [Test]
    public void Failed_Cancellation_IsNotLoggedAsAnError()
    {
        // A caller cancelling is control flow, not a fault, and an Error line per cancelled query is noise a
        // production configuration cannot filter out without losing real failures.
        using var factory = new CapturingLoggerFactory();
        CapturingLogger logger = factory.Logger(ClickHouseTcpDiagnostics.ClientLogCategory);

        using (ClientOperation operation = ClientOperation.Start(Options, logger, "SELECT 1", queryId: null))
        {
            operation.Failed(new System.OperationCanceledException());
        }

        Assert.Multiple(() =>
        {
            Assert.That(logger.WithEventId(1005), Is.Not.Empty, "reported as cancelled");
            Assert.That(logger.WithEventId(1002), Is.Empty, "and not as a failure");
            Assert.That(logger.Entries, Has.None.Matches<LogEntry>(e => e.Level == LogLevel.Error));
        });
    }
}
