using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using ClickHouse.Driver.Tcp.Diagnostic;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Diagnostic;

// The span shape, asserted through an ActivityListener. What a server round-trip cannot reach: which attributes
// are set, the exact attribute names (a renamed one is invisible to a working query), the SQL gating, and the
// error status.
[TestFixture]
public class TcpActivityTests
{
    private static readonly ClickHouseTcpClientOptions Options = new()
    {
        Host = "example.invalid",
        Port = 9123,
        Database = "analytics",
        Username = "reader",
    };

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

    [TestCase("SELECT 1", "SELECT")]
    [TestCase("   select number FROM numbers(1)", "SELECT")]
    [TestCase("INSERT INTO t VALUES", "INSERT")]
    [TestCase("WITH x AS (SELECT 1) SELECT * FROM x", "WITH")]
    [TestCase("", null)]
    [TestCase("   ", null)]
    [TestCase(null, null)]
    [TestCase("42", null)]
    [TestCase("/* comment */ SELECT 1", null)]
    [TestCase("SUPERCALIFRAGILISTIC 1", null)]
    public void OperationName_Statement_ReadsTheLeadingKeywordOrNothing(string sql, string expected)
        => Assert.That(TcpActivity.OperationName(sql), Is.EqualTo(expected));

    [Test]
    public void StartStatement_WithAListener_SetsTheEndpointAttributes()
    {
        using (Activity activity = TcpActivity.StartStatement(Options, "SELECT 1", "SELECT", queryId: null))
        {
            Assert.That(activity, Is.Not.Null);
        }

        Activity span = Single();
        Assert.Multiple(() =>
        {
            Assert.That(span.OperationName, Is.EqualTo("SELECT"), "the span is named after the operation");
            Assert.That(span.Kind, Is.EqualTo(ActivityKind.Client));
            Assert.That(span.GetTagItem("db.system.name"), Is.EqualTo("clickhouse"));
            Assert.That(span.GetTagItem("db.namespace"), Is.EqualTo("analytics"));
            Assert.That(span.GetTagItem("db.operation.name"), Is.EqualTo("SELECT"));
            Assert.That(span.GetTagItem("db.user"), Is.EqualTo("reader"));
            Assert.That(span.GetTagItem("server.address"), Is.EqualTo("example.invalid"));
            Assert.That(span.GetTagItem("server.port"), Is.EqualTo(9123));
        });
    }

    [Test]
    public void StartStatement_UnreadableOperationName_NamesTheSpanQueryAndOmitsTheAttribute()
    {
        TcpActivity.StartStatement(Options, "/* only a comment */", null, queryId: null)?.Dispose();

        Activity span = Single();
        Assert.Multiple(() =>
        {
            Assert.That(span.OperationName, Is.EqualTo("query"));
            Assert.That(span.GetTagItem("db.operation.name"), Is.Null, "no keyword read means no attribute rather than a guess");
        });
    }

    [Test]
    public void StartStatement_SqlNotIncluded_OmitsTheQueryText()
    {
        TcpActivity.StartStatement(Options, "SELECT 'secret'", "SELECT", queryId: null)?.Dispose();

        Assert.That(Single().GetTagItem("db.query.text"), Is.Null, "the statement is not in a trace by default");
    }

    [Test]
    public void StartStatement_SqlIncluded_SetsTheQueryText()
    {
        ClickHouseTcpClientOptions options = Options with { IncludeSqlInActivityTags = true, StatementMaxLength = 100 };

        TcpActivity.StartStatement(options, "SELECT 1", "SELECT", queryId: null)?.Dispose();

        Assert.That(Single().GetTagItem("db.query.text"), Is.EqualTo("SELECT 1"));
    }

    [Test]
    public void StartStatement_SqlLongerThanTheLimit_TruncatesTheQueryText()
    {
        ClickHouseTcpClientOptions options = Options with { IncludeSqlInActivityTags = true, StatementMaxLength = 6 };

        TcpActivity.StartStatement(options, "SELECT 1", "SELECT", queryId: null)?.Dispose();

        Assert.That(Single().GetTagItem("db.query.text"), Is.EqualTo("SELECT"));
    }

    [Test]
    public void StartStatement_ZeroStatementMaxLength_OmitsTheQueryText()
    {
        ClickHouseTcpClientOptions options = Options with { IncludeSqlInActivityTags = true, StatementMaxLength = 0 };

        TcpActivity.StartStatement(options, "SELECT 1", "SELECT", queryId: null)?.Dispose();

        Assert.That(Single().GetTagItem("db.query.text"), Is.Null);
    }

    [Test]
    public void StartStatement_QueryIdSet_ReportsItAsAnAttribute()
    {
        // The join key to the server's own record of the same query.
        TcpActivity.StartStatement(Options, "SELECT 1", "SELECT", "my-query-id")?.Dispose();

        Assert.That(Single().GetTagItem("db.clickhouse.query_id"), Is.EqualTo("my-query-id"));
    }

    [Test]
    public void StartStatement_NoQueryId_OmitsTheAttribute()
    {
        // Nothing useful to report: without one the server assigns an id the client never sees.
        TcpActivity.StartStatement(Options, "SELECT 1", "SELECT", queryId: null)?.Dispose();

        Assert.That(Single().GetTagItem("db.clickhouse.query_id"), Is.Null);
    }

    [Test]
    public void SetCounters_ReadCounters_SetsTheReadAttributesAndOmitsTheWriteOnes()
    {
        using (Activity activity = TcpActivity.StartStatement(Options, "SELECT 1", "SELECT", queryId: null))
        {
            activity.SetCounters(new ClickHouseTcpProgress(rows: 12, bytes: 96, totalRows: 12, wroteRows: 0, wroteBytes: 0, elapsedNs: 5000), rowsSent: null);
        }

        Activity span = Single();
        Assert.Multiple(() =>
        {
            Assert.That(span.GetTagItem("db.clickhouse.read_rows"), Is.EqualTo(12UL));
            Assert.That(span.GetTagItem("db.clickhouse.read_bytes"), Is.EqualTo(96UL));
            Assert.That(span.GetTagItem("db.clickhouse.elapsed_ns"), Is.EqualTo(5000UL));
            Assert.That(span.GetTagItem("db.clickhouse.written_rows"), Is.Null, "a SELECT writes nothing, so a zero tag would be noise");
            Assert.That(span.GetTagItem("db.clickhouse.written_bytes"), Is.Null);
        });
    }

    [Test]
    public void SetCounters_ProgressWithoutWriteCounters_StillReportsTheRowsTheClientSent()
    {
        using (Activity activity = TcpActivity.StartStatement(Options, "INSERT INTO t VALUES", "INSERT", queryId: null))
        {
            activity.SetCounters(new ClickHouseTcpProgress(rows: 0, bytes: 0, totalRows: 0, wroteRows: 0, wroteBytes: 0, elapsedNs: 4000), rowsSent: 3);
        }

        Activity span = Single();
        Assert.Multiple(() =>
        {
            Assert.That(span.GetTagItem("db.clickhouse.written_rows"), Is.EqualTo(3UL));
            Assert.That(span.GetTagItem("db.clickhouse.elapsed_ns"), Is.EqualTo(4000UL));
            Assert.That(span.GetTagItem("db.clickhouse.written_bytes"), Is.Null, "nothing reported it");
        });
    }

    [Test]
    public void SetProfileInfo_Summary_SetsTheResultAttributes()
    {
        using (Activity activity = TcpActivity.StartStatement(Options, "SELECT 1", "SELECT", queryId: null))
        {
            activity.SetProfileInfo(new ClickHouseTcpProfileInfo(rows: 3, blocks: 1, bytes: 24, appliedLimit: false, rowsBeforeLimit: 0, calculatedRowsBeforeLimit: false));
        }

        Activity span = Single();
        Assert.Multiple(() =>
        {
            Assert.That(span.GetTagItem("db.clickhouse.result_rows"), Is.EqualTo(3UL));
            Assert.That(span.GetTagItem("db.clickhouse.result_bytes"), Is.EqualTo(24UL));
        });
    }

    [Test]
    public void SetError_ServerException_SetsTheErrorStatusAndTheServerErrorCode()
    {
        var failure = new ClickHouseTcpServerException(60, "UNKNOWN_TABLE", "Table missing", "stack");

        using (Activity activity = TcpActivity.StartStatement(Options, "SELECT 1", "SELECT", queryId: null))
        {
            activity.SetError(failure);
        }

        Activity span = Single();
        Assert.Multiple(() =>
        {
            Assert.That(span.Status, Is.EqualTo(ActivityStatusCode.Error));
            Assert.That(span.StatusDescription, Is.EqualTo("Table missing"));
            Assert.That(span.GetTagItem("error.type"), Is.EqualTo(typeof(ClickHouseTcpServerException).FullName));
            Assert.That(span.GetTagItem("db.response.status_code"), Is.EqualTo("60"), "the server's own error code");
        });
    }

    [Test]
    public void SetError_AnyException_RecordsAnExceptionEvent()
    {
        var failure = new InvalidOperationException("broken");

        using (Activity activity = TcpActivity.StartStatement(Options, "SELECT 1", "SELECT", queryId: null))
        {
            activity.SetError(failure);
        }

        Activity span = Single();
        ActivityEvent recorded = span.Events.Single();

        Assert.Multiple(() =>
        {
            Assert.That(recorded.Name, Is.EqualTo("exception"));
            Assert.That(Tag(recorded, "exception.type"), Is.EqualTo(typeof(InvalidOperationException).FullName));
            Assert.That(Tag(recorded, "exception.message"), Is.EqualTo("broken"));
            Assert.That(Tag(recorded, "exception.stacktrace"), Is.Not.Null);
            Assert.That(span.GetTagItem("db.response.status_code"), Is.Null, "only a server error carries one");
        });
    }

    [Test]
    public void StartPing_WithAListener_NamesTheSpanPingAndSetsTheEndpoint()
    {
        TcpActivity.StartPing(Options)?.Dispose();

        Activity span = Single();
        Assert.Multiple(() =>
        {
            Assert.That(span.OperationName, Is.EqualTo("ping"));
            Assert.That(span.GetTagItem("server.address"), Is.EqualTo("example.invalid"));
            Assert.That(span.GetTagItem("db.operation.name"), Is.Null, "a ping runs no statement");
        });
    }

    [Test]
    public void StartStatement_NoListener_ReturnsNull()
    {
        listener.Dispose();

        Assert.That(TcpActivity.StartStatement(Options, "SELECT 1", "SELECT", queryId: null), Is.Null, "nothing listening costs no span");
    }

    private static object Tag(ActivityEvent recorded, string name)
        => recorded.Tags.First(tag => tag.Key == name).Value;

    private Activity Single()
    {
        Assert.That(finished, Has.Count.EqualTo(1), "exactly one span");
        return finished[0];
    }
}
