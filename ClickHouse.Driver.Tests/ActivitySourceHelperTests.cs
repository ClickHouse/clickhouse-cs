using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.Diagnostic;

namespace ClickHouse.Driver.Tests;

[TestFixture]
public class ActivitySourceHelperTests : AbstractConnectionTestFixture
{
    private bool originalIncludeSql;
    private int originalStatementLength;

    [SetUp]
    public void SetUpDiagnosticsOptions()
    {
        originalIncludeSql = ClickHouseDiagnosticsOptions.IncludeSqlInActivityTags;
        originalStatementLength = ClickHouseDiagnosticsOptions.StatementMaxLength;
    }

    [TearDown]
    public void RestoreDiagnosticsOptions()
    {
        ClickHouseDiagnosticsOptions.IncludeSqlInActivityTags = originalIncludeSql;
        ClickHouseDiagnosticsOptions.StatementMaxLength = originalStatementLength;
    }

    [Test]
    public void ShouldCreateActivity()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => true,
            SampleUsingParentId = (ref ActivityCreationOptions<string> activityOptions) => ActivitySamplingResult.AllData,
            Sample = (ref ActivityCreationOptions<ActivityContext> activityOptions) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(listener);

        using var activity = client.StartActivity("TestActivity");
        ClassicAssert.NotNull(activity);
    }

    [Test]
    public void SetQuery_DoesNotAttachTag_WhenDisabled()
    {
        ClickHouseDiagnosticsOptions.IncludeSqlInActivityTags = false;

        using var activity = new Activity("test");
        activity.Start();
        activity.SetQuery("SELECT 1");

        ClassicAssert.IsNull(activity.GetTagItem("db.statement"));
        activity.Stop();
    }

    [Test]
    public void SetQuery_AttachesTruncatedSql_WhenEnabled()
    {
        ClickHouseDiagnosticsOptions.IncludeSqlInActivityTags = true;
        ClickHouseDiagnosticsOptions.StatementMaxLength = 5;

        using var activity = new Activity("test");
        activity.Start();
        activity.SetQuery("SELECT 12345");

        var tagValue = activity.GetTagItem("db.statement") as string;
        ClassicAssert.IsNotNull(tagValue);
        ClassicAssert.That(tagValue, Is.EqualTo("SELEC"));
        activity.Stop();
    }

    [TestCase("", null, "")]
    [TestCase("system", null, "system")]
    [TestCase("", "system", "system")]
    [TestCase("system", "default", "default")]
    [TestCase("system", "", "")]
    public async Task ExecuteScalarAsync_ConfiguredAndQueryOptionsDatabase_SetsDbNameToDatabaseSentWithRequest(
        string configuredDatabase, string queryDatabase, string expectedDbName)
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings.Database = configuredDatabase;
        using var queryClient = new ClickHouseClient(settings);

        var spans = new List<Activity>();
        using var listener = ListenToDriverSpans(spans);

        await queryClient.ExecuteScalarAsync("SELECT 1", options: new QueryOptions { Database = queryDatabase });

        Assert.That(spans.Single().GetTagItem("db.name"), Is.EqualTo(expectedDbName));
    }

    [Test]
    public async Task InsertBinaryAsync_WithInsertOptionsDatabase_SetsDbNameToInsertDatabaseOnEverySpan()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (x UInt32) ENGINE = Memory");
        var bareTable = TestUtilities.BareTableName(table);

        var spans = new List<Activity>();
        using var listener = ListenToDriverSpans(spans);

        await client.InsertBinaryAsync(
            bareTable,
            ["x"],
            [[1u]],
            new InsertOptions { Database = TestUtilities.TestDatabase });

        Assert.That(
            spans.Select(s => (s.OperationName, s.GetTagItem("db.name"))),
            Is.EqualTo(new[]
            {
                ("PostSqlQueryAsync", (object)TestUtilities.TestDatabase),
                ("PostStreamAsync", TestUtilities.TestDatabase),
            }));
    }

    [Test]
    public async Task ExecuteScalarAsync_AfterChangeDatabase_SetsDbNameToSelectedDatabase()
    {
        using var conn = client.CreateConnection();
        await conn.OpenAsync();
        conn.ChangeDatabase("system");
        using var command = conn.CreateCommand("SELECT currentDatabase()");

        var spans = new List<Activity>();
        using var listener = ListenToDriverSpans(spans);

        var serverDatabase = await command.ExecuteScalarAsync();

        Assert.That(serverDatabase, Is.EqualTo("system"));
        Assert.That(spans.Single().GetTagItem("db.name"), Is.EqualTo("system"));
    }

    private static ActivityListener ListenToDriverSpans(List<Activity> spans)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == ClickHouseDiagnosticsOptions.ActivitySourceName,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                lock (spans)
                    spans.Add(a);
            },
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
