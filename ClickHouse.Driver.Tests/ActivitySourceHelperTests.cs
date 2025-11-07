using System.Diagnostics;
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

        using var activity = connection.StartActivity("TestActivity");
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
}
