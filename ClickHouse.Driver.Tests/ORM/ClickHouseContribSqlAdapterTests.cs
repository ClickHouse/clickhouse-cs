using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.Dapper;
using ClickHouse.Driver.Utility;
using Dapper.Contrib.Extensions;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ORM;

[TestFixture]
public class ClickHouseContribSqlAdapterTests : AbstractConnectionTestFixture
{
    static ClickHouseContribSqlAdapterTests()
    {
        ClickHouseDapper.Register();
    }

    [Table("test.contrib_adapter_sync")]
    public record class SyncRecord([property: ExplicitKey] int Id, string Value);

    [Test]
    public void AppendColumnName_QuotesWithBackticks()
    {
        var adapter = new ClickHouseContribSqlAdapter();
        var sb = new StringBuilder();

        adapter.AppendColumnName(sb, "my_column");

        Assert.That(sb.ToString(), Is.EqualTo("`my_column`"));
    }

    [Test]
    public void AppendColumnNameEqualsValue_QuotesAndBinds()
    {
        var adapter = new ClickHouseContribSqlAdapter();
        var sb = new StringBuilder();

        adapter.AppendColumnNameEqualsValue(sb, "my_column");

        Assert.That(sb.ToString(), Is.EqualTo("`my_column` = @my_column"));
    }

    [Test]
    public async Task SyncInsert_WritesRowAndReturnsZero()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.contrib_adapter_sync");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.contrib_adapter_sync (Id Int32, Value String) ENGINE Memory");

        var record = new SyncRecord(7, "sync");
        var key = connection.Insert(record);

        Assert.That(key, Is.EqualTo(0));
        Assert.That(await connection.GetAsync<SyncRecord>(7), Is.EqualTo(record));
    }
}
