using System;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.Dapper;
using ClickHouse.Driver.Utility;
using Dapper;
using Dapper.Contrib.Extensions;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ORM;

[TestFixture]
public class DapperContribTests : AbstractConnectionTestFixture
{
    private static readonly TestRecord ReferenceRecord = new(1, "value", new DateTime(2023, 4, 15, 1, 2, 3, DateTimeKind.Utc));

    [Table("test.dapper_contrib")]
    public record class TestRecord([property: ExplicitKey] int Id, string Value, DateTime Timestamp);

    [Table("test.dapper_contrib_dto")]
    public record class TestRecordWithOffset([property: ExplicitKey] int Id, string Value, DateTimeOffset Timestamp);

    static DapperContribTests()
    {
        ClickHouseDapper.Register();
    }

    [SetUp]
    public async Task SetUp()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_contrib");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_contrib (Id Int32, Value String, Timestamp DateTime('UTC')) ENGINE Memory");
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_contrib VALUES (1, 'value', toDateTime('2023/04/15 01:02:03', 'UTC'))");
    }

    [Test]
    public async Task ShouldGetAll() => Assert.That(await connection.GetAllAsync<TestRecord>(), Has.Member(ReferenceRecord));

    [Test]
    public async Task ShouldGet() => Assert.That(await connection.GetAsync<TestRecord>(1), Is.EqualTo(ReferenceRecord));

    [Test]
    public async Task ShouldInsertWithContribAdapter()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE test.dapper_contrib");
        var record = new TestRecord(42, "inserted", new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc));

        var key = await connection.InsertAsync(record);
        Assert.That(key, Is.EqualTo(0));

        var fetched = await connection.GetAsync<TestRecord>(42);
        Assert.That(fetched, Is.EqualTo(record));
    }

    [Test]
    public async Task ShouldRoundTripNonUtcTimezone()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_contrib_tz");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_contrib_tz (Id Int32, Value String, Timestamp DateTime('Europe/London')) ENGINE Memory");

        // 13:00 Europe/London during BST is 12:00 UTC.
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_contrib_tz VALUES (1, 'tz', toDateTime('2023-07-15 13:00:00', 'Europe/London'))");

        var rows = (await connection.QueryAsync<TestRecord>("SELECT Id, Value, Timestamp FROM test.dapper_contrib_tz")).ToList();
        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Timestamp.ToUniversalTime(), Is.EqualTo(new DateTime(2023, 7, 15, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public async Task ShouldRoundTripDateTimeOffset()
    {
        await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dapper_contrib_dto");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dapper_contrib_dto (Id Int32, Value String, Timestamp DateTime('UTC')) ENGINE Memory");
        await connection.ExecuteStatementAsync("INSERT INTO test.dapper_contrib_dto VALUES (1, 'dto', toDateTime('2023-04-15 01:02:03', 'UTC'))");

        var fetched = await connection.GetAsync<TestRecordWithOffset>(1);
        Assert.That(fetched.Timestamp.UtcDateTime, Is.EqualTo(new DateTime(2023, 4, 15, 1, 2, 3, DateTimeKind.Utc)));
    }
}
