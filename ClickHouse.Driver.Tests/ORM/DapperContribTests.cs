using System;
using System.Reflection;
using System.Threading.Tasks;
using ClickHouse.Driver.Utility;
using Dapper.Contrib.Extensions;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ORM;

[TestFixture]
public class DapperContribTests : AbstractConnectionTestFixture
{
    // TODO: Non-UTC timezones
    // TODO: DateTimeTimeOffset
    private readonly static TestRecord referenceRecord = new(1, "value", new DateTime(2023, 4, 15, 1, 2, 3, DateTimeKind.Utc));

    // Dapper.Contrib caches the resolved table name per POCO type for the lifetime of the process,
    // so the randomized name is shared by every test in this fixture rather than created per test.
    private static string tableName;

    public record class TestRecord(int Id, string Value, DateTime Timestamp);

    [OneTimeSetUp]
    public void SetUpTableNameMapper()
    {
        tableName = CreateTableName("dapper_contrib");

        // Dapper.Contrib derives the table name from the POCO type name or its [Table] attribute, both
        // of which are compile-time constants, so its resolution hook is the only way to hand it a name
        // that is unique per run. Other types keep Contrib's own naming rules.
        SqlMapperExtensions.TableNameMapper = type => type == typeof(TestRecord)
            ? tableName
            : type.GetCustomAttribute<TableAttribute>(false)?.Name ?? $"{type.Name}s";
    }

    [OneTimeTearDown]
    public void ResetTableNameMapper()
    {
        // The mapper is process-wide, and the fallback above only approximates Contrib's own naming
        // rules. Clearing it keeps a future Contrib fixture from silently inheriting them. Safe for
        // this fixture's own tests: Contrib has already cached TestRecord's resolved name by now.
        SqlMapperExtensions.TableNameMapper = null;
    }

    [SetUp]
    public async Task SetUp()
    {
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {tableName} (Id Int32, Value String, Timestamp DateTime('UTC')) ENGINE Memory");
        // Tests in this fixture share one table, so its contents are reset before each of them
        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"INSERT INTO {tableName} VALUES (1, 'value', toDateTime('2023/04/15 01:02:03', 'UTC'))");
    }

    [Test]
    public async Task ShouldGetAll() => Assert.That(await connection.GetAllAsync<TestRecord>(), Has.Member(referenceRecord));

    [Test]
    public async Task ShouldGet() => Assert.That(await connection.GetAsync<TestRecord>(1), Is.EqualTo(referenceRecord));

    [Test]
    [Ignore("Dapper.Contrib does not properly support ClickHouse yet")]
    public async Task ShouldInsert() => await connection.InsertAsync(referenceRecord);
}
