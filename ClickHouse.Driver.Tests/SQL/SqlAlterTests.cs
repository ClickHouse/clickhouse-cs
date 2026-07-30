using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

public class SqlAlterTests
{
    private readonly DbConnection connection;

    // Cannot derive from AbstractConnectionTestFixture: these tests need a session-enabled connection.
    private readonly string targetTable = TestUtilities.CreateTableName("table_delete_from");

    public SqlAlterTests()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.UseSession = true;
        builder.Compression = true;
        connection = new ClickHouseConnection(builder.ToString());
    }

    [Test]
    public async Task ShouldExecuteAlterTable()
    {
        await connection.ExecuteScalarAsync($"CREATE TABLE {targetTable} (value Int32) ENGINE MergeTree ORDER BY value");
        await connection.ExecuteScalarAsync($"ALTER TABLE {targetTable} DELETE WHERE 1=1");
    }

    [OneTimeTearDown]
    public void Dispose()
    {
        try
        {
            connection?.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}").GetAwaiter().GetResult();
        }
        catch
        {
            // Best effort: a leftover table must not fail an otherwise passing fixture.
        }

        connection?.Dispose();
    }
}
