using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

public class ConnectionGetClientTests : AbstractConnectionTestFixture
{
    [Test]
    public void GetClient_ConnectionFromClient_ReturnsThatClient()
    {
        Assert.That(connection.GetClient(), Is.SameAs(client));
    }

    [Test]
    public void GetClient_Clone_ReturnsSameClient()
    {
        using var clone = (ClickHouseConnection)connection.Clone();
        Assert.That(clone.GetClient(), Is.SameAs(connection.GetClient()));
    }

#if NET7_0_OR_GREATER
    [Test]
    public void GetClient_ConnectionFromDataSource_ReturnsDataSourceClient()
    {
        using var dataSource = new ClickHouseDataSource(TestUtilities.GetTestClickHouseClientSettings());
        using var dataSourceConnection = dataSource.CreateConnection();
        Assert.That(dataSourceConnection.GetClient(), Is.SameAs(dataSource.GetClient()));
    }
#endif

    [Test]
    public void GetClient_AfterSettingsChange_ReturnsNewClient()
    {
        using var settingsConnection = new ClickHouseConnection(TestUtilities.GetTestClickHouseClientSettings());
        var before = settingsConnection.GetClient();

        settingsConnection.Settings = new ClickHouseClientSettings(settingsConnection.Settings);

        Assert.That(settingsConnection.GetClient(), Is.Not.SameAs(before));
    }

    [Test]
    public void GetClient_ChangeDatabase_DoesNotChangeClientDatabase()
    {
        using var clone = (ClickHouseConnection)connection.Clone();
        var settingsDatabase = clone.GetClient().Settings.Database;

        clone.ChangeDatabase("other_database");

        Assert.That(clone.Database, Is.EqualTo("other_database"));
        Assert.That(clone.GetClient().Settings.Database, Is.EqualTo(settingsDatabase));
    }

    [Test]
    public async Task GetClient_InsertBinaryAsync_WritesRowsThatConnectionReads()
    {
        var table = CreateTableName();
        using (var create = connection.CreateCommand($"CREATE TABLE {table} (id UInt32) ENGINE = Memory"))
            await create.ExecuteNonQueryAsync();

        var rows = Enumerable.Range(1, 5).Select(i => new object[] { (uint)i });
        var inserted = await connection.GetClient().InsertBinaryAsync(table, new[] { "id" }, rows);

        using var count = connection.CreateCommand($"SELECT sum(id) FROM {table}");
        Assert.That(inserted, Is.EqualTo(5));
        Assert.That(await count.ExecuteScalarAsync(), Is.EqualTo(15UL));
    }
}
