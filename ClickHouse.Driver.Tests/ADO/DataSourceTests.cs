#if NET7_0_OR_GREATER
using System.Data;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

public class DataSourceTests
{
    [Test]
    public void CanCreateConnection()
    {
        var connectionString = new ClickHouseConnection("Host=localhost").ConnectionString;
        using var dataSource = new ClickHouseDataSource(connectionString);
        using var connection = dataSource.CreateConnection();
        Assert.That(connectionString, Is.EqualTo(connection.ConnectionString));
    }

    [Test]
    public async Task OpenConnectionAsync_WithoutCancellationToken_ReturnsOpenClickHouseConnection()
    {
        using var dataSource = new ClickHouseDataSource(TestUtilities.GetTestClickHouseClientSettings());
        await using ClickHouseConnection connection = await dataSource.OpenConnectionAsync();
        Assert.That(connection.State, Is.EqualTo(ConnectionState.Open));

        using var command = connection.CreateCommand("SELECT 1");
        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(1));
    }
}
#endif
