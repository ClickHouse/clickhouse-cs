#if NET7_0_OR_GREATER
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
}
#endif
