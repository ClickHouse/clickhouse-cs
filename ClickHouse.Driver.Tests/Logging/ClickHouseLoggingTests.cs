#if NET7_0_OR_GREATER

using System.Net.Http;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Tests.Logging;

public class ClickHouseLoggingTests
{
    [Test]
    public void DataSource_PropagatesLoggerFactoryToConnecction()
    {
        var factory = new CapturingLoggerFactory();
        using var httpClient = new HttpClient();
        var dataSource = new ClickHouseDataSource("Host=localhost", httpClient, disposeHttpClient: false)
        {
            LoggerFactory = factory,
        };
        
        try
        {
            using var connection = dataSource.CreateConnection();
            Assert.That(connection.LoggerFactory, Is.SameAs(factory));
        }
        finally
        {
            dataSource.Dispose();
        }
    }
}
#endif
