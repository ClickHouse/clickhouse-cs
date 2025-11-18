#if NET7_0_OR_GREATER

using System.Net.Http;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.Logging;

public class ClickHouseLoggingTests
{
    [Test]
    public void DataSource_PropagatesLoggerFactoryToConnection()
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
            Assert.That(connection.GetLogger("test"), Is.Not.Null);
        }
        finally
        {
            dataSource.Dispose();
        }
    }
}
#endif
