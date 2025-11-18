#if NET7_0_OR_GREATER

using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;

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

    [Test]
    public async Task Connection_WithEnableDebugMode_ActivatesTraceHelper()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
        
        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            LoggerFactory = factory,
            EnableDebugMode = true,
        };

        // Act
        using var connection = new ClickHouseConnection(settings);
        
        await connection.OpenAsync();

        // Assert - TraceHelper should create a logger for NetTrace when activated
        Assert.That(factory.Loggers, Does.ContainKey("ClickHouse.Driver.NetTrace"),
            "EnableDebugMode with LoggerFactory should activate TraceHelper and create NetTrace logger");
    }

    [Test]
    public async Task Connection_WithoutEnableDebugMode_DoesNotActivateTraceHelper()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        
        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            LoggerFactory = factory,
            EnableDebugMode = false,
        };

        // Act
        using var connection = new ClickHouseConnection(settings);
        
        await connection.OpenAsync();

        // Assert - TraceHelper should not create a NetTrace logger when not enabled
        Assert.That(factory.Loggers, Does.Not.ContainKey("ClickHouse.Driver.NetTrace"),
            "Without EnableDebugMode, TraceHelper should not be activated");
    }
}
#endif
