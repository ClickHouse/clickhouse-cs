using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Logging;
using ClickHouse.Driver.Tests.Utilities;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Logging;

public class ClickHouseConnectionLoggingTests
{
    [Test]
    public async Task PostStreamAsync_WithError_LogsTransportErrorWithException()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = LogLevel.Error;
        var client = MockHttpClientHelper.Create(new HttpRequestException("test"));

        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
            HttpClient = client,
        };

        using var connection = new ClickHouseConnection(settings);

        // Act
        try
        {
            await connection.PostStreamAsync(
                "INSERT INTO test VALUES",
                new MemoryStream(new byte[] { 1, 2, 3 }),
                false,
                CancellationToken.None);
        }
        catch
        {
            // Expected to fail
        }

        // Assert
        Assert.That(factory.Loggers, Does.ContainKey(ClickHouseLogCategories.Transport));
        var logger = factory.Loggers[ClickHouseLogCategories.Transport];

        var errorLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Error &&
            l.Message.Contains("Streamed request") &&
            l.Message.Contains("failed"));

        Assert.That(errorLog, Is.Not.Null, "Should log stream request failure at Error level");
        Assert.That(errorLog.Exception, Is.Not.Null, "Error log should include exception");
        Assert.That(errorLog.Exception, Is.InstanceOf<HttpRequestException>());
    }

    [Test]
    public async Task PostStreamAsync_WithSuccess_LogsDebugMessages()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = LogLevel.Debug;

        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };

        using var connection = new ClickHouseConnection(settings);

        // Act
        try
        {
            await connection.PostStreamAsync(
                "INSERT INTO test VALUES",
                new MemoryStream(new byte[] { 1, 2, 3 }),
                false,
                CancellationToken.None);
        }
        catch
        {
            // Might fail but we're testing logging
        }

        // Assert
        Assert.That(factory.Loggers, Does.ContainKey(ClickHouseLogCategories.Transport));
        var logger = factory.Loggers[ClickHouseLogCategories.Transport];

        // Should have logged the sending message
        var sendingLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Sending streamed request") &&
            l.Message.Contains("Compressed"));
        Assert.That(sendingLog, Is.Not.Null, "Should log stream request start at Debug level");

        // Should have logged the response message
        var responseLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Streamed request") &&
            l.Message.Contains("received response"));
        Assert.That(responseLog, Is.Not.Null, "Should log stream response at Debug level");
    }
}
