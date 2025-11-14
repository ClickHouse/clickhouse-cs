using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Logging;
using ClickHouse.Driver.Tests.Utilities;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Logging;

public class ClickHouseCommandLoggingTests
{
    [Test]
    public async Task ExecuteReaderAsync_WithDebugLogging_LogsQueryExecutionDetails()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = LogLevel.Debug;
        
        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };

        string queryId = "ko3489hsdf";
        using var connection = new ClickHouseConnection(settings);
        using var command = connection.CreateCommand();
        command.QueryId = queryId;
        command.CommandText = "SELECT 1";
        
        // Act
        using var reader = await command.ExecuteReaderAsync();

        // Assert
        Assert.That(factory.Loggers, Does.ContainKey(ClickHouseLogCategories.Command));
        var logger = factory.Loggers[ClickHouseLogCategories.Command];

        // Should have logged query execution start
        var startLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Executing SQL query") &&
            l.Message.Contains(queryId));
        Assert.That(startLog, Is.Not.Null, "Should log query execution start at Debug level");

        // Should have logged query completion with stats
        var completionLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Query") &&
            l.Message.Contains("succeeded") &&
            l.Message.Contains("Query Stats"));
        Assert.That(completionLog, Is.Not.Null, "Should log query completion with timing and stats at Debug level");
        
        // Check that elapsed time is logged in milliseconds
        Assert.That(completionLog.Message, Does.Match(@"\d+\.\d+\s*ms"), "Should include elapsed time in milliseconds");
    }

    [Test]
    public async Task ExecuteReaderAsync_WithoutDebugLogging_DoesNotLogQueryDetails()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = LogLevel.Information; // Debug is not enabled

        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };

        using var connection = new ClickHouseConnection(settings);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        
        // Act
        using var reader = await command.ExecuteReaderAsync();

        // Assert
        Assert.That(factory.Loggers, Does.ContainKey(ClickHouseLogCategories.Command));
        var logger = factory.Loggers[ClickHouseLogCategories.Command];

        // Should NOT have logged debug-level query details
        var debugLogs = logger.Logs.FindAll(l => l.LogLevel == LogLevel.Debug);
        Assert.That(debugLogs.Count, Is.EqualTo(0), "Should not log Debug level messages when Debug is not enabled");
    }

    [Test]
    public async Task ExecuteNonQueryAsync_WithDebugLogging_LogsQueryExecution()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = LogLevel.Debug;

        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };

        using var connection = new ClickHouseConnection(settings);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";

        // Act
        await command.ExecuteNonQueryAsync();

        // Assert
        var logger = factory.Loggers[ClickHouseLogCategories.Command];

        // Should have logged the query execution
        var startLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Executing SQL query"));
        Assert.That(startLog, Is.Not.Null, "Should log query execution for INSERT statements");
    }
}
