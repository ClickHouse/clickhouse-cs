using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Logging;
using ClickHouse.Driver.Utility;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Logging;

public class ClickHouseBulkCopyLoggingTests
{
    private string targetTable = "test.bulk_copy_logging_table";
    
    [SetUp]
    public async Task SetUp()
    {
        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder());
        var connection = new ClickHouseConnection(settings);

        await connection.ExecuteStatementAsync($"CREATE DATABASE IF NOT EXISTS test;");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (int Int32) ENGINE Null");
    }

    [Test]
    public async Task InitAsync_WithDebugLogging_LogsMetadataLoading()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = LogLevel.Debug;
        
        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };

        using var connection = new ClickHouseConnection(settings);
        var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };

        // Act
        await bulkCopy.InitAsync();

        // Assert
        Assert.That(factory.Loggers, Does.ContainKey(ClickHouseLogCategories.BulkCopy));
        var logger = factory.Loggers[ClickHouseLogCategories.BulkCopy];

        // Should have logged metadata loading start
        var startLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Loading metadata for table") &&
            l.Message.Contains(bulkCopy.DestinationTableName));
        Assert.That(startLog, Is.Not.Null, "Should log metadata loading start at Debug level");

        // Should have logged metadata loaded completion
        var completionLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Metadata loaded for table") &&
            l.Message.Contains(bulkCopy.DestinationTableName));
        Assert.That(completionLog, Is.Not.Null, "Should log metadata loaded completion at Debug level");
    }

    [Test]
    public async Task WriteToServerAsync_WithDebugLogging_LogsBulkCopyOperations()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = LogLevel.Debug;
        
        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };

        using var connection = new ClickHouseConnection(settings);
        var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            BatchSize = 100,
            MaxDegreeOfParallelism = 2
        };

        // Mock column initialization
        await bulkCopy.InitAsync();

        var rows = Enumerable.Range(1, 10).Select(i => new object[] { i }).ToList();

        // Act
        await bulkCopy.WriteToServerAsync(rows);

        // Assert
        var logger = factory.Loggers[ClickHouseLogCategories.BulkCopy];

        // Should have logged bulk copy start
        var startLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Starting bulk copy into") &&
            l.Message.Contains(bulkCopy.DestinationTableName) &&
            l.Message.Contains("batch size") &&
            l.Message.Contains(bulkCopy.BatchSize.ToString()) &&
            l.Message.Contains("degree") &&
            l.Message.Contains(bulkCopy.MaxDegreeOfParallelism.ToString()));
        Assert.That(startLog, Is.Not.Null, "Should log bulk copy start with batch size and degree at Debug level");
    }

    [Test]
    public async Task SendBatchAsync_WithDebugLogging_LogsBatchOperations()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = LogLevel.Debug;
        
        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };

        using var connection = new ClickHouseConnection(settings);
        var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            BatchSize = 5,
        };
        
        await bulkCopy.InitAsync();

        var rows = Enumerable.Range(1, 10).Select(i => new object[] { i }).ToList();
        
        await bulkCopy.WriteToServerAsync(rows);


        // Assert
        var logger = factory.Loggers[ClickHouseLogCategories.BulkCopy];

        // Should have logged batch sending
        var sendingLogs = logger.Logs.FindAll(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Sending batch of") &&
            l.Message.Contains("rows to") &&
            l.Message.Contains(bulkCopy.DestinationTableName));
        Assert.That(sendingLogs.Count, Is.GreaterThan(0), "Should log batch sending operations at Debug level");

        // Should have logged batch sent completion
        var sentLogs = logger.Logs.FindAll(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Batch sent to") &&
            l.Message.Contains(bulkCopy.DestinationTableName) &&
            l.Message.Contains("Total rows written"));
        Assert.That(sentLogs.Count, Is.GreaterThan(0), "Should log batch sent completion at Debug level");
    }

    [Test]
    public async Task WriteToServerAsync_WithCompletedBulkCopy_LogsTotalRows()
    {
        // Arrange
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = LogLevel.Debug;

        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };

        using var connection = new ClickHouseConnection(settings);
        var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };

        // Mock column initialization
        try
        {
            await bulkCopy.InitAsync();
        }
        catch
        {
            // Ignore init errors
        }

        var rows = Enumerable.Range(1, 10).Select(i => new object[] {i}).ToList();

        // Act
        try
        {
            await bulkCopy.WriteToServerAsync(rows);
        }
        catch
        {
            // Ignore errors
        }

        // Assert
        var logger = factory.Loggers[ClickHouseLogCategories.BulkCopy];

        // Should have logged completion with total rows
        var completionLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Bulk copy into") &&
            l.Message.Contains(bulkCopy.DestinationTableName) &&
            l.Message.Contains("completed") &&
            l.Message.Contains("Total rows"));
        Assert.That(completionLog, Is.Not.Null, "Should log bulk copy completion with total rows at Debug level");
    }
}
