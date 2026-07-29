using System;
using System.Collections.Concurrent;
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
#pragma warning disable CS0618 // Type or member is obsolete

namespace ClickHouse.Driver.Tests.Logging;

public class ClickHouseBulkCopyLoggingTests
{
    private readonly ConcurrentQueue<string> createdTables = new();

    private string targetTable;

    [SetUp]
    public async Task SetUp()
    {
        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder());
        using var connection = new ClickHouseConnection(settings);

        targetTable = TestUtilities.CreateTableName("bulk_copy_logging");
        createdTables.Enqueue(targetTable);

        await connection.ExecuteStatementAsync($"CREATE DATABASE IF NOT EXISTS test;");
        await connection.ExecuteStatementAsync($"CREATE TABLE {targetTable} (int Int32) ENGINE Null");
    }

    /// <summary>
    /// This fixture does not derive from <see cref="AbstractConnectionTestFixture"/>, so the tables
    /// handed out by <see cref="SetUp"/> are dropped here. Best-effort: a table which cannot be
    /// dropped must not fail an otherwise passing fixture.
    /// </summary>
    [OneTimeTearDown]
    public async Task DropCreatedTables()
    {
        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder());
        using var connection = new ClickHouseConnection(settings);

        foreach (var table in createdTables)
        {
            try
            {
                await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
            }
            catch (Exception e)
            {
                TestContext.Progress.WriteLine($"Failed to drop test table {table}: {e.Message}");
            }
        }
    }

    [Test]
    public async Task WriteToServerAsync_WithDebugLogging_LogsMetadataLoading()
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

        var rows = Enumerable.Range(1, 10).Select(i => new object[] { i }).ToList();

        // Act
        await bulkCopy.WriteToServerAsync(rows);

        // Assert
        Assert.That(factory.Loggers, Does.ContainKey(ClickHouseLogCategories.Client));
        var logger = factory.Loggers[ClickHouseLogCategories.Client];

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

        var rows = Enumerable.Range(1, 10).Select(i => new object[] { i }).ToList();

        // Act
        await bulkCopy.WriteToServerAsync(rows);

        // Assert
        var logger = factory.Loggers[ClickHouseLogCategories.Client];

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

        var rows = Enumerable.Range(1, 10).Select(i => new object[] { i }).ToList();
        
        await bulkCopy.WriteToServerAsync(rows);


        // Assert
        var logger = factory.Loggers[ClickHouseLogCategories.Client];

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
            l.Message.Contains(bulkCopy.DestinationTableName));
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
        var logger = factory.Loggers[ClickHouseLogCategories.Client];

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
