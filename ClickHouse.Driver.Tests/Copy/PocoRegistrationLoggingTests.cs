using System;
using System.Linq;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Logging;
using ClickHouse.Driver.Tests.Logging;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Copy;

[TestFixture]
public class PocoRegistrationLoggingTests
{
    // Mix of one mappable property and three that are silently skipped on the read side, each for
    // a different reason — the failure mode the debug logging is meant to make diagnosable.
    private class MixedPoco
    {
        public int Id { get; set; }            // read + insert

        // Custom column name + explicit ClickHouse type; maps on both sides under the alias.
        [ClickHouseColumn(Name = "event_ts", Type = "DateTime64(3)")]
        public DateTime Timestamp { get; set; }

        public string Name { get; init; }      // skipped on read (init-only); insert-mapped

        public int Computed { get; }            // skipped on read (no setter); insert-mapped

        [ClickHouseNotMapped]
        public int Ignored { get; set; }        // skipped on both sides
    }

    private static ClickHouseClient CreateClient(CapturingLoggerFactory factory) =>
        new(new ClickHouseClientSettings { LoggerFactory = factory });

    private static LogEntry SingleRegistrationLog(CapturingLoggerFactory factory, string kind)
    {
        Assert.That(factory.Loggers, Does.ContainKey(ClickHouseLogCategories.Client));
        var logs = factory.Loggers[ClickHouseLogCategories.Client].Logs
            .Where(l => l.LogLevel == LogLevel.Debug && l.Message.Contains($"for {kind}:"))
            .ToList();
        Assert.That(logs, Has.Count.EqualTo(1), $"expected exactly one '{kind}' registration log");
        return logs[0];
    }

    [Test]
    public void RegisterPocoType_WithDebugLogging_LogsMappedAndSkippedReadColumns()
    {
        var factory = new CapturingLoggerFactory();
        using var client = CreateClient(factory);

        client.RegisterPocoType<MixedPoco>();

        var read = SingleRegistrationLog(factory, "read").Message;
        Assert.Multiple(() =>
        {
            Assert.That(read, Does.Contain("MixedPoco"));
            Assert.That(read, Does.Contain("Id->Id"));
            // Custom [ClickHouseColumn(Name = ...)] alias is reflected in the mapping log.
            Assert.That(read, Does.Contain("Timestamp->event_ts"));
            // Each skipped property is named with its reason.
            Assert.That(read, Does.Contain("Name (init-only setter)"));
            Assert.That(read, Does.Contain("Computed (no setter)"));
            Assert.That(read, Does.Contain("Ignored ([ClickHouseNotMapped])"));
        });
    }

    [Test]
    public void RegisterPocoType_WithDebugLogging_LogsMappedAndSkippedInsertColumns()
    {
        var factory = new CapturingLoggerFactory();
        using var client = CreateClient(factory);

        client.RegisterPocoType<MixedPoco>();

        var insert = SingleRegistrationLog(factory, "insert").Message;
        Assert.Multiple(() =>
        {
            Assert.That(insert, Does.Contain("MixedPoco"));
            // All four readable properties are insert-mapped; only the NotMapped one is skipped.
            Assert.That(insert, Does.Contain("Id->Id"));
            Assert.That(insert, Does.Contain("Timestamp->event_ts"));
            Assert.That(insert, Does.Contain("Name->Name"));
            Assert.That(insert, Does.Contain("Computed->Computed"));
            Assert.That(insert, Does.Contain("Ignored ([ClickHouseNotMapped])"));
        });
    }

    [Test]
    public void RegisterBinaryInsertType_WithDebugLogging_LogsInsertRegistrationOnly()
    {
        var factory = new CapturingLoggerFactory();
        using var client = CreateClient(factory);

        client.RegisterBinaryInsertType<MixedPoco>();

        // Insert-only registration must not emit a read-side log.
        var clientLogs = factory.Loggers[ClickHouseLogCategories.Client].Logs;
        Assert.Multiple(() =>
        {
            Assert.That(clientLogs.Any(l => l.Message.Contains("for insert:")), Is.True);
            Assert.That(clientLogs.Any(l => l.Message.Contains("for read:")), Is.False);
        });
    }

    [Test]
    public void RegisterPocoType_BelowDebugLevel_DoesNotLog()
    {
        var factory = new CapturingLoggerFactory { MinimumLevel = LogLevel.Information };
        using var client = CreateClient(factory);

        client.RegisterPocoType<MixedPoco>();

        // No Debug entry is captured when the logger filters out Debug.
        if (factory.Loggers.TryGetValue(ClickHouseLogCategories.Client, out var logger))
            Assert.That(logger.Logs, Is.Empty);
    }

    [Test]
    public void RegisterPocoType_CalledTwice_LogsOnce()
    {
        var factory = new CapturingLoggerFactory();
        using var client = CreateClient(factory);

        client.RegisterPocoType<MixedPoco>();
        client.RegisterPocoType<MixedPoco>();

        // The second call is a no-op once both mappings are present, so it must not re-log.
        Assert.That(SingleRegistrationLog(factory, "read"), Is.Not.Null);
        Assert.That(SingleRegistrationLog(factory, "insert"), Is.Not.Null);
    }

    [Test]
    public void RegisterPocoType_WithoutLoggerFactory_DoesNotThrow()
    {
        using var client = new ClickHouseClient(new ClickHouseClientSettings());
        Assert.DoesNotThrow(() => client.RegisterPocoType<MixedPoco>());
    }
}
