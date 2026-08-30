using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Logging;

public class CapturingLoggerTests
{
    private const int Writers = 8;
    private const int EntriesPerWriter = 2000;

    private static async Task RunConcurrently(int workers, Action<int> body)
    {
        using var start = new Barrier(workers);
        var tasks = Enumerable.Range(0, workers)
            .Select(index => Task.Factory.StartNew(
                () =>
                {
                    start.SignalAndWait();
                    body(index);
                },
                TaskCreationOptions.LongRunning))
            .ToArray();
        await Task.WhenAll(tasks);
    }

    private static void Write(ILogger logger, string message) =>
        logger.Log(LogLevel.Debug, new EventId(1), message, null, (state, _) => state);

    [Test]
    public async Task Log_CalledConcurrently_RecordsEveryEntry()
    {
        var logger = new CapturingLogger();

        await RunConcurrently(Writers, writer =>
        {
            for (var i = 0; i < EntriesPerWriter; i++)
                Write(logger, $"writer {writer} entry {i}");
        });

        var logs = logger.Logs;
        Assert.That(logs.Count, Is.EqualTo(Writers * EntriesPerWriter));
        Assert.That(logs.Select(l => l.Message).Distinct().Count(), Is.EqualTo(Writers * EntriesPerWriter));
    }

    [Test]
    public async Task Logs_ReadWhileLoggingConcurrently_ReturnsUsableSnapshot()
    {
        var logger = new CapturingLogger();

        var writing = RunConcurrently(Writers, writer =>
        {
            for (var i = 0; i < EntriesPerWriter; i++)
                Write(logger, $"writer {writer} entry {i}");
        });

        try
        {
            var previousCount = 0;
            var deadline = DateTime.UtcNow.AddMinutes(1);
            while (!writing.IsCompleted && DateTime.UtcNow < deadline)
            {
                var snapshot = logger.Logs;
                var matched = snapshot.FindAll(l => l.LogLevel == LogLevel.Debug && l.Message.Contains("entry"));
                Assert.That(matched.Count, Is.EqualTo(snapshot.Count), "Every entry a reader observes must be fully constructed");
                Assert.That(snapshot.Count, Is.GreaterThanOrEqualTo(previousCount), "Snapshots must not lose entries");
                previousCount = snapshot.Count;
            }
        }
        finally
        {
            await writing;
        }

        Assert.That(logger.Logs.Count, Is.EqualTo(Writers * EntriesPerWriter));
    }

    [Test]
    public async Task CreateLogger_CalledConcurrentlyForSameCategory_ReturnsSingleInstance()
    {
        var factory = new CapturingLoggerFactory();
        var created = new ILogger[Writers];

        await RunConcurrently(Writers, writer =>
        {
            created[writer] = factory.CreateLogger("category");
            Write(created[writer], $"writer {writer}");
        });

        Assert.That(created.Distinct().Count(), Is.EqualTo(1), "One category must map to one logger instance");
        Assert.That(factory.Loggers["category"].Logs.Count, Is.EqualTo(Writers));
    }

    [Test]
    public async Task MinimumLevel_SetWhileCreatingLoggersConcurrently_AppliesToEveryLogger()
    {
        var factory = new CapturingLoggerFactory();

        await RunConcurrently(Writers, writer =>
        {
            if (writer == 0)
                factory.MinimumLevel = LogLevel.Warning;
            else
                factory.CreateLogger($"category {writer}");
        });

        Assert.That(factory.Loggers.Values.Select(l => l.MinimumLevel), Is.All.EqualTo(LogLevel.Warning));
    }

    [Test]
    public void Logs_AfterSequentialLogging_KeepsEntriesInOrder()
    {
        var logger = new CapturingLogger();

        Write(logger, "first");
        Write(logger, "second");
        Write(logger, "third");

        Assert.That(logger.Logs.Select(l => l.Message), Is.EqualTo(new List<string> { "first", "second", "third" }));
    }
}
