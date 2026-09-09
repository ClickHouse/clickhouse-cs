using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>One captured log line, with the message already formatted.</summary>
internal sealed class LogEntry
{
    public LogLevel Level { get; init; }

    public EventId EventId { get; init; }

    public string Message { get; init; }

    public Exception Exception { get; init; }

    public override string ToString() => $"{Level} [{EventId.Id}] {Message}";
}

/// <summary>Records everything written to it, for a test to assert against.</summary>
internal sealed class CapturingLogger : ILogger
{
    private readonly List<LogEntry> entries = [];
    private readonly object gate = new();

    /// <summary>The lowest level this logger reports as enabled.</summary>
    public LogLevel MinimumLevel { get; set; } = LogLevel.Trace;

    /// <summary>A snapshot of what has been logged so far.</summary>
    public IReadOnlyList<LogEntry> Entries
    {
        get
        {
            lock (gate)
            {
                return entries.ToArray();
            }
        }
    }

    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull
        => NullScope.Instance;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= MinimumLevel && logLevel != LogLevel.None;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        var entry = new LogEntry
        {
            Level = logLevel,
            EventId = eventId,
            Message = formatter(state, exception),
            Exception = exception,
        };

        lock (gate)
        {
            entries.Add(entry);
        }
    }

    /// <summary>The entries with this event id.</summary>
    /// <param name="eventId">The event id to match.</param>
    /// <returns>The matching entries, in order.</returns>
    public IReadOnlyList<LogEntry> WithEventId(int eventId) => Entries.Where(e => e.EventId.Id == eventId).ToArray();

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
        }
    }
}

/// <summary>
/// Hands out a <see cref="CapturingLogger"/> per category, so a test can assert both what was logged and which
/// category it went to.
/// </summary>
internal sealed class CapturingLoggerFactory : ILoggerFactory
{
    private readonly Dictionary<string, CapturingLogger> loggers = new(StringComparer.Ordinal);
    private readonly object gate = new();
    private LogLevel minimumLevel = LogLevel.Trace;

    /// <summary>The lowest level every logger, existing and future, reports as enabled.</summary>
    public LogLevel MinimumLevel
    {
        get => minimumLevel;
        set
        {
            lock (gate)
            {
                minimumLevel = value;
                foreach (CapturingLogger logger in loggers.Values)
                {
                    logger.MinimumLevel = value;
                }
            }
        }
    }

    /// <summary>The logger for a category, creating it if the client has not asked for it yet.</summary>
    /// <param name="categoryName">The category name.</param>
    /// <returns>The capturing logger for that category.</returns>
    public CapturingLogger Logger(string categoryName)
    {
        lock (gate)
        {
            if (!loggers.TryGetValue(categoryName, out CapturingLogger logger))
            {
                logger = new CapturingLogger { MinimumLevel = minimumLevel };
                loggers.Add(categoryName, logger);
            }

            return logger;
        }
    }

    /// <summary>The categories the client has asked for a logger for.</summary>
    public IReadOnlyCollection<string> Categories
    {
        get
        {
            lock (gate)
            {
                return loggers.Keys.ToArray();
            }
        }
    }

    ILogger ILoggerFactory.CreateLogger(string categoryName) => Logger(categoryName);

    public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();

    public void Dispose()
    {
    }
}

/// <summary>
/// A logger factory whose loggers throw from every member, for asserting that a broken logger cannot break the
/// operation it was reporting on.
/// </summary>
internal sealed class ThrowingLoggerFactory : ILoggerFactory
{
    public ILogger CreateLogger(string categoryName) => new ThrowingLogger();

    public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();

    public void Dispose()
    {
    }

    private sealed class ThrowingLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
            => throw new InvalidOperationException("from BeginScope");

        public bool IsEnabled(LogLevel logLevel) => throw new InvalidOperationException("from IsEnabled");

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            => throw new InvalidOperationException("from Log");
    }
}
