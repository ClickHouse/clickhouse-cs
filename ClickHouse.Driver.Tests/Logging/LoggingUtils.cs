using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
namespace ClickHouse.Driver.Tests.Logging;

internal class CapturingLogger : ILogger
    {
        private sealed class NoopDisposable : IDisposable
        {
            public static readonly NoopDisposable Instance = new();

            public void Dispose()
            {
            }
        }

        private readonly List<LogEntry> logs = new();

        /// <summary>
        /// Snapshot of the entries captured so far. The driver logs from every thread it uses, so
        /// callers get a private copy taken under the lock rather than the live collection.
        /// </summary>
        public List<LogEntry> Logs
        {
            get
            {
                lock (logs)
                {
                    return new List<LogEntry>(logs);
                }
            }
        }

        private volatile LogLevel minimumLevel = LogLevel.Trace;

        public LogLevel MinimumLevel
        {
            get => minimumLevel;
            set => minimumLevel = value;
        }

        public string Category { get; set; }

        public IDisposable BeginScope<TState>(TState state) => NoopDisposable.Instance;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= MinimumLevel;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (!IsEnabled(logLevel)) return;

            var entry = new LogEntry
            {
                LogLevel = logLevel,
                EventId = eventId,
                Message = formatter(state, exception),
                Exception = exception
            };

            lock (logs)
            {
                logs.Add(entry);
            }
        }
    }

    internal sealed class LogEntry
    {
        public LogLevel LogLevel { get; set; }
        public EventId EventId { get; set; }
        public string Message { get; set; }
        public Exception Exception { get; set; }
    }

    internal sealed class CapturingLoggerFactory : ILoggerFactory
    {
        private readonly object levelLock = new();
        private LogLevel minimumLevel = LogLevel.Trace;

        public ConcurrentDictionary<string, CapturingLogger> Loggers { get; } = new();

        public LogLevel MinimumLevel
        {
            get
            {
                lock (levelLock)
                {
                    return minimumLevel;
                }
            }
            set
            {
                // Held across the propagation so that a logger created concurrently cannot be
                // added with the previous level after the loop has already passed it by.
                lock (levelLock)
                {
                    minimumLevel = value;
                    foreach (var logger in Loggers.Values)
                    {
                        logger.MinimumLevel = value;
                    }
                }
            }
        }

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName)
        {
            lock (levelLock)
            {
                return Loggers.GetOrAdd(categoryName, name => new CapturingLogger { Category = name, MinimumLevel = minimumLevel });
            }
        }

        public void Dispose()
        {
        }
    }
