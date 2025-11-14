using System;
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

        public List<LogEntry> Logs { get; } = new();
        
        public LogLevel MinimumLevel { get; set; } = LogLevel.Trace;

        public string Category { get; set; }

        public IDisposable BeginScope<TState>(TState state) => NoopDisposable.Instance;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= MinimumLevel;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (!IsEnabled(logLevel)) return;
            
            Logs.Add(new LogEntry
            {
                LogLevel = logLevel,
                EventId = eventId,
                Message = formatter(state, exception),
                Exception = exception
            });
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
        private LogLevel minimumLevel = LogLevel.Trace;
        public Dictionary<string, CapturingLogger> Loggers { get; } = new();

        public LogLevel MinimumLevel
        {
            get => minimumLevel;
            set
            {
                minimumLevel = value;
                foreach (var logger in Loggers.Values)
                {
                    logger.MinimumLevel = minimumLevel;
                }
            }
        }

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName)
        {
            if (Loggers.TryGetValue(categoryName, out var existingLogger)) 
                return existingLogger;
            
            var logger = new CapturingLogger { Category = categoryName, MinimumLevel = minimumLevel};
            Loggers[categoryName] = logger;
            return logger;
        }

        public void Dispose()
        {
        }
    }
