using System;
using ClickHouse.Driver.Tcp.Diagnostic;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Tests.Diagnostic;

// The guard around a caller's logger. Its point is what happens at the sites the log calls sit between — a
// connection taken from the idle list but not yet leased, opened but not yet handed back — where an exception
// would leave a socket nobody owns. That consequence is covered where those sites are; this pins the boundary
// itself, which nothing else can observe.
[TestFixture]
public class DiagnosticLoggerTests
{
    private const string Category = "ClickHouse.Driver.Tcp.Tests";

    [Test]
    public void Create_NoFactory_ReturnsNull()
    {
        Assert.That(DiagnosticLogger.Create(null, Category), Is.Null, "no factory means no logger to call");
    }

    [Test]
    public void Log_LoggerThrows_Swallows()
    {
        ILogger logger = DiagnosticLogger.Create(new ThrowingLoggerFactory(), Category);

        Assert.DoesNotThrow(() => logger.LogInformation("anything"));
    }

    [Test]
    public void IsEnabled_LoggerThrows_ReadsAsDisabled()
    {
        // Reported as off rather than on: the source-generated methods check it first, so a false here also keeps
        // them from formatting a message for a logger that cannot take it.
        ILogger logger = DiagnosticLogger.Create(new ThrowingLoggerFactory(), Category);

        Assert.That(logger.IsEnabled(LogLevel.Error), Is.False);
    }

    [Test]
    public void BeginScope_LoggerThrows_ReturnsNull()
    {
        ILogger logger = DiagnosticLogger.Create(new ThrowingLoggerFactory(), Category);

        Assert.That(logger.BeginScope("scope"), Is.Null);
    }

    [Test]
    public void Log_WorkingLogger_ReachesIt()
    {
        var factory = new RecordingLoggerFactory();
        ILogger logger = DiagnosticLogger.Create(factory, Category);

        logger.LogInformation("through the guard");

        Assert.That(factory.Logger.Written, Is.EqualTo(1), "the guard forwards rather than replacing");
    }

    private sealed class RecordingLoggerFactory : ILoggerFactory
    {
        public RecordingLogger Logger { get; } = new();

        public ILogger CreateLogger(string categoryName) => Logger;

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public void Dispose()
        {
        }

        internal sealed class RecordingLogger : ILogger
        {
            public int Written { get; private set; }

            public IDisposable BeginScope<TState>(TState state) => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
                => Written++;
        }
    }
}
