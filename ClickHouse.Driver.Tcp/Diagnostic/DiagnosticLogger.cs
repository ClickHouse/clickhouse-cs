using System;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Diagnostic;

/// <summary>
/// Builds the client's loggers, wrapped so that a logger which throws cannot break the operation it was
/// reporting on.
/// </summary>
/// <remarks>
/// <para>
/// The guard matters because of where the log calls sit: between the steps that hand a connection from one owner
/// to the next — taken from the idle list but not yet leased, opened but not yet returned to the caller. An
/// exception out of any of them would leave a socket that no owner is left to close, so a broken logger would
/// surface as the client leaking connections.
/// </para>
/// <para>
/// A caller callback is deliberately not treated this way and still propagates: that is the caller's own code
/// acting on their own data, where failing the operation is a defensible answer. A logger is infrastructure, and
/// losing a log line is always better than losing the query.
/// </para>
/// </remarks>
internal static class DiagnosticLogger
{
    /// <summary>The logger for one category, guarded, or null when no factory is configured.</summary>
    /// <param name="factory">The caller's logger factory, or null to log nothing.</param>
    /// <param name="category">The category to log under.</param>
    /// <returns>The logger to pass to the log methods, or null.</returns>
    public static ILogger Create(ILoggerFactory factory, string category)
    {
        // Not guarded: a factory that cannot build a logger fails at client construction, where the caller sees it.
        ILogger inner = factory?.CreateLogger(category);
        return inner is null ? null : new NoThrowLogger(inner);
    }

    private sealed class NoThrowLogger : ILogger
    {
        private readonly ILogger inner;

        public NoThrowLogger(ILogger inner) => this.inner = inner;

        public IDisposable BeginScope<TState>(TState state)
        {
            try
            {
                return inner.BeginScope(state);
            }
            catch (Exception)
            {
                return null;
            }
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            try
            {
                return inner.IsEnabled(logLevel);
            }
            catch (Exception)
            {
                // Read as "this level is off", so nothing is formatted for it either.
                return false;
            }
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            try
            {
                inner.Log(logLevel, eventId, state, exception, formatter);
            }
            catch (Exception)
            {
                // There is nowhere to report a logger that cannot log.
            }
        }
    }
}
