using System;
using System.Diagnostics.Tracing;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Diagnostic;
#if NET5_0_OR_GREATER
/// <summary>
/// Helper class for enabling low-level .NET network tracing via EventSource (.NET 5+).
/// This class uses a singleton EventListener to monitor System.Net events including HTTP, Sockets, DNS, and TLS.
/// WARNING: Enabling network tracing can significantly impact performance and generate very large amounts of log data.
/// Only use for debugging purposes - not recommended for production environments.
/// Requires the logger to be configured with Trace-level logging enabled to see output.
/// </summary>
public static class TraceHelper
{
    private static readonly object ListenerLock = new object();
    private static NetEventListener listener;

    /// <summary>
    /// Activates network tracing with the specified logger factory (.NET 5+).
    /// Events are logged at Trace level to the "ClickHouse.Driver.NetTrace" logger.
    /// This method is thread-safe.
    /// WARNING: Network tracing can significantly impact performance.
    /// </summary>
    /// <param name="loggerFactory">The logger factory to use for logging trace events. Must have Trace-level logging enabled.</param>
    public static void Activate(ILoggerFactory loggerFactory)
    {
        if (loggerFactory == null)
        {
            throw new ArgumentNullException(nameof(loggerFactory));
        }

        lock (ListenerLock)
        {
            listener ??= new NetEventListener(loggerFactory);
        }
    }

    /// <summary>
    /// Deactivates network tracing and disposes the event listener.
    /// This method is thread-safe.
    /// </summary>
    public static void Deactivate()
    {
        lock (ListenerLock)
        {
            listener?.Dispose();
            listener = null;
        }
    }

    /// <summary>
    /// EventListener that captures .NET System.Net trace events and forwards them to ILogger.
    /// </summary>
    private sealed class NetEventListener : EventListener
    {
        private readonly ILogger logger;

        // .NET 5+ event source names
        private static readonly string[] EventSourceNames = new[]
        {
            "Private.InternalDiagnostics.System.Net.Sockets",
            "Private.InternalDiagnostics.System.Net.NameResolution",
            "Private.InternalDiagnostics.System.Net.Http",
            "Private.InternalDiagnostics.System.Net.Security",
            "System.Net.Http",
            "System.Net.NameResolution",
            "System.Net.Security",
            "System.Net.Sockets",
        };

        public NetEventListener(ILoggerFactory loggerFactory)
        {
            logger = loggerFactory.CreateLogger("ClickHouse.Driver.NetTrace");
        }

        protected override void OnEventSourceCreated(EventSource eventSource)
        {
            base.OnEventSourceCreated(eventSource);

            // Check if this event source is one we're interested in
            if (EventSourceNames.Any(name => eventSource.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
            {
                EnableEvents(eventSource, EventLevel.LogAlways, EventKeywords.All);
            }
        }

        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            if (!logger.IsEnabled(LogLevel.Trace))
                return;

            try
            {
                var message = FormatEventMessage(eventData);
                logger.LogTrace("[{EventSource}] {EventName}: {Message}",
                    eventData.EventSource?.Name ?? "Unknown",
                    eventData.EventName ?? "Unknown",
                    message);
            }
            catch
            {
                // Swallow exceptions to prevent listener from disrupting normal operation
            }
        }

        private static string FormatEventMessage(EventWrittenEventArgs eventData)
        {
            if (eventData.Payload == null || eventData.Payload.Count == 0)
                return string.Empty;

            return string.Join(", ", eventData.Payload);
        }
    }
}
#endif
