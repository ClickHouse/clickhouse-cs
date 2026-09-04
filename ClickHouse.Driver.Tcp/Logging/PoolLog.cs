using System;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Logging;

/// <summary>
/// The messages written under <see cref="ClickHouseTcpDiagnostics.PoolLogCategory"/>. Source-generated, so a disabled
/// level formats nothing.
/// </summary>
/// <remarks>
/// Two of these report work no caller can see, because the pool deliberately swallows it: a background top-up
/// dial and a sweep both run on a timer with nobody awaiting them. Without a logger configured those failures
/// are invisible.
/// </remarks>
internal static partial class PoolLog
{
    [LoggerMessage(
        EventId = 3000,
        Level = LogLevel.Trace,
        Message = "Reusing a pooled connection, its {UsageCount} operation, open for {AgeMs:0} ms")]
    public static partial void Reused(ILogger logger, int usageCount, double ageMs);

    [LoggerMessage(
        EventId = 3001,
        Level = LogLevel.Debug,
        Message = "No idle connection to reuse; opening one")]
    public static partial void Dialing(ILogger logger);

    [LoggerMessage(
        EventId = 3002,
        Level = LogLevel.Debug,
        Message = "Closing a returned connection rather than pooling it: it is no longer reusable after {UsageCount} operations")]
    public static partial void Discarded(ILogger logger, int usageCount);

    [LoggerMessage(
        EventId = 3003,
        Level = LogLevel.Debug,
        Message = "Retired {Count} idle connections past MaxConnectionLifetime or IdleTimeout")]
    public static partial void Retired(ILogger logger, int count);

    [LoggerMessage(
        EventId = 3004,
        Level = LogLevel.Warning,
        Message = "All {MaxPoolSize} connections were in use and none became free within {TimeoutSeconds:0.###}s (PoolTimeout)")]
    public static partial void Exhausted(ILogger logger, int maxPoolSize, double timeoutSeconds);

    [LoggerMessage(
        EventId = 3005,
        Level = LogLevel.Warning,
        Message = "A background top-up towards MinPoolSize failed; the next sweep tries again")]
    public static partial void RefillFailed(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 3006,
        Level = LogLevel.Warning,
        Message = "A pool sweep failed; the next one tries again")]
    public static partial void SweepFailed(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 3007,
        Level = LogLevel.Debug,
        Message = "Draining the pool: closing {IdleCount} idle connections and waiting for {LeasedCount} in flight")]
    public static partial void Draining(ILogger logger, int idleCount, int leasedCount);
}
