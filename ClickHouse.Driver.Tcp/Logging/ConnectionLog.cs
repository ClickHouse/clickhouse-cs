using System;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Logging;

/// <summary>
/// The messages written under <see cref="ClickHouseTcpDiagnostics.ConnectionLogCategory"/>. Source-generated, so a
/// disabled level formats nothing.
/// </summary>
internal static partial class ConnectionLog
{
    [LoggerMessage(
        EventId = 2000,
        Level = LogLevel.Debug,
        Message = "Connecting to {Host}:{Port} as {Username} (TLS {Tls})")]
    public static partial void Opening(ILogger logger, string host, int port, string username, bool tls);

    // Both revisions, because they differ whenever the client and the server are not the same age, and only the
    // negotiated one governs which features the connection has.
    [LoggerMessage(
        EventId = 2001,
        Level = LogLevel.Debug,
        Message = "Connected to {ServerName} {VersionMajor}.{VersionMinor}.{VersionPatch} in {ElapsedMs:0.###} ms: protocol revision {NegotiatedRevision} in force (server advertised {ServerRevision}), server timezone {Timezone}")]
    public static partial void Opened(ILogger logger, string serverName, int versionMajor, int versionMinor, int versionPatch, double elapsedMs, int negotiatedRevision, int serverRevision, string timezone);

    [LoggerMessage(
        EventId = 2002,
        Level = LogLevel.Warning,
        Message = "Connecting to {Host}:{Port} failed after {ElapsedMs:0.###} ms")]
    public static partial void OpenFailed(ILogger logger, string host, int port, double elapsedMs, Exception exception);

    [LoggerMessage(
        EventId = 2003,
        Level = LogLevel.Debug,
        Message = "Connecting to {Host}:{Port} was cancelled after {ElapsedMs:0.###} ms")]
    public static partial void OpenCancelled(ILogger logger, string host, int port, double elapsedMs);
}
