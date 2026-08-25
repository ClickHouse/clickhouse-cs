using System;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Logging;

/// <summary>
/// The messages written under <see cref="ClickHouseTcpDiagnostics.ClientLogCategory"/>. Source-generated, so a
/// disabled level formats nothing.
/// </summary>
/// <remarks>
/// Every line carries <c>QueryId</c>, the key that joins it to the server's own record of the same query in
/// <c>system.query_log</c>. It is null unless the caller set
/// <see cref="ClickHouseTcpQueryOptions.QueryId"/>, a server-assigned id never being sent back.
/// </remarks>
internal static partial class ClientLog
{
    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Debug,
        Message = "Running {Operation} (query id {QueryId}): {Sql}")]
    public static partial void StatementStarted(ILogger logger, string operation, string queryId, string sql);

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Debug,
        Message = "{Operation} (query id {QueryId}) completed in {ElapsedMs:0.###} ms, reading {ReadRows} rows / {ReadBytes} bytes")]
    public static partial void StatementCompleted(ILogger logger, string operation, string queryId, double elapsedMs, ulong readRows, ulong readBytes);

    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Error,
        Message = "{Operation} (query id {QueryId}) failed after {ElapsedMs:0.###} ms")]
    public static partial void StatementFailed(ILogger logger, string operation, string queryId, double elapsedMs, Exception exception);

    [LoggerMessage(
        EventId = 1003,
        Level = LogLevel.Debug,
        Message = "{Operation} (query id {QueryId}) was abandoned after {ElapsedMs:0.###} ms with its result partly read")]
    public static partial void StatementAbandoned(ILogger logger, string operation, string queryId, double elapsedMs);

    [LoggerMessage(
        EventId = 1004,
        Level = LogLevel.Debug,
        Message = "{Operation} (query id {QueryId}) completed in {ElapsedMs:0.###} ms, writing {WrittenRows} rows / {WrittenBytes} bytes")]
    public static partial void StatementWrote(ILogger logger, string operation, string queryId, double elapsedMs, ulong writtenRows, ulong writtenBytes);

    [LoggerMessage(
        EventId = 1005,
        Level = LogLevel.Debug,
        Message = "{Operation} (query id {QueryId}) was cancelled after {ElapsedMs:0.###} ms")]
    public static partial void StatementCancelled(ILogger logger, string operation, string queryId, double elapsedMs);
}
