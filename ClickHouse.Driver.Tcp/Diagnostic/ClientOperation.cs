using System;
using System.Diagnostics;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Logging;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Diagnostic;

/// <summary>
/// Brackets one client operation with everything that watches it: its span, its log lines, and the metadata
/// handlers both are fed from. They are built together because the span's counters and the completion log line
/// come from the same Progress and ProfileInfo packets the caller's own callbacks do.
/// </summary>
/// <remarks>
/// <see cref="Start"/> returns null when there is nothing to observe — no tracing listener, no logger factory,
/// and no caller callbacks — so every member is reached through <c>operation?.</c> and the
/// no-configuration path allocates nothing.
/// </remarks>
internal sealed class ClientOperation : IDisposable
{
    private readonly Activity activity;
    private readonly ILogger logger;
    private readonly string operationName;
    private readonly string queryId;
    private readonly long startedAt;
    private ClickHouseTcpProgress totals;
    private ulong? rowsSent;
    private bool ended;

    private ClientOperation(Activity activity, ILogger logger, string operationName, string queryId)
    {
        this.activity = activity;
        this.logger = logger;
        this.operationName = operationName;
        this.queryId = queryId;
        startedAt = Stopwatch.GetTimestamp();
    }

    /// <summary>The client's own metadata observers, or null when nothing needs them.</summary>
    public ClickHouseTcpQueryCallbacks Telemetry { get; private set; }

    /// <summary>Starts the span and the log line for a statement, and builds its handlers.</summary>
    /// <param name="options">The client options the span's endpoint attributes come from.</param>
    /// <param name="logger">The client-category logger, or null when none is configured.</param>
    /// <param name="sql">The statement.</param>
    /// <param name="queryId">The query id in force, which every line this operation logs carries.</param>
    /// <returns>The operation, or null when nothing is observing it.</returns>
    /// <remarks>
    /// The caller's own callbacks are not this type's concern: the connection invokes them alongside
    /// <see cref="Telemetry"/> rather than through it, so a caller who wants progress and no tracing starts no
    /// operation at all.
    /// </remarks>
    public static ClientOperation Start(
        ClickHouseTcpClientOptions options,
        ILogger logger,
        string sql,
        string queryId)
    {
        // Tested before the statement is scanned: reading the leading keyword allocates, and an unconfigured
        // client must not pay for it on every operation.
        if (!TcpActivity.Source.HasListeners() && logger is null)
        {
            return null;
        }

        string operationName = TcpActivity.OperationName(sql);
        Activity activity = TcpActivity.StartStatement(options, sql, operationName, queryId);
        var operation = new ClientOperation(activity, logger, operationName ?? TcpActivity.UnknownOperation, queryId);

        // The counters feed both the span's attributes and the completion log line, so either one alone is reason
        // enough to accumulate them. These run before the caller's callbacks, so a caller callback that throws
        // cannot rob the client of telemetry it already had.
        operation.Telemetry = new ClickHouseTcpQueryCallbacks
        {
            OnProgress = operation.Accumulate,
            OnProfileInfo = activity is null ? null : activity.SetProfileInfo,
        };

        if (logger is not null)
        {
            // Capped by the same knob as the span attribute, so one setting governs how much statement text
            // leaves the client whichever channel it leaves by.
            ClientLog.StatementStarted(logger, operation.operationName, queryId, options.StatementForTelemetry(sql));
        }

        return operation;
    }

    /// <summary>Records that the operation ran to completion.</summary>
    /// <param name="rowsSent">
    /// The rows the client sent, for an insert; null for a statement that only reads. The server sends no Progress
    /// packet for rows a client streams to it, so this is the only count an insert has.
    /// </param>
    public void Succeeded(ulong? rowsSent = null)
    {
        ended = true;
        this.rowsSent = rowsSent;
        activity?.SetSuccess();

        if (logger is null)
        {
            return;
        }

        // An insert reports the rows it wrote and a query the rows it read. Two messages rather than one, so
        // neither has to carry a field of zeroes for the other.
        if (rowsSent.HasValue || totals.WroteRows != 0)
        {
            ClientLog.StatementWrote(logger, operationName, queryId, Elapsed(), rowsSent ?? totals.WroteRows);
        }
        else
        {
            ClientLog.StatementCompleted(logger, operationName, queryId, Elapsed(), totals.Rows, totals.Bytes);
        }
    }

    /// <summary>Records the exception that ended the operation.</summary>
    /// <param name="exception">The exception about to propagate.</param>
    public void Failed(Exception exception)
    {
        ended = true;
        activity?.SetError(exception);

        if (logger is null)
        {
            return;
        }

        // A caller cancelling is normal control flow, so it is not an error in the log — though it still ends the
        // span as one, OpenTelemetry having no cancelled status.
        if (exception is OperationCanceledException)
        {
            ClientLog.StatementCancelled(logger, operationName, queryId, Elapsed());
        }
        else
        {
            ClientLog.StatementFailed(logger, operationName, queryId, Elapsed(), exception);
        }
    }

    /// <summary>Writes the accumulated counters and ends the span.</summary>
    public void Dispose()
    {
        activity?.SetCounters(totals, rowsSent);
        activity?.Dispose();

        // Neither succeeded nor failed means the caller stopped reading a result part-way. Worth a line, because
        // it is also what leaves the connection to be discarded and redialed rather than reused.
        if (!ended && logger is not null)
        {
            ClientLog.StatementAbandoned(logger, operationName, queryId, Elapsed());
        }
    }

    private double Elapsed() => Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds;

    // Progress packets carry increments, so the totals are their sum, not the last one seen.
    private void Accumulate(ClickHouseTcpProgress increment) => totals += increment;
}
