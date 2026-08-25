using System;
using System.Diagnostics;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Logging;
using ClickHouse.Driver.Tcp.Protocol;
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
    private bool ended;

    private ClientOperation(Activity activity, ILogger logger, string operationName, string queryId)
    {
        this.activity = activity;
        this.logger = logger;
        this.operationName = operationName;
        this.queryId = queryId;
        startedAt = Stopwatch.GetTimestamp();
    }

    /// <summary>The handlers the operation's response drains into, or null when nothing needs them.</summary>
    public MetadataHandlers Handlers { get; private set; }

    /// <summary>Starts the span and the log line for a statement, and builds its handlers.</summary>
    /// <param name="options">The client options the span's endpoint attributes come from.</param>
    /// <param name="logger">The client-category logger, or null when none is configured.</param>
    /// <param name="sql">The statement.</param>
    /// <param name="queryId">The caller's query id, or null to let the server assign one.</param>
    /// <param name="callbacks">The caller's metadata callbacks, or null.</param>
    /// <returns>The operation, or null when nothing is observing it.</returns>
    public static ClientOperation Start(
        ClickHouseTcpClientOptions options,
        ILogger logger,
        string sql,
        string queryId,
        ClickHouseTcpQueryCallbacks callbacks)
    {
        // Tested before the statement is scanned: reading the leading keyword allocates, and an unconfigured
        // client must not pay for it on every operation.
        if (!TcpActivity.Source.HasListeners() && logger is null && callbacks is null)
        {
            return null;
        }

        string operationName = TcpActivity.OperationName(sql);
        Activity activity = TcpActivity.StartStatement(options, sql, operationName, queryId);
        var operation = new ClientOperation(activity, logger, operationName ?? TcpActivity.UnknownOperation, queryId);

        // The counters feed both the span's attributes and the completion log line, so either one alone is reason
        // enough to accumulate them.
        operation.Handlers = MetadataCallbackBridge.Build(
            callbacks,
            activity is null && logger is null ? null : operation.Accumulate,
            activity is null ? null : activity.SetProfileInfo);

        if (logger is not null)
        {
            // Capped by the same knob as the span attribute, so one setting governs how much statement text
            // leaves the client whichever channel it leaves by.
            ClientLog.StatementStarted(logger, operation.operationName, queryId, options.StatementForTelemetry(sql));
        }

        return operation;
    }

    /// <summary>Records that the operation ran to completion.</summary>
    public void Succeeded()
    {
        ended = true;
        activity?.SetSuccess();

        if (logger is null)
        {
            return;
        }

        // An insert reports what it wrote and a query what it read, so the line carries whichever the server
        // counted rather than a row of zeroes for the other.
        if (totals.WroteRows != 0 || totals.WroteBytes != 0)
        {
            ClientLog.StatementWrote(logger, operationName, queryId, Elapsed(), totals.WroteRows, totals.WroteBytes);
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
        activity?.SetProgressTotals(totals);
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
