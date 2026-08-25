using System;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Diagnostic;

/// <summary>
/// Starts and annotates the client spans. Every method tolerates a null <see cref="Activity"/>, because
/// <see cref="ActivitySource.StartActivity(string, ActivityKind)"/> returns null when nothing is listening — so a
/// caller writes <c>activity?.SetSuccess()</c> and never branches on whether tracing is on.
/// </summary>
/// <remarks>
/// The attribute names are the current OpenTelemetry database conventions
/// (<c>db.system.name</c>, <c>db.namespace</c>, <c>db.query.text</c>, <c>server.address</c>), not the older
/// <c>db.system</c>/<c>db.statement</c>/<c>peer.service</c> set the HTTP transport still emits. Two are outside
/// that set: <c>db.user</c>, which the conventions dropped without a replacement and the HTTP transport also
/// emits, and the <c>db.clickhouse.*</c> counters, which are shared with it.
/// </remarks>
internal static class TcpActivity
{
    internal const string TagSystemName = "db.system.name";
    internal const string TagNamespace = "db.namespace";
    internal const string TagOperationName = "db.operation.name";
    internal const string TagQueryText = "db.query.text";
    internal const string TagUser = "db.user";
    internal const string TagServerAddress = "server.address";
    internal const string TagServerPort = "server.port";
    internal const string TagErrorType = "error.type";
    internal const string TagResponseStatusCode = "db.response.status_code";
    internal const string TagQueryId = "db.clickhouse.query_id";
    internal const string TagReadRows = "db.clickhouse.read_rows";
    internal const string TagReadBytes = "db.clickhouse.read_bytes";
    internal const string TagWrittenRows = "db.clickhouse.written_rows";
    internal const string TagWrittenBytes = "db.clickhouse.written_bytes";
    internal const string TagElapsedNs = "db.clickhouse.elapsed_ns";
    internal const string TagResultRows = "db.clickhouse.result_rows";
    internal const string TagResultBytes = "db.clickhouse.result_bytes";

    /// <summary>Stands in for the operation name of a statement whose leading keyword could not be read.</summary>
    internal const string UnknownOperation = "query";

    private const string SystemName = "clickhouse";

    // A statement whose leading keyword is longer than this is not a keyword, so no operation name is reported
    // rather than putting an unbounded string into a low-cardinality attribute.
    private const int MaxOperationNameLength = 16;

    internal static ActivitySource Source { get; } = CreateSource();

    /// <summary>Starts the span for one SQL statement, naming it after the statement's leading keyword.</summary>
    /// <param name="options">The client options the endpoint and database come from.</param>
    /// <param name="sql">The statement, reported as <c>db.query.text</c> when that is enabled.</param>
    /// <param name="operation">The statement's operation name from <see cref="OperationName"/>, or null.</param>
    /// <param name="queryId">The caller's query id, or null to let the server assign one.</param>
    /// <returns>The started span, or null when nothing is listening.</returns>
    public static Activity StartStatement(ClickHouseTcpClientOptions options, string sql, string operation, string queryId)
    {
        Activity activity = Source.StartActivity(operation ?? UnknownOperation, ActivityKind.Client);
        if (activity is null)
        {
            return null;
        }

        if (!SetEndpointTags(activity, options))
        {
            return activity;
        }

        if (operation is not null)
        {
            activity.SetTag(TagOperationName, operation);
        }

        // The join key to the server's own record of the same query, in system.query_log and
        // system.opentelemetry_span_log. Only reported when the caller set one: a server-assigned id never
        // reaches the client.
        if (queryId is not null)
        {
            activity.SetTag(TagQueryId, queryId);
        }

        if (options.IncludeSqlInActivityTags)
        {
            string text = options.StatementForTelemetry(sql);
            if (text.Length != 0)
            {
                activity.SetTag(TagQueryText, text);
            }
        }

        return activity;
    }

    /// <summary>Starts the span for a Ping.</summary>
    /// <param name="options">The client options the endpoint comes from.</param>
    /// <returns>The started span, or null when nothing is listening.</returns>
    public static Activity StartPing(ClickHouseTcpClientOptions options)
    {
        Activity activity = Source.StartActivity("ping", ActivityKind.Client);
        SetEndpointTags(activity, options);
        return activity;
    }

    /// <summary>Starts the span covering a dial, the TLS negotiation and the handshake.</summary>
    /// <param name="options">The client options the endpoint comes from.</param>
    /// <returns>The started span, or null when nothing is listening.</returns>
    public static Activity StartConnect(ClickHouseTcpClientOptions options)
    {
        Activity activity = Source.StartActivity("connect", ActivityKind.Client);
        SetEndpointTags(activity, options);
        return activity;
    }

    /// <summary>Records the accumulated progress counters for the operation.</summary>
    /// <param name="activity">The span, or null.</param>
    /// <param name="totals">The sum of the operation's progress increments.</param>
    public static void SetProgressTotals(this Activity activity, ClickHouseTcpProgress totals)
    {
        if (activity is null || !activity.IsAllDataRequested)
        {
            return;
        }

        activity.SetTag(TagReadRows, totals.Rows);
        activity.SetTag(TagReadBytes, totals.Bytes);
        activity.SetTag(TagElapsedNs, totals.ElapsedNs);

        // Only reported for an INSERT, and a zero tag on every SELECT would be noise.
        if (totals.WroteRows != 0 || totals.WroteBytes != 0)
        {
            activity.SetTag(TagWrittenRows, totals.WroteRows);
            activity.SetTag(TagWrittenBytes, totals.WroteBytes);
        }
    }

    /// <summary>Records the query's execution summary.</summary>
    /// <param name="activity">The span, or null.</param>
    /// <param name="info">The summary the server sent.</param>
    public static void SetProfileInfo(this Activity activity, ClickHouseTcpProfileInfo info)
    {
        if (activity is null || !activity.IsAllDataRequested)
        {
            return;
        }

        activity.SetTag(TagResultRows, info.Rows);
        activity.SetTag(TagResultBytes, info.Bytes);
    }

    /// <summary>Marks the span as completed without error.</summary>
    /// <param name="activity">The span, or null.</param>
    public static void SetSuccess(this Activity activity) => activity?.SetStatus(ActivityStatusCode.Ok);

    /// <summary>Marks the span as failed and attaches the exception as an event.</summary>
    /// <param name="activity">The span, or null.</param>
    /// <param name="exception">The exception that ended the operation.</param>
    public static void SetError(this Activity activity, Exception exception)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, exception.Message);

        if (!activity.IsAllDataRequested)
        {
            return;
        }

        activity.SetTag(TagErrorType, exception.GetType().FullName);
        if (exception is ClickHouseServerException server)
        {
            activity.SetTag(TagResponseStatusCode, server.Code.ToString(CultureInfo.InvariantCulture));
        }

        activity.AddEvent(new ActivityEvent(
            "exception",
            tags: new ActivityTagsCollection
            {
                { "exception.type", exception.GetType().FullName },
                { "exception.message", exception.Message },
                { "exception.stacktrace", exception.ToString() },
            }));
    }

    // Returns whether the span wants attributes at all, so a caller adding more of its own needs no second check.
    private static bool SetEndpointTags(Activity activity, ClickHouseTcpClientOptions options)
    {
        if (activity is null || !activity.IsAllDataRequested)
        {
            return false;
        }

        activity.SetTag(TagSystemName, SystemName);
        activity.SetTag(TagServerAddress, options.Host);
        activity.SetTag(TagServerPort, options.ResolvedPort);
        activity.SetTag(TagNamespace, options.Database);
        activity.SetTag(TagUser, options.Username);
        return true;
    }

    /// <summary>
    /// The statement's leading keyword, uppercased — <c>SELECT</c>, <c>INSERT</c>, <c>WITH</c>. Null when the
    /// statement does not start with a plain word, which keeps a generated or oddly-formatted statement out of
    /// the span name and out of a low-cardinality attribute.
    /// </summary>
    /// <param name="sql">The statement.</param>
    /// <returns>The operation name, or null when there is none to read.</returns>
    public static string OperationName(string sql)
    {
        if (sql is null)
        {
            return null;
        }

        int start = 0;
        while (start < sql.Length && char.IsWhiteSpace(sql[start]))
        {
            start++;
        }

        int end = start;
        while (end < sql.Length && char.IsAsciiLetter(sql[end]))
        {
            end++;
        }

        int length = end - start;
        return length is > 0 and <= MaxOperationNameLength
            ? sql[start..end].ToUpperInvariant()
            : null;
    }

    private static ActivitySource CreateSource()
    {
        string version = typeof(TcpActivity).Assembly.GetCustomAttribute<AssemblyFileVersionAttribute>()?.Version;
        return new ActivitySource(ClickHouseTcpDiagnostics.ActivitySourceName, version);
    }
}
