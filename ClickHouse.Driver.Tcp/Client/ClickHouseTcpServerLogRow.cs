using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// One row of the server's own log, streamed to the client while a query runs. The server sends these only when
/// the query asks it to, with the <c>send_logs_level</c> setting (its default, <c>fatal</c>, is effectively
/// silent).
/// </summary>
/// <remarks>
/// Every member is owned, so a row is safe to keep after the callback returns.
/// </remarks>
public readonly record struct ClickHouseTcpServerLogRow
{
    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpServerLogRow"/> struct.</summary>
    /// <param name="eventTime">When the server wrote the line, to microsecond resolution.</param>
    /// <param name="hostName">The server host that wrote the line.</param>
    /// <param name="queryId">The id of the query the line belongs to.</param>
    /// <param name="threadId">The OS thread that wrote the line.</param>
    /// <param name="level">The severity.</param>
    /// <param name="source">The server-side logger name.</param>
    /// <param name="text">The message.</param>
    public ClickHouseTcpServerLogRow(
        DateTimeOffset eventTime,
        string hostName,
        string queryId,
        ulong threadId,
        ClickHouseTcpServerLogLevel level,
        string source,
        string text)
    {
        EventTime = eventTime;
        HostName = hostName;
        QueryId = queryId;
        ThreadId = threadId;
        Level = level;
        Source = source;
        Text = text;
    }

    /// <summary>When the server wrote the line, to microsecond resolution, as a UTC instant.</summary>
    public DateTimeOffset EventTime { get; }

    /// <summary>The server host that wrote the line.</summary>
    public string HostName { get; }

    /// <summary>The id of the query the line belongs to.</summary>
    public string QueryId { get; }

    /// <summary>The OS thread that wrote the line.</summary>
    public ulong ThreadId { get; }

    /// <summary>The severity.</summary>
    public ClickHouseTcpServerLogLevel Level { get; }

    /// <summary>The server-side logger name, e.g. <c>executeQuery</c>.</summary>
    public string Source { get; }

    /// <summary>The message.</summary>
    public string Text { get; }
}

/// <summary>
/// The severity of a <see cref="ClickHouseTcpServerLogRow"/>. The values are the server's own log priorities, so
/// a lower number is more severe — the reverse of <c>Microsoft.Extensions.Logging.LogLevel</c>.
/// </summary>
public enum ClickHouseTcpServerLogLevel
{
    /// <summary>The server sent a priority outside the range it documents.</summary>
    Unknown = 0,

    /// <summary>The process cannot continue.</summary>
    Fatal = 1,

    /// <summary>A failure that needs attention now.</summary>
    Critical = 2,

    /// <summary>A failure.</summary>
    Error = 3,

    /// <summary>Something unexpected that did not fail the operation.</summary>
    Warning = 4,

    /// <summary>A normal but significant event.</summary>
    Notice = 5,

    /// <summary>Informational progress.</summary>
    Information = 6,

    /// <summary>Detail for diagnosing a problem.</summary>
    Debug = 7,

    /// <summary>The most detailed level the server emits by query.</summary>
    Trace = 8,

    /// <summary>Test-only output.</summary>
    Test = 9,
}
