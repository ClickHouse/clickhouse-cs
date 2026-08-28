using System;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Optional callbacks for the metadata the server interleaves into a query or insert response: progress,
/// the execution summary, its own log lines, its performance counters, and the WITH TOTALS / extremes rows.
/// Set it on <see cref="ClickHouseTcpQueryOptions.Callbacks"/>.
/// </summary>
/// <remarks>
/// <para>
/// Every member is optional, and an unset one costs nothing but the discarded result: the packets are decoded
/// either way, to keep the connection aligned.
/// </para>
/// <para>
/// Callbacks run <b>synchronously on the thread draining the response</b>, in packet order. Keep them fast, and
/// never throw: an exception propagates out of the operation and terminates the connection.
/// </para>
/// <para>
/// <b>Every block is borrowed</b>, on the same contract as
/// <see cref="IClickHouseTcpOperations.StreamAsync"/>: it is released as soon as the callback returns, so copy
/// out what must outlive it and retain neither the block, its columns, nor their value spans.
/// </para>
/// </remarks>
public sealed class ClickHouseTcpQueryCallbacks
{
    /// <summary>
    /// Called for each progress increment the server reports as the query runs. The counters are increments, not
    /// running totals — see <see cref="ClickHouseTcpProgress"/>.
    /// </summary>
    public Action<ClickHouseTcpProgress> OnProgress { get; init; }

    /// <summary>Called once with the query's execution summary (result rows, blocks, bytes, whether a LIMIT applied).</summary>
    public Action<ClickHouseTcpProfileInfo> OnProfileInfo { get; init; }

    /// <summary>
    /// Called with a borrowed block of the server's own log lines, whose columns are <c>event_time</c>,
    /// <c>event_time_microseconds</c>, <c>host_name</c>, <c>query_id</c>, <c>thread_id</c>, <c>priority</c>,
    /// <c>source</c> and <c>text</c>. The server sends them only when the query sets <c>send_logs_level</c>;
    /// its default, <c>fatal</c>, is effectively silent.
    /// </summary>
    /// <remarks>
    /// <c>priority</c> is an <c>Int8</c> Poco severity, so a <b>lower number is more severe</b> — 1 fatal up to
    /// 9 test. Filter with <c>&lt;=</c>, and treat a value outside 1..9 as unknown. <c>event_time</c> is a
    /// <c>DateTime</c> column read as <see cref="uint"/> whole Unix seconds.
    /// </remarks>
    /// <example>
    /// <code>
    /// OnLog = block =>
    /// {
    ///     ReadOnlySpan&lt;sbyte&gt; priority = block.Column&lt;sbyte&gt;("priority").Values;
    ///     IColumn&lt;string&gt; text = block.Column&lt;string&gt;("text");
    ///     for (int row = 0; row &lt; block.RowCount; row++)
    ///     {
    ///         if (priority[row] &lt;= 4) logger.LogWarning("{Message}", text[row]);   // warning or worse
    ///     }
    /// }
    /// </code>
    /// </example>
    public Action<Block> OnLog { get; init; }

    /// <summary>
    /// Called with a borrowed block of the server's performance counters, whose columns are <c>host_name</c>,
    /// <c>current_time</c>, <c>thread_id</c> (0 for a query-wide total), <c>type</c> (1 an increment to add up,
    /// 2 a gauge reading that replaces the last), <c>name</c> and <c>value</c> (a signed <see cref="long"/>,
    /// because a gauge can fall). The same counter arrives repeatedly as the query progresses, and reading
    /// <c>name</c> allocates a string per row.
    /// </summary>
    public Action<Block> OnProfileEvents { get; init; }

    /// <summary>Called with the borrowed WITH TOTALS block, whose single row has the query's own result shape.</summary>
    public Action<Block> OnTotals { get; init; }

    /// <summary>
    /// Called with the borrowed extremes block, whose two rows are the minimum and the maximum in the query's own
    /// result shape. The server sends it only when the query sets the <c>extremes</c> setting.
    /// </summary>
    public Action<Block> OnExtremes { get; init; }
}
