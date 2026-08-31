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
    /// <remarks>
    /// <b>How often this fires is the server's <c>interactive_delay</c> setting</b>, in microseconds, defaulting
    /// to 100,000 — so about ten packets a second on a query that runs long enough. Lower it per query to drive a
    /// smoother progress bar (<c>Settings = { ["interactive_delay"] = "30000" }</c>) and accept more packets for
    /// the same result. A query that finishes inside one interval reports once or not at all, so a consumer must
    /// not wait for a first packet before showing anything.
    /// <para>
    /// An <b>insert</b> gets none of these: the server reports no progress for rows a client streams to it. Use
    /// <see cref="OnBlockWritten"/> there.
    /// </para>
    /// </remarks>
    public Action<ClickHouseTcpProgress> OnProgress { get; init; }

    /// <summary>
    /// Called as each wire block of an insert finishes going out, with the client's own count of its rows and
    /// bytes. Never called for a query, which sends no blocks.
    /// </summary>
    /// <remarks>
    /// This is an insert's progress, and the only one it has — see <see cref="ClickHouseTcpBlockWritten"/> for why
    /// <see cref="OnProgress"/> cannot serve. It reports what the client has sent, not what the server has
    /// applied, so a callback that has seen every block still does not know the insert succeeded.
    /// </remarks>
    public Action<ClickHouseTcpBlockWritten> OnBlockWritten { get; init; }

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
