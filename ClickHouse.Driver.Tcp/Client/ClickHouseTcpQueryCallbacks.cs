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
/// Every member is optional, and setting one costs almost nothing: the packets arrive and are decoded either
/// way, to keep the connection aligned, and an unset callback only means the result is discarded instead of
/// handed over. What is <i>not</i> free is asking the server to send them at all — see <see cref="OnLog"/>.
/// </para>
/// <para>
/// Callbacks run <b>synchronously on the thread draining the response</b>, in the order the packets arrive.
/// They sit on the read path between result blocks, so keep them fast; hand work off to a queue rather than
/// doing it here. A callback that throws propagates out of the operation and terminates the connection, so
/// never throw for control flow.
/// </para>
/// <para>
/// <b>Every block is borrowed</b>, on the same contract as
/// <see cref="IClickHouseTcpOperations.StreamAsync"/>: it is valid for the call only, and is released as soon
/// as the callback returns. Copy out what must outlive it, and do not retain the block, its columns, or their
/// value spans. Nothing is materialized on your behalf, so a callback that reads two columns of one row pays
/// for two columns of one row.
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
    /// Called with a borrowed block of the server's own log lines. The server sends these only when the query
    /// sets <c>send_logs_level</c> (its default, <c>fatal</c>, is effectively silent), so setting this callback
    /// alone produces nothing.
    /// </summary>
    /// <remarks>
    /// <para>The block's schema is fixed by the server:</para>
    /// <list type="table">
    /// <item><term><c>event_time</c></term><description><c>DateTime</c>, read as <see cref="uint"/> — whole Unix seconds.</description></item>
    /// <item><term><c>event_time_microseconds</c></term><description><c>UInt32</c> — the sub-second part, to be added to <c>event_time</c>.</description></item>
    /// <item><term><c>host_name</c></term><description><c>String</c>.</description></item>
    /// <item><term><c>query_id</c></term><description><c>String</c>.</description></item>
    /// <item><term><c>thread_id</c></term><description><c>UInt64</c>.</description></item>
    /// <item><term><c>priority</c></term><description><c>Int8</c> — see the warning below.</description></item>
    /// <item><term><c>source</c></term><description><c>String</c> — the server-side logger name, e.g. <c>executeQuery</c>.</description></item>
    /// <item><term><c>text</c></term><description><c>String</c> — the message.</description></item>
    /// </list>
    /// <para>
    /// <b><c>priority</c> runs the opposite way to <see cref="Microsoft.Extensions.Logging.LogLevel"/>: a lower
    /// number is more severe.</b> It is a Poco severity — 1 fatal, 2 critical, 3 error, 4 warning, 5 notice,
    /// 6 information, 7 debug, 8 trace, 9 test — so filter with <c>&lt;=</c>, not <c>&gt;=</c>. Treat a value
    /// outside 1..9 as unknown rather than failing: the server may add a level.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// OnLog = block =>
    /// {
    ///     ReadOnlySpan&lt;sbyte&gt; priority = block.Column&lt;sbyte&gt;("priority").Values;
    ///     IColumn&lt;string&gt; text = block.Column&lt;string&gt;("text");
    ///     for (int row = 0; row &lt; block.RowCount; row++)
    ///     {
    ///         if (priority[row] &lt;= 4)   // warning or worse
    ///         {
    ///             logger.LogWarning("{Message}", text[row]);
    ///         }
    ///     }
    /// }
    /// </code>
    /// </example>
    public Action<Block> OnLog { get; init; }

    /// <summary>
    /// Called with a borrowed block of the server's performance counters. The same counter arrives repeatedly as
    /// the query progresses.
    /// </summary>
    /// <remarks>
    /// <para>The block's schema is fixed by the server:</para>
    /// <list type="table">
    /// <item><term><c>host_name</c></term><description><c>String</c>.</description></item>
    /// <item><term><c>current_time</c></term><description><c>DateTime</c>, read as <see cref="uint"/> — whole Unix seconds.</description></item>
    /// <item><term><c>thread_id</c></term><description><c>UInt64</c> — 0 for a query-wide total.</description></item>
    /// <item><term><c>type</c></term><description><c>Int8</c> — 1 an increment to add up, 2 a gauge reading that replaces the last.</description></item>
    /// <item><term><c>name</c></term><description><c>String</c> — the counter, e.g. <c>SelectedRows</c>.</description></item>
    /// <item><term><c>value</c></term><description><c>Int64</c>, and signed because a gauge can fall.</description></item>
    /// </list>
    /// <para>
    /// Reading <c>name</c> allocates a string per row, and the server reports many counters per block, so bind
    /// only the columns you need and read <c>name</c> no more often than the filter requires.
    /// </para>
    /// </remarks>
    public Action<Block> OnProfileEvents { get; init; }

    /// <summary>Called with the borrowed WITH TOTALS block, whose single row has the query's own result shape.</summary>
    public Action<Block> OnTotals { get; init; }

    /// <summary>
    /// Called with the borrowed extremes block, whose two rows are the minimum and the maximum in the query's own
    /// result shape. The server sends it only when the query sets the <c>extremes</c> setting.
    /// </summary>
    public Action<Block> OnExtremes { get; init; }
}
