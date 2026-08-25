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
/// handed over. What is <i>not</i> free is asking the server to send them at all — see
/// <see cref="OnServerLog"/>.
/// </para>
/// <para>
/// Callbacks run <b>synchronously on the thread draining the response</b>, in the order the packets arrive.
/// They sit on the read path between result blocks, so keep them fast; hand work off to a queue rather than
/// doing it here. A callback that throws propagates out of the operation and terminates the connection, so
/// never throw for control flow.
/// </para>
/// <para>
/// <see cref="OnTotals"/> and <see cref="OnExtremes"/> <b>borrow</b> their block: it is valid for the call only,
/// and is released as soon as the callback returns. Copy out what must outlive it, and do not retain the block,
/// its columns, or their value spans. Every other callback receives owned values that are safe to keep.
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
    /// Called once per line of the server's own log. The server sends these only when the query sets
    /// <c>send_logs_level</c> (its default, <c>fatal</c>, is effectively silent), so setting this callback alone
    /// produces nothing.
    /// </summary>
    public Action<ClickHouseTcpServerLogRow> OnServerLog { get; init; }

    /// <summary>Called once per server performance counter sample.</summary>
    public Action<ClickHouseTcpProfileEvent> OnProfileEvent { get; init; }

    /// <summary>
    /// Called with the borrowed WITH TOTALS block. Valid for the call only.
    /// </summary>
    public Action<Block> OnTotals { get; init; }

    /// <summary>
    /// Called with the borrowed extremes block, whose two rows are the minimum and the maximum. The server sends
    /// it only when the query sets the <c>extremes</c> setting. Valid for the call only.
    /// </summary>
    public Action<Block> OnExtremes { get; init; }
}
