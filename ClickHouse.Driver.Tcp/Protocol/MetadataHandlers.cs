using System;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Optional callbacks for the metadata packets the server interleaves into a query or insert response. Each
/// handler is optional; where one is null the corresponding packet is still consumed to keep the stream
/// aligned, but its contents are discarded.
///
/// <para>
/// Handlers run synchronously on the thread draining the response, in the order the packets arrive on the
/// wire. Keep them fast: they sit on the read path between blocks. A handler that throws propagates out of the
/// operation and terminates the connection, so a handler must not throw for control flow.
/// </para>
///
/// <para>
/// <b>Block-bearing handlers borrow.</b> Blocks are valid only for the duration of the call;
/// the object is disposed and its storage is released as soon as the handler returns.
/// Copy out anything you need to keep, and do not retain the block, its columns, or their value spans.
/// The scalar handlers (<see cref="OnProgress"/>, <see cref="OnProfileInfo"/>)
/// receive owned, immutable values that are safe to retain.
/// </para>
/// </summary>
internal sealed class MetadataHandlers
{
    /// <summary>Invoked for each Progress packet, which the server sends repeatedly as work advances.</summary>
    public Action<Progress> OnProgress { get; init; }

    /// <summary>Invoked for the ProfileInfo summary (rows/blocks/bytes read, limit application).</summary>
    public Action<ProfileInfo> OnProfileInfo { get; init; }

    /// <summary>Invoked with the borrowed Totals block (the WITH TOTALS row). Valid only for the call.</summary>
    public Action<Block> OnTotals { get; init; }

    /// <summary>Invoked with the borrowed Extremes block (min/max rows). Valid only for the call.</summary>
    public Action<Block> OnExtremes { get; init; }

    /// <summary>Invoked with a borrowed block of server log rows. Valid only for the call.</summary>
    public Action<Block> OnLog { get; init; }

    /// <summary>Invoked with a borrowed block of profile-event metric rows. Valid only for the call.</summary>
    public Action<Block> OnProfileEvents { get; init; }
}
