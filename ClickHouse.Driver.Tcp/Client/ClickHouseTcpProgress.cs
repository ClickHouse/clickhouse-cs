using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// One decoded Progress packet, which the server sends repeatedly while a query runs.
/// </summary>
/// <remarks>
/// <b>Every counter is an increment, not a running total.</b> A consumer that wants the totals so far has to add
/// the packets up — use <see cref="operator +"/> or <see cref="Add"/> rather than keeping the last packet, which
/// only reports the most recent step. <see cref="TotalRows"/> is an increment too: the server raises its estimate
/// as it learns how much data the query has to touch.
/// </remarks>
public readonly record struct ClickHouseTcpProgress
{
    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpProgress"/> struct.</summary>
    /// <param name="rows">Rows read in this increment.</param>
    /// <param name="bytes">Bytes read in this increment.</param>
    /// <param name="totalRows">The rise in the server's estimate of the rows to read.</param>
    /// <param name="wroteRows">Rows written in this increment (INSERT).</param>
    /// <param name="wroteBytes">Bytes written in this increment (INSERT).</param>
    /// <param name="elapsedNs">Server-side time in nanoseconds spent during this increment.</param>
    public ClickHouseTcpProgress(ulong rows, ulong bytes, ulong totalRows, ulong wroteRows, ulong wroteBytes, ulong elapsedNs)
    {
        Rows = rows;
        Bytes = bytes;
        TotalRows = totalRows;
        WroteRows = wroteRows;
        WroteBytes = wroteBytes;
        ElapsedNs = elapsedNs;
    }

    /// <summary>Rows read in this increment.</summary>
    public ulong Rows { get; }

    /// <summary>Bytes read in this increment.</summary>
    public ulong Bytes { get; }

    /// <summary>The rise in the server's estimate of the rows this query has to read.</summary>
    public ulong TotalRows { get; }

    /// <summary>Rows written in this increment, for an INSERT.</summary>
    public ulong WroteRows { get; }

    /// <summary>Bytes written in this increment, for an INSERT.</summary>
    public ulong WroteBytes { get; }

    /// <summary>Server-side time in nanoseconds spent during this increment.</summary>
    public ulong ElapsedNs { get; }

    /// <summary>Adds two increments field by field, giving the running total.</summary>
    /// <param name="left">The total so far.</param>
    /// <param name="right">The increment to add.</param>
    /// <returns>The new total.</returns>
    public static ClickHouseTcpProgress operator +(ClickHouseTcpProgress left, ClickHouseTcpProgress right) => new(
        left.Rows + right.Rows,
        left.Bytes + right.Bytes,
        left.TotalRows + right.TotalRows,
        left.WroteRows + right.WroteRows,
        left.WroteBytes + right.WroteBytes,
        left.ElapsedNs + right.ElapsedNs);

    /// <summary>Adds two increments field by field, giving the running total.</summary>
    /// <param name="left">The total so far.</param>
    /// <param name="right">The increment to add.</param>
    /// <returns>The new total.</returns>
    public static ClickHouseTcpProgress Add(ClickHouseTcpProgress left, ClickHouseTcpProgress right) => left + right;

    /// <summary>Reads a Progress packet body at the negotiated version.</summary>
    /// <param name="reader">The reader positioned at the packet body.</param>
    /// <param name="negotiated">The negotiated protocol, gating the trailing counters.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded progress.</returns>
    internal static async ValueTask<ClickHouseTcpProgress> ReadAsync(ClickHouseBinaryReader reader, NegotiatedProtocol negotiated, CancellationToken cancellationToken)
    {
        ulong rows = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        ulong bytes = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        ulong totalRows = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);

        ulong wroteRows = 0;
        ulong wroteBytes = 0;
        if (negotiated.Supports(ProtocolFeature.ProgressWriteInfo))
        {
            wroteRows = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
            wroteBytes = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        }

        ulong elapsedNs = 0;
        if (negotiated.Supports(ProtocolFeature.ProgressElapsedNs))
        {
            elapsedNs = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        }

        return new ClickHouseTcpProgress(rows, bytes, totalRows, wroteRows, wroteBytes, elapsedNs);
    }
}
