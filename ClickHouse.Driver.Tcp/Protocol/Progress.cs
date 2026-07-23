using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// A decoded Progress packet: cumulative counters the server emits as a query runs. Each packet is a delta;
/// callers accumulate.
/// </summary>
internal readonly struct Progress
{
    /// <summary>Initializes a new instance of the <see cref="Progress"/> struct.</summary>
    /// <param name="rows">Rows read so far (delta).</param>
    /// <param name="bytes">Bytes read so far (delta).</param>
    /// <param name="totalRows">Total rows to read, if known.</param>
    /// <param name="wroteRows">Rows written (INSERT), if applicable.</param>
    /// <param name="wroteBytes">Bytes written (INSERT), if applicable.</param>
    /// <param name="elapsedNs">Server-side elapsed time in nanoseconds.</param>
    public Progress(ulong rows, ulong bytes, ulong totalRows, ulong wroteRows, ulong wroteBytes, ulong elapsedNs)
    {
        Rows = rows;
        Bytes = bytes;
        TotalRows = totalRows;
        WroteRows = wroteRows;
        WroteBytes = wroteBytes;
        ElapsedNs = elapsedNs;
    }

    /// <summary>Rows read (delta).</summary>
    public ulong Rows { get; }

    /// <summary>Bytes read (delta).</summary>
    public ulong Bytes { get; }

    /// <summary>Total rows to read, if known.</summary>
    public ulong TotalRows { get; }

    /// <summary>Rows written (delta), for INSERT.</summary>
    public ulong WroteRows { get; }

    /// <summary>Bytes written (delta), for INSERT.</summary>
    public ulong WroteBytes { get; }

    /// <summary>Server-side elapsed time in nanoseconds.</summary>
    public ulong ElapsedNs { get; }

    /// <summary>Reads a Progress packet body at the negotiated version.</summary>
    /// <param name="reader">The reader positioned at the packet body.</param>
    /// <param name="negotiated">The negotiated protocol, gating the trailing counters.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded progress.</returns>
    public static async ValueTask<Progress> ReadAsync(ClickHouseBinaryReader reader, NegotiatedProtocol negotiated, CancellationToken cancellationToken)
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

        return new Progress(rows, bytes, totalRows, wroteRows, wroteBytes, elapsedNs);
    }
}
