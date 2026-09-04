using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The once-per-query execution summary the server sends alongside the result. Unlike
/// <see cref="ClickHouseTcpProgress"/> these are totals, not increments.
/// </summary>
public readonly record struct ClickHouseTcpProfileInfo
{
    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpProfileInfo"/> struct.</summary>
    /// <param name="rows">Rows in the result.</param>
    /// <param name="blocks">Blocks in the result.</param>
    /// <param name="bytes">Bytes in the result.</param>
    /// <param name="appliedLimit">Whether a LIMIT was applied.</param>
    /// <param name="rowsBeforeLimit">Rows before the LIMIT.</param>
    /// <param name="calculatedRowsBeforeLimit">Whether <paramref name="rowsBeforeLimit"/> is meaningful.</param>
    public ClickHouseTcpProfileInfo(ulong rows, ulong blocks, ulong bytes, bool appliedLimit, ulong rowsBeforeLimit, bool calculatedRowsBeforeLimit)
    {
        Rows = rows;
        Blocks = blocks;
        Bytes = bytes;
        AppliedLimit = appliedLimit;
        RowsBeforeLimit = rowsBeforeLimit;
        CalculatedRowsBeforeLimit = calculatedRowsBeforeLimit;
    }

    /// <summary>Rows in the result.</summary>
    public ulong Rows { get; }

    /// <summary>Blocks in the result.</summary>
    public ulong Blocks { get; }

    /// <summary>Bytes in the result.</summary>
    public ulong Bytes { get; }

    /// <summary>Whether a LIMIT was applied.</summary>
    public bool AppliedLimit { get; }

    /// <summary>Rows before the LIMIT was applied.</summary>
    public ulong RowsBeforeLimit { get; }

    /// <summary>Whether <see cref="RowsBeforeLimit"/> was calculated (otherwise it is not meaningful).</summary>
    public bool CalculatedRowsBeforeLimit { get; }

    /// <summary>Reads a ProfileInfo packet body.</summary>
    /// <param name="reader">The reader positioned at the packet body.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded profile info.</returns>
    internal static async ValueTask<ClickHouseTcpProfileInfo> ReadAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        ulong rows = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        ulong blocks = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        ulong bytes = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        bool appliedLimit = await reader.ReadBoolAsync(cancellationToken).ConfigureAwait(false);
        ulong rowsBeforeLimit = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        bool calculatedRowsBeforeLimit = await reader.ReadBoolAsync(cancellationToken).ConfigureAwait(false);

        return new ClickHouseTcpProfileInfo(rows, blocks, bytes, appliedLimit, rowsBeforeLimit, calculatedRowsBeforeLimit);
    }
}
