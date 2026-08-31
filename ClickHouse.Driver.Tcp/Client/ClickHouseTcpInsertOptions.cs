using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Per-insert query and data-stream options.
/// </summary>
public sealed record ClickHouseTcpInsertOptions : ClickHouseTcpQueryOptions
{
    /// <summary>
    /// A cap on the rows per wire block, applied alongside the client's internal byte target, so a wide or large
    /// insert is split into several blocks instead of one huge one. Defaults to 50,000 rows. Set null to write
    /// the whole insert as a single block, whatever its row count.
    ///
    /// <para>
    /// A row insert (<c>InsertRowsAsync</c>) converts one block at a time, so this cap also bounds the memory it
    /// converts through: one buffer per target column, of this many values. A larger cap — or null, for a single
    /// block — raises that in proportion to the rows.
    /// </para>
    /// </summary>
    public int? MaxRowsPerBlock { get; init; } = ClickHouseTcpConnection.DefaultMaxRowsPerBlock;
}
