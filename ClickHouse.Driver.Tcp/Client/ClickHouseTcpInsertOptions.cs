using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Per-insert query and data-stream options.
/// </summary>
public sealed record ClickHouseTcpInsertOptions : ClickHouseTcpQueryOptions
{
    /// <summary>
    /// A cap on the rows per wire block, applied alongside the client's internal byte target, so a wide or large
    /// insert is split into several blocks instead of one huge one. Defaults to 1,000,000 rows. Set null to write
    /// the whole insert as a single block, whatever its row count.
    /// </summary>
    public int? MaxRowsPerBlock { get; init; } = ClickHouseTcpConnection.DefaultMaxRowsPerBlock;
}
