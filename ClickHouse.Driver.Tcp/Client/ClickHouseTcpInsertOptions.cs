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

    /// <summary>
    /// A token identifying <em>this</em> batch of rows, so a repeat of the same insert under the same token is
    /// discarded by the server instead of inserted twice. Null, the default, sends no token. Sets the server's
    /// <c>insert_deduplication_token</c> setting.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This is what makes an insert safe to retry.</b> A
    /// <see cref="ClickHouseTcpTransportException"/> means the connection failed with the insert's outcome unknown,
    /// so a plain retry can duplicate the rows. Retried under the same token, the server recognises the second
    /// attempt and drops it, so the retry is safe whichever way the first attempt went.
    /// </para>
    /// <para>
    /// Two rules make it work. The token must be <b>derived from the data</b>, not generated per attempt — one
    /// token per batch, reused by every retry of that batch, and a new one for the next batch. And it must be
    /// <b>the same across retries of a failed attempt but different for a genuinely new batch</b>, or a later
    /// batch that happens to reuse a token is silently dropped.
    /// </para>
    /// <para>
    /// The server keeps tokens per table and per partition, for a bounded window
    /// (<c>replicated_deduplication_window</c> on a <c>Replicated*</c> engine,
    /// <c>non_replicated_deduplication_window</c> otherwise, the latter off by default). A retry after the window
    /// has passed is no longer recognised, so this bounds how late a retry can be, not how many times it can
    /// happen. Setting the token in <see cref="ClickHouseTcpQueryOptions.Settings"/> does the same thing; this
    /// property wins when both name it.
    /// </para>
    /// </remarks>
    public string DeduplicationToken { get; init; }
}
