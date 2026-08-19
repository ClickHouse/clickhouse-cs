using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// A session over one pinned connection. Opened with <see cref="ClickHouseTcpClient.OpenSessionAsync"/>; see
/// <see cref="IClickHouseTcpSession"/> for what pinning buys and what it costs.
/// </summary>
/// <remarks>
/// The operations are the client's own, run over a source that hands out the pinned connection instead of a pooled
/// one (<see cref="PinnedConnectionSource"/>). Nothing here re-implements a query or an insert, so a session cannot
/// drift from the client, and the pinning rules live in one place rather than in each operation.
/// </remarks>
[Experimental("CHTCP0001")]
internal sealed class ClickHouseTcpSession : IClickHouseTcpSession
{
    private readonly PinnedConnectionSource pinned;
    private readonly ClickHouseTcpClient operations;

    /// <summary>Builds a session over an already-pinned connection.</summary>
    /// <param name="pinned">The source holding the pinned connection's lease.</param>
    /// <param name="operations">A client running over <paramref name="pinned"/>.</param>
    internal ClickHouseTcpSession(PinnedConnectionSource pinned, ClickHouseTcpClient operations)
    {
        this.pinned = pinned;
        this.operations = operations;
    }

    /// <inheritdoc/>
    public ClickHouseTcpClientOptions Options => operations.Options;

    /// <inheritdoc/>
    public bool IsOpen => pinned.IsOpen;

    /// <inheritdoc/>
    public IAsyncEnumerable<Block> StreamAsync(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default)
        => operations.StreamAsync(sql, options, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<object[]> QueryAsync(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default)
        => operations.QueryAsync(sql, options, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<T> QueryAsync<T>(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default)
        where T : class
        => operations.QueryAsync<T>(sql, options, cancellationToken);

    /// <inheritdoc/>
    public ValueTask ExecuteAsync(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default)
        => operations.ExecuteAsync(sql, options, cancellationToken);

    /// <inheritdoc/>
    public ValueTask InsertAsync(
        string sql,
        IReadOnlyList<IColumn> columns,
        ClickHouseTcpInsertOptions options = null,
        CancellationToken cancellationToken = default)
        => operations.InsertAsync(sql, columns, options, cancellationToken);

    /// <inheritdoc/>
    public ValueTask InsertAsync<T>(
        string sql,
        IEnumerable<T> rows,
        ClickHouseTcpInsertOptions options = null,
        CancellationToken cancellationToken = default)
        where T : class
        => operations.InsertAsync(sql, rows, options, cancellationToken);

    /// <inheritdoc/>
    public ValueTask InsertAsync(
        string sql,
        IEnumerable<object[]> rows,
        ClickHouseTcpInsertOptions options = null,
        CancellationToken cancellationToken = default)
        => operations.InsertAsync(sql, rows, options, cancellationToken);

    /// <inheritdoc/>
    public ValueTask PingAsync(CancellationToken cancellationToken = default)
        => operations.PingAsync(cancellationToken);

    /// <summary>
    /// Ends the session, closing its connection rather than pooling it. The client the session came from is
    /// unaffected and keeps working.
    /// </summary>
    /// <returns>
    /// A task that completes when the session is closed — which is not always when its connection is: an operation
    /// still running is aborted rather than waited for, and it is that operation's unwinding, not this call, that
    /// gives the slot back. See <see cref="PinnedConnectionSource.DisposeAsync"/> for what that costs when the
    /// operation is one nothing can resume.
    /// </returns>
    /// <remarks>
    /// Disposing the inner client disposes the pinned source, which is where the closing happens. The inner client
    /// owns no pool of its own, so there is nothing else to tear down. A second call does nothing rather than
    /// waiting for the first, a session having one owner; the pool, which does not, makes concurrent disposals
    /// wait.
    /// </remarks>
    public ValueTask DisposeAsync() => operations.DisposeAsync();
}
