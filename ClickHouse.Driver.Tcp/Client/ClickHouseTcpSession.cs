using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Implements <see cref="IClickHouseTcpSession"/> by delegating to a client backed by a
/// <see cref="PinnedConnectionSource"/>.
/// </summary>
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
    public ValueTask InsertRowsAsync<T>(
        string sql,
        IReadOnlyList<T> rows,
        ClickHouseTcpInsertOptions options = null,
        CancellationToken cancellationToken = default)
        where T : class
        => operations.InsertRowsAsync(sql, rows, options, cancellationToken);

    /// <inheritdoc/>
    public ValueTask InsertRowsAsync(
        string sql,
        IReadOnlyList<object[]> rows,
        ClickHouseTcpInsertOptions options = null,
        CancellationToken cancellationToken = default)
        => operations.InsertRowsAsync(sql, rows, options, cancellationToken);

    /// <inheritdoc/>
    public ValueTask PingAsync(CancellationToken cancellationToken = default)
        => operations.PingAsync(cancellationToken);

    /// <inheritdoc/>
    public ValueTask<ClickHouseTcpServerInfo> GetServerInfoAsync(CancellationToken cancellationToken = default)
        => operations.GetServerInfoAsync(cancellationToken);

    /// <inheritdoc/>
    public ValueTask<object> ExecuteScalarAsync(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default)
        => operations.ExecuteScalarAsync(sql, options, cancellationToken);

    /// <summary>
    /// Ends the session and closes its connection without disposing the parent client.
    /// </summary>
    /// <remarks>
    /// Active operations are aborted without waiting for their pool slot to be released; see
    /// <see cref="PinnedConnectionSource.DisposeAsync"/>. Repeated calls return immediately.
    /// </remarks>
    /// <returns>A task that completes when disposal has finished or an active operation has been aborted.</returns>
    public ValueTask DisposeAsync() => operations.DisposeAsync();
}
