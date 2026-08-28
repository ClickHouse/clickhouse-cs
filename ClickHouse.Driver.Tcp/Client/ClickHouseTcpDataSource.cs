using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Owns one <see cref="ClickHouseTcpClient"/> and its connection pool, and hands out views onto it that cannot
/// close it. Register this as a singleton in a dependency-injection container and let it be the thing that gets
/// disposed at shutdown; inject <see cref="GetClient"/>'s result everywhere else.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="ClickHouseTcpClient"/> is already thread-safe and pooled, so this adds no pooling of its own. What
/// it adds is ownership: <see cref="GetClient"/> returns a client whose <c>DisposeAsync</c> does nothing, so a
/// scoped service that disposes what it was injected cannot take the shared pool down with it. Disposing the data
/// source closes the pool, once.
/// </para>
/// <para>
/// This type is experimental: its surface may change in a future release. Suppress diagnostic
/// <c>CHTCP0001</c> to acknowledge that.
/// </para>
/// </remarks>
[Experimental("CHTCP0001")]
public sealed class ClickHouseTcpDataSource : IAsyncDisposable, IDisposable
{
    private readonly ClickHouseTcpClient client;
    private readonly NonOwningClient view;

    /// <summary>Creates a data source from a connection string.</summary>
    /// <param name="connectionString">The connection string (keys such as <c>Host</c>, <c>Port</c>, <c>Username</c>, <c>set_&lt;name&gt;</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
    /// <exception cref="ArgumentException">A resulting option value is invalid.</exception>
    public ClickHouseTcpDataSource(string connectionString)
        : this(ClickHouseTcpClientOptions.FromConnectionString(connectionString))
    {
    }

    /// <summary>Creates a data source from options.</summary>
    /// <param name="options">The client configuration (endpoint, credentials, timeouts, client-level settings).</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">An option value is invalid (see <see cref="ClickHouseTcpClientOptions"/>).</exception>
    public ClickHouseTcpDataSource(ClickHouseTcpClientOptions options)
    {
        client = new ClickHouseTcpClient(options);
        view = new NonOwningClient(client);
    }

    /// <summary>The configuration every operation from this data source runs under.</summary>
    public ClickHouseTcpClientOptions Options => client.Options;

    /// <summary>
    /// Returns the shared client. The same instance every time, and disposing it does nothing — only disposing
    /// the data source closes the pool.
    /// </summary>
    /// <returns>A non-owning view of the shared client.</returns>
    public IClickHouseTcpClient GetClient() => view;

    /// <summary>
    /// Opens a session on the shared pool: one connection, held until the session is disposed, that carries
    /// server-side state such as a temporary table or a <c>SET</c> from one operation to the next.
    /// </summary>
    /// <remarks>Unlike <see cref="GetClient"/>, a session <b>is</b> the caller's to dispose, and holds one of the
    /// pool's connections until it is.</remarks>
    /// <param name="cancellationToken">A token to observe while waiting for and establishing the connection.</param>
    /// <returns>A session pinned to one connection.</returns>
    public ValueTask<IClickHouseTcpSession> OpenSessionAsync(CancellationToken cancellationToken = default)
        => client.OpenSessionAsync(cancellationToken);

    /// <summary>Closes the pool and every connection in it. Views handed out by <see cref="GetClient"/> stop working.</summary>
    /// <returns>A task that completes when the pool is closed.</returns>
    public ValueTask DisposeAsync() => client.DisposeAsync();

    /// <summary>
    /// Closes the pool, blocking until it is closed. Present because a synchronous
    /// <c>ServiceProvider.Dispose()</c> rejects a singleton that offers only <see cref="IAsyncDisposable"/>;
    /// prefer <see cref="DisposeAsync"/> wherever the call site can await.
    /// </summary>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    /// <summary>
    /// Delegates every operation to the owned client and swallows disposal, so an injected consumer cannot close
    /// a pool it does not own.
    /// </summary>
    private sealed class NonOwningClient(ClickHouseTcpClient inner) : IClickHouseTcpClient
    {
        public ClickHouseTcpClientOptions Options => inner.Options;

        public IAsyncEnumerable<Block> StreamAsync(string sql, ClickHouseTcpQueryOptions options = null, CancellationToken cancellationToken = default)
            => inner.StreamAsync(sql, options, cancellationToken);

        public IAsyncEnumerable<object[]> QueryAsync(string sql, ClickHouseTcpQueryOptions options = null, CancellationToken cancellationToken = default)
            => inner.QueryAsync(sql, options, cancellationToken);

        public IAsyncEnumerable<T> QueryAsync<T>(string sql, ClickHouseTcpQueryOptions options = null, CancellationToken cancellationToken = default)
            where T : class
            => inner.QueryAsync<T>(sql, options, cancellationToken);

        public ValueTask ExecuteAsync(string sql, ClickHouseTcpQueryOptions options = null, CancellationToken cancellationToken = default)
            => inner.ExecuteAsync(sql, options, cancellationToken);

        public ValueTask<object> ExecuteScalarAsync(string sql, ClickHouseTcpQueryOptions options = null, CancellationToken cancellationToken = default)
            => inner.ExecuteScalarAsync(sql, options, cancellationToken);

        public ValueTask InsertAsync(string sql, IReadOnlyList<IColumn> columns, ClickHouseTcpInsertOptions options = null, CancellationToken cancellationToken = default)
            => inner.InsertAsync(sql, columns, options, cancellationToken);

        public ValueTask InsertRowsAsync<T>(string sql, IReadOnlyList<T> rows, ClickHouseTcpInsertOptions options = null, CancellationToken cancellationToken = default)
            where T : class
            => inner.InsertRowsAsync(sql, rows, options, cancellationToken);

        public ValueTask InsertRowsAsync(string sql, IReadOnlyList<object[]> rows, ClickHouseTcpInsertOptions options = null, CancellationToken cancellationToken = default)
            => inner.InsertRowsAsync(sql, rows, options, cancellationToken);

        public ValueTask PingAsync(CancellationToken cancellationToken = default)
            => inner.PingAsync(cancellationToken);

        public ValueTask<ClickHouseTcpServerInfo> GetServerInfoAsync(CancellationToken cancellationToken = default)
            => inner.GetServerInfoAsync(cancellationToken);

        public ValueTask<IClickHouseTcpSession> OpenSessionAsync(CancellationToken cancellationToken = default)
            => inner.OpenSessionAsync(cancellationToken);

        /// <summary>Does nothing: the data source owns the client.</summary>
        /// <returns>A completed task.</returns>
        public ValueTask DisposeAsync() => default;
    }
}
