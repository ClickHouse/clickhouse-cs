using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Owns a shared <see cref="ClickHouseTcpClient"/> and its pool. Register and dispose the data source as a
/// singleton; callers must not dispose the client returned by <see cref="GetClient"/>.
/// </summary>
[Experimental("CHTCP0001")]
public sealed class ClickHouseTcpDataSource : IClickHouseTcpDataSource
{
    private readonly ClickHouseTcpClient client;

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
    }

    /// <summary>The configuration every operation from this data source runs under.</summary>
    public ClickHouseTcpClientOptions Options => client.Options;

    /// <summary>
    /// Returns the shared client: the same instance on every call, and the data source's to dispose, not the
    /// caller's. Disposing it closes the pool, which ends every other holder's operations as well.
    /// </summary>
    /// <returns>The client this data source owns.</returns>
    public IClickHouseTcpClient GetClient() => client;

    /// <summary>Opens a caller-owned session pinned to one connection for preserving server-side state.</summary>
    /// <param name="cancellationToken">A token to observe while waiting for and establishing the connection.</param>
    /// <returns>A session pinned to one connection.</returns>
    public ValueTask<IClickHouseTcpSession> OpenSessionAsync(CancellationToken cancellationToken = default)
        => client.OpenSessionAsync(cancellationToken);

    /// <summary>Closes the pool and every connection in it. The client from <see cref="GetClient"/> stops working.</summary>
    /// <returns>A task that completes when the pool is closed.</returns>
    public ValueTask DisposeAsync() => client.DisposeAsync();

    /// <summary>
    /// Closes the pool, blocking until it is closed. Present because a synchronous
    /// <c>ServiceProvider.Dispose()</c> rejects a singleton that offers only <see cref="IAsyncDisposable"/>;
    /// prefer <see cref="DisposeAsync"/> wherever the call site can await.
    /// </summary>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
