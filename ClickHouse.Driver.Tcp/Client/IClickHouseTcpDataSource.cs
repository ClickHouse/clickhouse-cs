using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// What <see cref="ClickHouseTcpDataSource"/> offers: the single owner of a client and its pool, for a container
/// to register and for a wrapper to forward. The counterpart of HTTP's <see cref="IClickHouseDataSource"/>.
/// </summary>
/// <remarks>
/// <para>
/// Depend on this rather than on the class where a test wants to substitute the data source, or where a
/// composition root wraps it — to add a per-tenant endpoint, a health gate, or metrics. Registering the
/// interface is what <c>AddClickHouseTcpDataSource</c> does.
/// </para>
/// <para>
/// <b>Disposing this disposes the client too</b>, which is the point: the data source is the one thing that
/// owns the pool. Nothing that receives <see cref="GetClient"/>'s result may dispose it.
/// </para>
/// <para>
/// This type is experimental: its surface may change in a future release. Suppress diagnostic
/// <c>CHTCP0001</c> to acknowledge that.
/// </para>
/// </remarks>
[Experimental("CHTCP0001")]
public interface IClickHouseTcpDataSource : IAsyncDisposable, IDisposable
{
    /// <summary>The configuration every operation from this data source runs under.</summary>
    ClickHouseTcpClientOptions Options { get; }

    /// <summary>
    /// Returns the shared client: the same instance on every call, and the data source's to dispose, not the
    /// caller's.
    /// </summary>
    /// <returns>The client this data source owns.</returns>
    IClickHouseTcpClient GetClient();

    /// <summary>
    /// Opens a session on the shared pool: one connection, held until the session is disposed, that carries
    /// server-side state such as a temporary table or a <c>SET</c> from one operation to the next.
    /// </summary>
    /// <remarks>Unlike <see cref="GetClient"/>, a session <b>is</b> the caller's to dispose, and holds one of the
    /// pool's connections until it is.</remarks>
    /// <param name="cancellationToken">A token to observe while waiting for and establishing the connection.</param>
    /// <returns>A session pinned to one connection.</returns>
    ValueTask<IClickHouseTcpSession> OpenSessionAsync(CancellationToken cancellationToken = default);
}
