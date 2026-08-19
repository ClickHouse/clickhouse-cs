using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The contract of a ClickHouse client that speaks the native TCP protocol. It runs every
/// <see cref="IClickHouseTcpOperations"/> member over a pool, so consecutive operations need not land on the same
/// connection, and adds the one thing that pins them to one: <see cref="OpenSessionAsync"/>.
/// <see cref="ClickHouseTcpClient"/> is the implementation; code against this interface to substitute a test double.
///
/// <para>
/// This type is experimental: its surface may change in a future release. Suppress diagnostic
/// <c>CHTCP0001</c> to acknowledge that.
/// </para>
/// </summary>
[Experimental("CHTCP0001")]
public interface IClickHouseTcpClient : IClickHouseTcpOperations
{
    /// <summary>
    /// Opens a session: one connection, taken from the pool and held until the session is disposed, that every
    /// operation on the returned object runs over. That is what carries a connection's server-side state — a
    /// temporary table, a <c>SET</c> — from one operation to the next.
    /// </summary>
    /// <remarks>
    /// <b>Dispose it, and keep it short.</b> A session holds one of the pool's
    /// <see cref="ClickHouseTcpClientOptions.MaxPoolSize"/> connections for its whole lifetime, so as many sessions
    /// as the pool is wide leaves nothing for anything else. Disposal closes the connection rather than pooling it
    /// (see <see cref="IClickHouseTcpSession"/>), so it costs a reconnect.
    /// </remarks>
    /// <param name="cancellationToken">A token to observe while waiting for and establishing the connection.</param>
    /// <returns>A session pinned to one connection.</returns>
    /// <exception cref="System.TimeoutException">No connection became available within
    /// <see cref="ClickHouseTcpClientOptions.PoolTimeout"/>.</exception>
    /// <exception cref="System.ObjectDisposedException">The client has been disposed.</exception>
    ValueTask<IClickHouseTcpSession> OpenSessionAsync(CancellationToken cancellationToken = default);
}
