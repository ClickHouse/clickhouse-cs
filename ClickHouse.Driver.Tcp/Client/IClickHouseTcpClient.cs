using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// A pooled native TCP client. Use <see cref="OpenSessionAsync"/> to run operations on one connection.
/// Implemented by <see cref="ClickHouseTcpClient"/>.
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
    /// Reserves one pooled connection for a session, preserving temporary tables and <c>SET</c> settings
    /// across its operations.
    /// </summary>
    /// <remarks>
    /// Each session occupies one of <see cref="ClickHouseTcpClientOptions.MaxPoolSize"/> pool slots. Keep sessions
    /// short-lived and dispose them before the client to avoid delaying client disposal by
    /// <see cref="ClickHouseTcpClientOptions.PoolTimeout"/>. Disposal closes the connection rather than reusing it,
    /// so a replacement requires a new connection.
    /// <para>
    /// A session allows one operation at a time. The client can still run concurrent operations on other
    /// connections; see <see cref="IClickHouseTcpSession"/> for session lifetime and streaming requirements.
    /// </para>
    /// </remarks>
    /// <param name="cancellationToken">A token to observe while waiting for and establishing the connection.</param>
    /// <returns>A session pinned to one connection.</returns>
    /// <exception cref="System.TimeoutException">No connection became available within
    /// <see cref="ClickHouseTcpClientOptions.PoolTimeout"/>.</exception>
    /// <exception cref="System.ObjectDisposedException">The client has been disposed.</exception>
    ValueTask<IClickHouseTcpSession> OpenSessionAsync(CancellationToken cancellationToken = default);
}
