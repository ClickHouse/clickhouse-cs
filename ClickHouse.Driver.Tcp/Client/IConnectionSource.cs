using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// Supplies connections to the client, hiding whether a connection is opened fresh, reused, or drawn from a
/// pool. A caller rents a connection for one operation and disposes the returned lease to give it back; the
/// source decides, when a lease returns, whether the connection is reusable or must be discarded.
///
/// <para>
/// <see cref="ConnectionPool"/> is the implementation the client builds. The seam remains so the client's own
/// behaviour can be exercised over a source that needs no server.
/// </para>
/// </summary>
internal interface IConnectionSource : IAsyncDisposable
{
    /// <summary>
    /// Rents a ready connection, waiting if none is currently available. Dispose the returned lease to return
    /// the connection. On failure to obtain one (a dial timeout, or no connection freeing up in time) the call
    /// throws and nothing is leased.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for and establishing a connection.</param>
    /// <returns>A lease over a ready connection.</returns>
    /// <exception cref="TimeoutException">No connection became available within the source's wait limit.</exception>
    ValueTask<IConnectionLease> RentAsync(CancellationToken cancellationToken);
}

/// <summary>
/// A rented connection. Disposing the lease returns the connection to its source exactly once (disposing more
/// than once is a no-op). The lease does not own the connection's teardown — the source decides, when the lease
/// is returned, whether to keep the connection for reuse or discard it (a connection left terminated by a failed
/// operation is never reused).
/// </summary>
internal interface IConnectionLease : IAsyncDisposable
{
    /// <summary>The rented connection, valid until the lease is disposed.</summary>
    ClickHouseTcpConnection Connection { get; }

    /// <summary>
    /// How long before the source retires this connection for age, or null when it has no age limit. An
    /// operation bounds its own server-side execution time by this, so the server ends a query that would
    /// outlive its connection rather than the connection being cut off mid-stream.
    /// </summary>
    TimeSpan? RemainingLifetime { get; }
}
