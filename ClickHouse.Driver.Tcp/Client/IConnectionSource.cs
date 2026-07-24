using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// Supplies connections to the client, hiding whether a connection is dialed fresh, reused, or drawn from a
/// pool. A caller rents a connection for one operation and disposes the returned lease to give it back; the
/// source decides on the next rent whether a returned connection is reusable or must be discarded and redialed.
///
/// <para>
/// The single-connection implementation shipped today serializes rents so the one non-thread-safe connection
/// only ever has one in-flight operation; a future multi-connection pool implements the same interface and
/// hands out distinct connections without any change at the call site.
/// </para>
/// </summary>
internal interface IConnectionSource : IAsyncDisposable
{
    /// <summary>
    /// Rents a ready connection, waiting if none is currently available. Dispose the returned lease to return
    /// the connection. On failure to obtain one (e.g. a dial timeout) the call throws and nothing is leased.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for and establishing a connection.</param>
    /// <returns>A lease over a ready connection.</returns>
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
}
