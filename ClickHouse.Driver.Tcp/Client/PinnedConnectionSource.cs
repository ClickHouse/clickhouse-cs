using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// A connection source that hands out the same connection every time: the one a session pinned. It holds a lease on
/// the underlying source for its whole lifetime, so the connection is never anyone else's in between — which is what
/// carries a session's temporary tables and settings from one operation to the next.
/// </summary>
/// <remarks>
/// Being the seam the pool is, it lets a session run the ordinary client code. It adds only the bookkeeping pinning
/// brings: one operation at a time, a lost connection reported as such, and, on disposal, the connection closed
/// instead of handed back. Every state change is made under <c>gate</c>, and the slow parts outside it. The lock is
/// for interlocking, not contention: a rent must not start while disposal is deciding whether the connection is idle.
/// </remarks>
internal sealed class PinnedConnectionSource : IConnectionSource
{
    // Not nameof: CHTCP0001 objects to naming the [Experimental] session type from ordinary internal code.
    private const string SessionTypeName = "ClickHouseTcpSession";

    private readonly IConnectionLease lease;
    private readonly object gate = new();

    // An operation holds the connection: rented and not yet released.
    private bool busy;

    // The connection was lost, so nothing more can run on it. Latched by HasLostTheConnection.
    private bool faulted;

    private bool disposed;

    // The lease has been given back to the pool. Doing that twice would release a permit the source does not hold,
    // letting the pool run one operation over its limit.
    private bool returned;

    /// <summary>Pins the connection behind a lease for the lifetime of this source.</summary>
    /// <param name="lease">The lease to hold, taken from the client's own source.</param>
    internal PinnedConnectionSource(IConnectionLease lease) => this.lease = lease;

    /// <summary>
    /// Whether the pinned connection can still carry an operation. Mid-operation it reports what is known so far, a
    /// busy connection not being reusable by definition.
    /// </summary>
    internal bool IsOpen
    {
        get
        {
            lock (gate)
            {
                if (disposed)
                {
                    return false;
                }

                return busy ? !faulted : !HasLostTheConnection();
            }
        }
    }

    /// <inheritdoc/>
    /// <exception cref="InvalidOperationException">An operation is already running on this connection, or the
    /// connection has been lost.</exception>
    public ValueTask<IConnectionLease> RentAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (gate)
        {
            if (disposed)
            {
                throw new ObjectDisposedException(SessionTypeName);
            }

            // Before the connection is tested: a connection carrying an operation is not reusable, so asking the
            // question below while one runs would condemn a session that is merely busy.
            if (busy)
            {
                throw new InvalidOperationException(
                    "The session is already running an operation, and one connection carries one query at a time. Run the operations one after another, or use the client itself to run them at once over separate connections. A streamed result holds the session until it is read to the end or its enumerator is disposed.");
            }

            if (HasLostTheConnection())
            {
                throw new InvalidOperationException(
                    "The session's connection can no longer be used, so its temporary tables and settings are gone with it. That follows a failed or cancelled operation, a streamed result that was not read to the end, or the server closing the connection. Open a new session to continue.");
            }

            busy = true;
        }

        // A token for one operation, not a second claim: disposing it releases the gate above, not the underlying
        // lease, which this source holds until it is disposed.
        return new ValueTask<IConnectionLease>(new PinnedLease(this));
    }

    /// <summary>
    /// Ends the session: closes the pinned connection and gives the lease back, so the pool accounts for the slot
    /// again. Idempotent.
    /// </summary>
    /// <remarks>
    /// The connection is closed rather than pooled because the next caller must not inherit a session's state. During
    /// an operation it cannot be closed that way — the buffers being read into would go back to the pool underneath it
    /// — so disposal aborts the transport, which is safe to race with, and leaves the return to the operation's
    /// unwinding. That frees an operation parked on the socket but not one parked at a <c>yield</c>: a streamed result
    /// whose consumer stopped advancing never returns its lease, and the pool is short a slot until the client is
    /// disposed. Returning it here instead would let the pool terminate a connection a live operation still reads into.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        bool abort;
        lock (gate)
        {
            if (disposed)
            {
                return;
            }

            disposed = true;

            // `returned` cannot be set yet: only this method and ReleaseAsync's disposed branch set it, and both are
            // reachable only once `disposed` is.
            abort = busy;
            returned = !abort;
        }

        if (abort)
        {
            // Marks the connection final, so the pool cannot reuse it whatever happens next, and frees an operation
            // parked on a read that will never arrive.
            lease.Connection.AbortTransport();
            return;
        }

        await ReturnAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Takes the connection back from a finished operation, keeping it pinned. A session disposed mid-operation is
    /// completed here instead, this being the first moment the connection is idle enough to close.
    /// </summary>
    private ValueTask ReleaseAsync()
    {
        lock (gate)
        {
            busy = false;

            if (!disposed)
            {
                // So an operation that broke the connection is reported as such even if nothing runs on the session
                // again, which is what IsOpen reports.
                HasLostTheConnection();
                return default;
            }

            if (returned)
            {
                return default;
            }

            returned = true;
        }

        return ReturnAsync();
    }

    /// <summary>
    /// Whether the pinned connection has been lost, latching the answer once it has. Call under <c>gate</c>, and only
    /// when no operation is running: mid-operation the connection is legitimately not reusable, so the question does
    /// not apply.
    /// </summary>
    /// <returns>True when nothing more can run on this session.</returns>
    /// <remarks>
    /// The predicate is the pool's, <see cref="ClickHouseTcpConnection.IsReusable"/>, asked at both ends of an
    /// operation as the pool asks at both ends of a lease: on release it catches what the operation did to the
    /// connection, before the next one what happened while the session sat idle. It latches because the causes are
    /// final. A false positive costs more here than in the pool — a reconnect there, the temporary tables and settings
    /// a session exists for here — so a session dying for no visible reason under TLS is the first thing to suspect
    /// (see <see cref="ClickHouseTcpConnection.IsReusable"/> on late session tickets).
    /// </remarks>
    private bool HasLostTheConnection()
    {
        faulted = faulted || !lease.Connection.IsReusable;
        return faulted;
    }

    /// <summary>Closes the pinned connection and returns the lease, in that order.</summary>
    /// <remarks>
    /// The order is the whole point: the pool decides a returned connection's fate from its state, so terminating has
    /// to come first or the pool would keep one carrying this session's temporary tables and settings. A throw from
    /// <c>Terminate</c> is swallowed because the connection is discarded either way, and letting it escape would
    /// leave the pool a slot short.
    /// </remarks>
    private async ValueTask ReturnAsync()
    {
        try
        {
            lease.Connection.Terminate();
        }
        catch (Exception e) when (e is not OutOfMemoryException and not StackOverflowException)
        {
        }
        finally
        {
            await lease.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The lease one operation runs under. Disposing it hands the connection back to the session rather than to the
    /// pool, which is what keeps it pinned across operations.
    /// </summary>
    private sealed class PinnedLease : IConnectionLease
    {
        private readonly PinnedConnectionSource source;
        private int released;

        internal PinnedLease(PinnedConnectionSource source) => this.source = source;

        public ClickHouseTcpConnection Connection => source.lease.Connection;

        public ValueTask DisposeAsync()
            => Interlocked.Exchange(ref released, 1) == 0 ? source.ReleaseAsync() : default;
    }
}
