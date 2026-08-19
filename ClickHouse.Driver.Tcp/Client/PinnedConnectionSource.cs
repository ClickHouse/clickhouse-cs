using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// A connection source that hands out the same connection every time: the one a session pinned. It holds a lease
/// on the underlying source for its whole lifetime, so the connection is never anyone else's in between, which is
/// what carries a session's temporary tables and settings from one operation to the next.
///
/// <para>
/// It is the same seam the pool is, so a session runs the ordinary client code over it and needs no session-aware
/// operations of its own. What it adds is the bookkeeping that pinning brings with it: exactly one operation at a
/// time, a connection lost mid-session reported as such rather than as a wall of transport errors, and, on
/// disposal, the connection closed instead of handed back.
/// </para>
/// </summary>
/// <remarks>
/// Every state change is made under <c>gate</c>, and the slow parts — closing the socket, returning the lease —
/// are done outside it. Contention is not the reason for the lock: the states interlock (a rent must not start
/// while disposal is deciding whether the connection is idle, and vice versa), and one lock is cheaper to reason
/// about than the interlocked pairs that would replace it. The path it guards runs once per operation.
/// </remarks>
internal sealed class PinnedConnectionSource : IConnectionSource
{
    /// <summary>
    /// The type an <see cref="ObjectDisposedException"/> from here names: the session the caller holds, not this
    /// internal source. Spelled out rather than taken from <c>nameof</c>, because CHTCP0001 objects to naming the
    /// <c>[Experimental]</c> session type from ordinary internal code.
    /// </summary>
    private const string SessionTypeName = "ClickHouseTcpSession";

    private readonly IConnectionLease lease;
    private readonly object gate = new();

    // An operation holds the connection: rented and not yet released.
    private bool busy;

    // The connection was lost, so nothing more can run on it. Latched by HasLostTheConnection, which is asked at
    // both ends of an operation.
    private bool faulted;

    private bool disposed;

    // The lease has been given back to the pool. Guards against doing it twice, which would release a permit the
    // source does not hold and let the pool run one operation over its limit.
    private bool returned;

    /// <summary>Pins the connection behind a lease for the lifetime of this source.</summary>
    /// <param name="lease">The lease to hold, taken from the client's own source.</param>
    internal PinnedConnectionSource(IConnectionLease lease) => this.lease = lease;

    /// <summary>
    /// Whether the pinned connection can still carry an operation: the source is neither disposed nor holding a
    /// connection that was lost.
    /// </summary>
    /// <remarks>
    /// A session running an operation is open on what is known so far, and is not tested further: a connection
    /// mid-operation is not reusable by definition, so asking would condemn every session that is busy.
    /// </remarks>
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

            // Tested before the connection is, and not only to report the likelier mistake first: a connection
            // carrying an operation is not reusable, so asking the question below while one runs would condemn a
            // session that is merely busy.
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

        // The pinned lease is a token for one operation, not a second claim on the connection: it releases the
        // gate above rather than the underlying lease, which this source holds until it is disposed.
        return new ValueTask<IConnectionLease>(new PinnedLease(this));
    }

    /// <summary>
    /// Ends the session: closes the pinned connection and gives the lease back, so the pool accounts for the slot
    /// again and opens a fresh connection for the next caller. Idempotent.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The connection is closed rather than returned reusable, because a session leaves its state on it and the
    /// next caller must not inherit that. Disposal during an operation cannot close it that way — the buffers the
    /// operation is reading into would go back to the pool underneath it — so it aborts the transport instead,
    /// which is safe to race with, and leaves the return to the operation's own unwinding.
    /// </para>
    /// <para>
    /// <b>That hands the return to something this call cannot make happen.</b> Aborting frees an operation parked
    /// on the socket, and that is the case it is for. An operation parked anywhere else does not resume: a
    /// streamed result whose consumer stopped advancing is suspended at its <c>yield</c>, not on a read, so
    /// closing the socket reaches nothing and the lease is never returned — the pool is short a slot until the
    /// client itself is disposed. Disposal still returns promptly and reports nothing, because the alternative is
    /// worse: returning the lease here would let the pool terminate a connection whose buffers a live operation
    /// still points at. The client has the same hazard for an abandoned enumerator; a session cannot do better,
    /// only say so.
    /// </para>
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

            // `returned` cannot be set yet: only this method and ReleaseAsync's disposed branch set it, and both
            // are reachable only once `disposed` is, which the early return above has just ruled out.
            abort = busy;
            returned = !abort;
        }

        if (abort)
        {
            // Marks the connection final, so the pool cannot reuse it whatever happens next, and frees an
            // operation parked on a read that will never arrive.
            lease.Connection.AbortTransport();
            return;
        }

        await ReturnAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Takes the connection back from a finished operation. While the session is open the connection stays pinned,
    /// and only its usability is recorded; a session disposed mid-operation is completed here instead, this being
    /// the first moment the connection is idle enough to close.
    /// </summary>
    private ValueTask ReleaseAsync()
    {
        lock (gate)
        {
            busy = false;

            if (!disposed)
            {
                // Asked now as well as before the next operation, so an operation that broke the connection is
                // reported as such even if nothing runs on the session again — which is what IsOpen reports.
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
    /// Whether the pinned connection has been lost, latching the answer once it has. Call under <c>gate</c>, and
    /// only when no operation is running: mid-operation the connection is legitimately not reusable, so the
    /// question does not apply and the callers both exclude that case.
    /// </summary>
    /// <returns>True when nothing more can run on this session.</returns>
    /// <remarks>
    /// <para>
    /// The predicate is the pool's, <see cref="ClickHouseTcpConnection.IsReusable"/>, and it is asked at both ends
    /// of an operation for the reason the pool asks at both ends of a lease. Asked on release it catches what the
    /// operation did to the connection: terminated it, or left it out of step by not reading the whole result.
    /// Asked before the next operation it catches what happened while the session sat idle — a proxy or the server
    /// dropping a connection nobody was using, which is the case release cannot see, because at release nothing
    /// has had time to happen yet.
    /// </para>
    /// <para>
    /// The answer latches because the causes are all final, and because the test must not be able to change its
    /// mind between <see cref="IsOpen"/> saying the session is finished and the next operation acting on it.
    /// </para>
    /// <para>
    /// <b>A false positive costs more here than it does in the pool.</b> The test polls the raw socket, and
    /// <see cref="ClickHouseTcpConnection.IsReusable"/> records that under TLS a record carrying no application
    /// data — a late session ticket — can make a healthy connection look readable. The pool pays a reconnect for
    /// that; a session pays the temporary tables and settings it exists for. The risk is the same one the pool
    /// already runs and has not been seen against ClickHouse, so it is accepted rather than worked around, but a
    /// session dying for no visible reason under TLS is the first thing to suspect here.
    /// </para>
    /// </remarks>
    private bool HasLostTheConnection()
    {
        faulted = faulted || !lease.Connection.IsReusable;
        return faulted;
    }

    /// <summary>Closes the pinned connection and returns the lease, in that order.</summary>
    /// <remarks>
    /// The order is the whole point: the pool decides a returned connection's fate from its state, so it has to be
    /// terminated before it is handed back or the pool would keep a connection carrying this session's temporary
    /// tables and settings. Terminating can throw, from a socket that fails to close, and there is nothing useful
    /// to do about it — the connection is being discarded either way, and letting it escape would fail a disposal
    /// that has otherwise done its job and leave the pool a slot short.
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
    /// The lease one operation runs under. Disposing it hands the connection back to the session rather than to
    /// the pool, which is what keeps the connection pinned across operations.
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
