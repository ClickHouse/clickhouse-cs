using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// Holds one connection lease for a session and lends it to one operation at a time.
/// </summary>
/// <remarks>
/// State changes are guarded by <c>gate</c> so renting cannot race with disposal. Transport teardown and lease
/// return run outside the lock. The connection is closed on disposal to keep session state out of the pool.
/// </remarks>
internal sealed class PinnedConnectionSource : IConnectionSource
{
    // A string literal avoids CHTCP0001 when referencing the experimental session type.
    private const string SessionTypeName = "ClickHouseTcpSession";

    private readonly IConnectionLease lease;
    private readonly object gate = new();

    // True while an operation holds the connection.
    private bool busy;

    // Permanently set by HasLostTheConnection when the connection becomes unusable.
    private bool faulted;

    private bool disposed;

    // Guards the single pool return; returning twice would over-release the pool's permit.
    private bool returned;

    /// <summary>Pins the connection behind a lease for the lifetime of this source.</summary>
    /// <param name="lease">The lease owned by this source.</param>
    internal PinnedConnectionSource(IConnectionLease lease) => this.lease = lease;

    /// <summary>
    /// Whether the session is undisposed and the connection is not known to be unusable.
    /// Skips the reusability check during an active operation.
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

            // A busy connection is not reusable; reject concurrency before testing connection health.
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

        // Disposing this lease releases the operation, not the session's underlying lease.
        return new ValueTask<IConnectionLease>(new PinnedLease(this));
    }

    /// <summary>
    /// Ends the session, closing an idle connection or aborting an active operation. Idempotent.
    /// </summary>
    /// <remarks>
    /// An active operation may still use connection buffers, so only its transport is aborted here. The operation
    /// returns the lease through <see cref="ReleaseAsync"/> when it exits. An undisposed enumerator suspended at
    /// a <c>yield</c> cannot be resumed by aborting the transport and keeps the pool slot until client disposal.
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

            // No return can have started before disposal; reserve it here only if idle.
            abort = busy;
            returned = !abort;
        }

        if (abort)
        {
            // Unblock pending I/O without releasing buffers the operation may still use.
            lease.Connection.AbortTransport();
            return;
        }

        await ReturnAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Releases an operation's claim. Keeps the connection pinned unless the session has been disposed.
    /// </summary>
    private ValueTask ReleaseAsync()
    {
        lock (gate)
        {
            busy = false;

            if (!disposed)
            {
                // Record operation failures for IsOpen and subsequent rents.
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
    /// Checks connection reusability and permanently records failure.
    /// </summary>
    /// <returns>True if the connection is unusable or was previously found unusable.</returns>
    /// <remarks>
    /// Call under <c>gate</c> only when idle: <see cref="ClickHouseTcpConnection.IsReusable"/> is false during an
    /// operation. Checks before and after operations detect both idle disconnects and operation failures.
    /// The same TLS liveness limitations as <see cref="ClickHouseTcpConnection.IsReusable"/> apply.
    /// </remarks>
    private bool HasLostTheConnection()
    {
        faulted = faulted || !lease.Connection.IsReusable;
        return faulted;
    }

    /// <summary>Closes the pinned connection and returns the lease, in that order.</summary>
    /// <remarks>
    /// Terminate before returning so the pool cannot reuse session state. Suppress non-fatal termination errors
    /// and always return the lease to release the pool slot.
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
    /// An operation's lease. Disposal releases it to the session rather than the pool.
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
