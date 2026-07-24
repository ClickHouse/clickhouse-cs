using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// A minimal connection source that holds a single connection and serializes access to it. Rents wait on a
/// gate so the one non-thread-safe connection carries at most one in-flight operation at a time; a connection
/// left terminated by a failed operation is discarded and redialed on the next rent. This is the interim stand
/// in for a real connection pool: it satisfies <see cref="IConnectionSource"/> so a pooled implementation can
/// replace it without touching the client.
/// </summary>
internal sealed class SingleConnectionSource : IConnectionSource
{
    private readonly ClickHouseTcpClientOptions options;
    private readonly SemaphoreSlim gate = new(1, 1);
    private ClickHouseTcpConnection current;
    private int disposed;

    /// <summary>Initializes the source over the client's options; no connection is opened until the first rent.</summary>
    /// <param name="options">The validated client options (endpoint, credentials, dial timeout).</param>
    public SingleConnectionSource(ClickHouseTcpClientOptions options)
    {
        this.options = options;
    }

    /// <inheritdoc/>
    public async ValueTask<IConnectionLease> RentAsync(CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        // From here the gate is held; every path must either return a lease (which releases it on dispose) or
        // release it before throwing, or the source deadlocks.
        try
        {
            if (Volatile.Read(ref disposed) != 0)
            {
                throw new ObjectDisposedException(nameof(SingleConnectionSource));
            }

            if (current is null || current.State == TcpConnectionState.Terminated)
            {
                // Discard a connection a prior operation terminated, then dial a fresh one. A terminated
                // connection has already closed its transport, so nothing beyond dropping the reference is
                // needed before redialing.
                current = null;
                current = await DialAsync(cancellationToken).ConfigureAwait(false);
            }

            return new Lease(this, current);
        }
        catch
        {
            gate.Release();
            throw;
        }
    }

    /// <summary>Dials and hands back a ready connection, bounding connect + handshake with the dial timeout.</summary>
    private async ValueTask<ClickHouseTcpConnection> DialAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(options.DialTimeout);
        try
        {
            return await ClickHouseTcpConnection.ConnectAsync(
                options.Host, options.Port, options.ToHandshakeParameters(), linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && linked.IsCancellationRequested)
        {
            // The linked token, not the caller's, fired: the dial deadline elapsed. Surface it as a timeout so a
            // hung connect is distinguishable from a caller cancellation.
            throw new TimeoutException(
                $"Connecting to {options.Host}:{options.Port} timed out after {options.DialTimeout.TotalSeconds:0.###}s (DialTimeout).");
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        // Idempotent: only the first caller tears the source down. Marking disposed before taking the gate
        // means a rent already parked on the gate, once admitted, observes disposed and fails cleanly rather
        // than handing out a connection about to be closed.
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        // Take the gate so disposal waits for any in-flight operation to return its lease before closing the
        // connection out from under it.
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (current is not null)
            {
                await current.DisposeAsync().ConfigureAwait(false);
                current = null;
            }
        }
        finally
        {
            gate.Release();
        }

        // The gate is deliberately not disposed: SemaphoreSlim holds no unmanaged resource unless its
        // AvailableWaitHandle is accessed (it never is here), and disposing it could fault a rent still parked
        // on WaitAsync. Leaving it undisposed lets such a waiter wake, observe disposed, and throw cleanly.
    }

    /// <summary>
    /// The lease handed to a caller. Its dispose releases the gate exactly once (guarded so a double dispose is
    /// a no-op) and, when the returned connection is terminated, clears the source's held connection so the next
    /// rent redials.
    /// </summary>
    private sealed class Lease : IConnectionLease
    {
        private readonly SingleConnectionSource source;
        private int returned;

        public Lease(SingleConnectionSource source, ClickHouseTcpConnection connection)
        {
            this.source = source;
            Connection = connection;
        }

        public ClickHouseTcpConnection Connection { get; }

        public ValueTask DisposeAsync()
        {
            // Return exactly once even if the caller disposes twice (e.g. an await using plus an explicit call).
            if (Interlocked.Exchange(ref returned, 1) != 0)
            {
                return default;
            }

            // A connection a failed operation terminated must not be leased again; drop it so the next rent
            // dials a fresh one. A still-ready connection stays as the source's held connection for reuse.
            if (Connection.State == TcpConnectionState.Terminated && ReferenceEquals(source.current, Connection))
            {
                source.current = null;
            }

            source.gate.Release();
            return default;
        }
    }
}
