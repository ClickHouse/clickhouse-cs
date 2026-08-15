using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// A pool of native-protocol connections. One connection carries one query — the protocol has no multiplexing —
/// so the pool is what lets a single client run operations concurrently: it hands out up to
/// <see cref="ClickHouseTcpClientOptions.MaxPoolSize"/> connections at once and queues callers beyond that.
///
/// <para>
/// A checked-out connection is exclusively the lease-holder's, which is what keeps the non-thread-safe
/// connection safe under a client that is itself thread-safe. The one exception is disposal, which aborts the
/// transport of anything still out once its drain deadline passes. Returned connections are kept for reuse
/// unless they are terminated, out of step with the server, or too old; retired ones are closed and never
/// handed out again.
/// </para>
/// </summary>
/// <remarks>
/// Waiting callers are <b>not</b> served in a guaranteed order. The wait is a <see cref="SemaphoreSlim"/>, which
/// makes no ordering promise, and building a fair queue on top would cost more than it is worth here: a caller
/// that needs operations to happen in a set order has to sequence them itself, since two operations that overlap
/// on different connections have no ordering at the server either.
/// </remarks>
internal sealed class ConnectionPool : IConnectionSource
{
    /// <summary>
    /// The type an <see cref="ObjectDisposedException"/> from here names — the client the caller actually holds,
    /// not this internal pool. Spelled out rather than taken from <c>nameof</c>, because naming the
    /// <c>[Experimental]</c> client type from ordinary internal code is what CHTCP0001 objects to.
    /// </summary>
    private const string ClientTypeName = "ClickHouseTcpClient";

    /// <summary>The floor on the sweep period, so a short idle timeout cannot make the sweep spin.</summary>
    private static readonly TimeSpan MinSweepInterval = TimeSpan.FromSeconds(1);

    /// <summary>The ceiling on the sweep period, so a long lifetime still releases idle sockets promptly.</summary>
    private static readonly TimeSpan MaxSweepInterval = TimeSpan.FromSeconds(30);

    private readonly ClickHouseTcpClientOptions options;
    private readonly IConnectionFactory factory;
    private readonly TimeProvider time;
    private readonly SemaphoreSlim permits;
    private readonly ITimer sweeper;

    // Idle connections, ordered by the time they were returned: index 0 was returned first and so has been idle
    // longest. Only ever appended to, so that order holds under either reuse policy. Guarded by `gate`.
    private readonly List<PooledConnection> idle = [];

    // The connections currently checked out. The pool cannot reach a leased connection through `idle`, so
    // without this set a lease that is never returned would leave its socket open with nothing able to close
    // it — not even disposal. Guarded by `gate`.
    private readonly HashSet<PooledConnection> leased = [];

    private readonly object gate = new();

    private int disposed;

    /// <summary>Creates a pool over the client's options, opening no connection until the first rent.</summary>
    /// <param name="options">The validated client options.</param>
    internal ConnectionPool(ClickHouseTcpClientOptions options)
        : this(options, new TcpConnectionFactory(options), TimeProvider.System)
    {
    }

    /// <summary>Creates a pool over an explicit factory and clock. The test seam.</summary>
    /// <param name="options">The validated client options.</param>
    /// <param name="factory">Opens the connections the pool hands out.</param>
    /// <param name="time">The clock age, idleness, and the sweep run against.</param>
    internal ConnectionPool(ClickHouseTcpClientOptions options, IConnectionFactory factory, TimeProvider time)
    {
        this.options = options;
        this.factory = factory;
        this.time = time;
        permits = new SemaphoreSlim(options.MaxPoolSize, options.MaxPoolSize);

        TimeSpan period = SweepInterval(options);
        sweeper = period == TimeSpan.Zero
            ? null
            : time.CreateTimer(static state => ((ConnectionPool)state).SweepQuietly(), this, period, period);
    }

    /// <summary>The number of connections currently sitting idle. For tests and diagnostics.</summary>
    internal int IdleCount
    {
        get
        {
            lock (gate)
            {
                return idle.Count;
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IConnectionLease> RentAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await AcquirePermitAsync(cancellationToken).ConfigureAwait(false);

        // From here a permit is held; every path must either return a lease (which releases it on dispose) or
        // release it before throwing, or the pool loses a slot for good.
        try
        {
            // Disposal may have started while this caller waited for the permit.
            ThrowIfDisposed();

            PooledConnection connection = TakeReusableIdle()
                ?? new PooledConnection(await factory.CreateAsync(cancellationToken).ConfigureAwait(false), time);

            connection.OnRented();
            lock (gate)
            {
                // Re-checked here rather than only above, and under the same lock as the add: opening a
                // connection can take up to DialTimeout, long enough for disposal to run its whole drain in
                // between. Because disposal sets the flag before it ever takes this lock, and empties `leased`
                // strictly after that, seeing 0 here proves this add lands before the drain rather than after it
                // — where nothing would ever close the connection.
                if (Volatile.Read(ref disposed) != 0)
                {
                    connection.Close();
                    throw new ObjectDisposedException(ClientTypeName);
                }

                leased.Add(connection);
            }

            return new Lease(this, connection);
        }
        catch
        {
            // Only the permit is undone. Every throw either precedes the add to `leased` or, in the disposed
            // case above, removes what it added — so there is nothing else outstanding to clean up here.
            permits.Release();
            throw;
        }
    }

    /// <summary>
    /// Closes the pool: every idle connection at once, then the ones still out as their operations finish.
    /// Waits up to <see cref="ClickHouseTcpClientOptions.PoolTimeout"/> for that, after which whatever is left
    /// is aborted where it stands — so an operation still running then fails rather than holding disposal open
    /// for good.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Idempotent: only the first caller tears the pool down. Marking disposed first means a rent already
        // waiting for a permit, once admitted, observes it and fails cleanly rather than taking a connection
        // that is about to be closed.
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        if (sweeper is not null)
        {
            // Awaited rather than disposed synchronously so a sweep already running finishes before the drain.
            await sweeper.DisposeAsync().ConfigureAwait(false);
        }

        // Nothing can re-enter the idle set after this: `Return` re-reads `disposed` under `gate`, and the
        // Interlocked.Exchange above happens before this lock is taken, so a returner either added before the
        // drain (and is closed by it) or observes the flag and closes its own connection.
        CloseAll(TakeAllIdle());

        // Wait for the operations still running to give their connections back — each one, finding the pool
        // disposed, closes rather than pools it — by acquiring every permit. Bounded by PoolTimeout for the
        // same reason a checkout is: an operation that never releases its connection (an `await foreach` whose
        // enumerator is never disposed) must not turn disposal into a hang.
        using var drainDeadline = new CancellationTokenSource(options.PoolTimeout);
        try
        {
            for (int i = 0; i < options.MaxPoolSize; i++)
            {
                await permits.WaitAsync(drainDeadline.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // The drain deadline elapsed: at least one operation did not give its connection back. Abort what is
            // still out, since nothing else can — the pool holds the only other reference to it, and the caller
            // has evidently lost theirs. Aborting closes the transport only, which frees an operation parked on
            // a read that will never arrive without touching the buffers that operation may still be using.
            foreach (PooledConnection connection in TakeAllLeased() ?? [])
            {
                connection.Abort();
            }
        }

        // The semaphore is deliberately not disposed: it holds no unmanaged resource unless its
        // AvailableWaitHandle is accessed (it never is), and disposing it would fault a rent still waiting on it
        // instead of letting that caller wake and report the pool as disposed.
    }

    /// <summary>
    /// The interval between sweeps for the given options, or <see cref="TimeSpan.Zero"/> when nothing can
    /// expire and no sweep is needed. A quarter of the shortest limit in force, so an expiry is found well
    /// inside it, clamped at both ends so a short limit cannot make the sweep spin and a long one still
    /// releases sockets in reasonable time.
    /// </summary>
    /// <param name="options">The client options holding the lifetime and idle limits.</param>
    /// <returns>The sweep period, or zero for no sweep.</returns>
    internal static TimeSpan SweepInterval(ClickHouseTcpClientOptions options)
    {
        TimeSpan shortest = TimeSpan.Zero;
        if (options.IdleTimeout > TimeSpan.Zero)
        {
            shortest = options.IdleTimeout;
        }

        if (options.MaxConnectionLifetime > TimeSpan.Zero && (shortest == TimeSpan.Zero || options.MaxConnectionLifetime < shortest))
        {
            shortest = options.MaxConnectionLifetime;
        }

        if (shortest == TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        TimeSpan period = shortest / 4;
        if (period < MinSweepInterval)
        {
            return MinSweepInterval;
        }

        return period > MaxSweepInterval ? MaxSweepInterval : period;
    }

    /// <summary>
    /// Runs a sweep, swallowing anything it throws. What the timer calls. A timer callback runs on a thread-pool
    /// thread with no one to catch for it, so an exception escaping here would take the process down — and a
    /// sweep that failed to close one socket is not worth that. There is nothing to log to yet; observability
    /// lands in Epic P.
    /// </summary>
    internal void SweepQuietly()
    {
        try
        {
            Sweep();
        }
        catch (Exception e) when (e is not (OutOfMemoryException or StackOverflowException))
        {
        }
    }

    /// <summary>
    /// Closes the idle connections that have expired: every one past its lifetime, whatever the count, then the
    /// longest-idle ones down to <see cref="ClickHouseTcpClientOptions.MinPoolSize"/>. Runs on the sweep timer;
    /// called directly by tests.
    /// </summary>
    internal void Sweep()
    {
        if (Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        List<PooledConnection> reaped = null;
        lock (gate)
        {
            for (int i = idle.Count - 1; i >= 0; i--)
            {
                if (idle[i].IsPastLifetime(options.MaxConnectionLifetime))
                {
                    (reaped ??= []).Add(idle[i]);
                    idle.RemoveAt(i);
                }
            }

            // Index 0 has been idle longest, so trimming from the front retires the coldest connections and
            // keeps the ones most likely to be wanted next. Idleness only decreases along the list, so the
            // first entry still inside the timeout ends the trim.
            while (options.IdleTimeout > TimeSpan.Zero
                && idle.Count > options.MinPoolSize
                && idle[0].IdleFor >= options.IdleTimeout)
            {
                (reaped ??= []).Add(idle[0]);
                idle.RemoveAt(0);
            }
        }

        CloseAll(reaped);
    }

    /// <summary>
    /// Takes the next idle connection that can still be used, closing any that cannot on the way. Returns null
    /// when the idle set holds nothing usable, leaving the caller to open a connection.
    /// </summary>
    /// <returns>A usable idle connection, or null.</returns>
    /// <remarks>
    /// This loop is the pool's whole answer to a connection that died while idle: rather than counting retries,
    /// it discards candidates until one passes or the set is empty, and then a fresh connection is opened. A
    /// failure to open <i>that</i> connection is reported to the caller rather than retried, so a server that is
    /// down is reported as down instead of being hidden behind the pool timeout.
    /// </remarks>
    private PooledConnection TakeReusableIdle()
    {
        while (true)
        {
            PooledConnection candidate;
            lock (gate)
            {
                if (idle.Count == 0)
                {
                    return null;
                }

                int index = options.PoolReusePolicy == ClickHouseTcpPoolReusePolicy.Fifo ? 0 : idle.Count - 1;
                candidate = idle[index];
                idle.RemoveAt(index);
            }

            // Outside the lock: closing a socket should not hold up another caller's checkout.
            if (candidate.CanBeRented(options.MaxConnectionLifetime))
            {
                return candidate;
            }

            candidate.Close();
        }
    }

    /// <summary>Waits for a free slot, honouring <see cref="ClickHouseTcpClientOptions.PoolTimeout"/>.</summary>
    /// <param name="cancellationToken">A token to observe while waiting.</param>
    /// <exception cref="TimeoutException">No connection became free within the pool timeout.</exception>
    private async ValueTask AcquirePermitAsync(CancellationToken cancellationToken)
    {
        if (await permits.WaitAsync(options.PoolTimeout, cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        // Disposal takes every permit and keeps them, so it is a likelier cause than genuine exhaustion here.
        ThrowIfDisposed();

        throw new TimeoutException(
            string.Format(
                CultureInfo.InvariantCulture,
                "All {0} connections are in use and none became free within {1:0.###}s (PoolTimeout). Raise MaxPoolSize or PoolTimeout, or run fewer operations at once. A connection is also held by an unfinished result: enumerate a StreamAsync/QueryAsync result to the end, or dispose its enumerator, to give it back.",
                options.MaxPoolSize,
                options.PoolTimeout.TotalSeconds));
    }

    /// <summary>
    /// Takes a returned connection back: kept for reuse when it is still usable and the pool is still open,
    /// closed otherwise. Releases the slot last, so a waiter woken by the release finds the connection already
    /// in the idle set.
    /// </summary>
    /// <param name="connection">The connection being returned.</param>
    private void Return(PooledConnection connection)
    {
        // The permit must come back whatever happens below, or the pool silently shrinks by one slot for the
        // rest of its life and later callers are told they are running too much work at once.
        try
        {
            // A connection a failed or abandoned operation terminated is never recycled; nor is one past its age
            // limit, which the next checkout would retire anyway — closing now frees the socket sooner.
            bool reusable = connection.Connection.State == TcpConnectionState.Ready
                && !connection.IsPastLifetime(options.MaxConnectionLifetime);

            bool pooled = false;
            lock (gate)
            {
                leased.Remove(connection);

                // The disposed check belongs under the lock, because disposal marks the pool disposed and then
                // drains the idle set: an entry added after that drain, by a check made outside the lock, would
                // never be closed. Stamping the idle clock here too keeps the list ordered by that stamp, which
                // is what lets the sweep trim the coldest connections and stop at the first live one.
                if (reusable && Volatile.Read(ref disposed) == 0)
                {
                    connection.OnReturned();
                    idle.Add(connection);
                    pooled = true;
                }
            }

            if (!pooled)
            {
                connection.Close();
            }
        }
        finally
        {
            permits.Release();
        }
    }

    /// <summary>Empties the idle set and returns what it held.</summary>
    private List<PooledConnection> TakeAllIdle() => TakeAll(idle);

    /// <summary>Empties the leased set and returns what it held.</summary>
    private List<PooledConnection> TakeAllLeased() => TakeAll(leased);

    private List<PooledConnection> TakeAll(ICollection<PooledConnection> connections)
    {
        lock (gate)
        {
            if (connections.Count == 0)
            {
                return null;
            }

            var taken = new List<PooledConnection>(connections);
            connections.Clear();
            return taken;
        }
    }

    /// <summary>Closes each connection, if any. Always called outside the lock.</summary>
    private static void CloseAll(List<PooledConnection> connections)
    {
        if (connections is null)
        {
            return;
        }

        foreach (PooledConnection connection in connections)
        {
            connection.Close();
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref disposed) != 0)
        {
            throw new ObjectDisposedException(ClientTypeName);
        }
    }

    /// <summary>
    /// The lease handed to a caller. Its dispose returns the connection exactly once (guarded so a double
    /// dispose is a no-op), letting the pool decide whether to keep or close it.
    /// </summary>
    private sealed class Lease : IConnectionLease
    {
        private readonly ConnectionPool pool;
        private readonly PooledConnection pooled;
        private int returned;

        internal Lease(ConnectionPool pool, PooledConnection pooled)
        {
            this.pool = pool;
            this.pooled = pooled;
        }

        public ClickHouseTcpConnection Connection => pooled.Connection;

        public ValueTask DisposeAsync()
        {
            // Return exactly once even if the caller disposes twice (e.g. an await using plus an explicit call).
            if (Interlocked.Exchange(ref returned, 1) == 0)
            {
                pool.Return(pooled);
            }

            return default;
        }
    }
}
