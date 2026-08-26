using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// A pool of native-protocol connections. One connection carries one query, the protocol having no multiplexing,
/// so the pool is what lets a single client run operations concurrently: it hands out up to
/// <see cref="ClickHouseTcpClientOptions.MaxPoolSize"/> connections at once and queues callers beyond that.
///
/// <para>
/// A checked-out connection belongs to its lease-holder alone, which is how a connection that is not thread-safe
/// stays safe under a client that is. Disposal is the one exception: it aborts the transport of anything still out
/// once the drain deadline passes. A returned connection is kept for reuse unless it is terminated, out of step
/// with the server, too old, or has sat unused too long. A retired one is closed and never handed out again.
/// </para>
/// </summary>
/// <remarks>
/// Waiting callers are <b>not</b> served in a guaranteed order, because the wait is a
/// <see cref="SemaphoreSlim"/>, which makes no ordering promise. A caller that needs operations to happen in a set
/// order must sequence them itself. Two operations that overlap on different connections have no ordering at the
/// server either.
/// </remarks>
internal sealed class ConnectionPool : IConnectionSource
{
    /// <summary>
    /// The type an <see cref="ObjectDisposedException"/> from here names: the client the caller holds, not this
    /// internal pool. Spelled out rather than taken from <c>nameof</c>, because CHTCP0001 objects to naming the
    /// <c>[Experimental]</c> client type from ordinary internal code.
    /// </summary>
    private const string ClientTypeName = "ClickHouseTcpClient";

    /// <summary>
    /// How many sweeps fall inside the shortest limit in force when the period is derived. This bounds how long
    /// past its limit a connection can sit before a sweep finds it: at four, a quarter of the limit. Any value
    /// above one works. <see cref="ClickHouseTcpClientOptions.SweepInterval"/> replaces the derived period
    /// outright.
    /// </summary>
    private const int SweepsPerLimit = 4;

    /// <summary>The floor on the derived sweep period, so a short idle timeout cannot make the sweep spin.</summary>
    private static readonly TimeSpan MinSweepInterval = TimeSpan.FromSeconds(1);

    /// <summary>The ceiling on the derived sweep period, so a long lifetime still releases idle sockets promptly.</summary>
    private static readonly TimeSpan MaxSweepInterval = TimeSpan.FromSeconds(30);

    private readonly ClickHouseTcpClientOptions options;
    private readonly IConnectionFactory factory;
    private readonly TimeProvider time;
    private readonly SemaphoreSlim permits;
    private readonly ITimer sweeper;

    // Idle connections, ordered by the time they were returned: index 0 was returned first and so has been idle
    // longest. Only ever appended to, which holds that order and makes either end of the list the reuse policy's
    // choice. Guarded by `gate`.
    private readonly List<PooledConnection> idle = new();

    // The connections currently checked out. The pool cannot reach a leased connection through `idle`, so without
    // this set a lease that was never returned would leave its socket open with nothing able to close it, disposal
    // included. Guarded by `gate`.
    private readonly HashSet<PooledConnection> leased = new();

    // Scratch space for the connections a sweep retires, reused so a sweep that finds something allocates
    // nothing. Filled under `gate` and drained outside it, which is safe only because `sweeping` admits one
    // sweep at a time. Left empty between sweeps, so it roots no connection it has already closed.
    private readonly List<PooledConnection> reaped = new();

    private readonly object gate = new();

    // Cancelled when the pool is disposed, so a dial in flight, a checkout's or a top-up's, gives up at once rather
    // than holding a permit the drain is waiting for until DialTimeout elapses.
    private readonly CancellationTokenSource shutdown = new();

    // Completed when the first caller's teardown finishes. A second, concurrent DisposeAsync waits on this rather
    // than returning at once, so awaiting it means the pool is closed and not merely closing.
    private readonly TaskCompletionSource teardown = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Capacity taken but not yet filed in `idle` or `leased`: a dial in flight, or a connection lifted out of
    // `idle` by a checkout that has not yet recorded it. Counting these keeps a top-up from opening past
    // MaxPoolSize while they are in the air. Guarded by `gate`.
    private int pending;

    // Factory use has a lifetime of its own. A dial starts before it has a connection the pool can put in a set,
    // so a drain that times out cannot tell it apart from an established lease by counting permits alone. The
    // counter keeps the factory alive for those dials; once teardown requests disposal, the last one out releases
    // it. Guarded by `gate`.
    private int activeDials;
    private bool factoryDisposalRequested;
    private bool factoryDisposed;

    private int disposed;

    // 1 while a top-up is running. Only one at a time, or two sweeps race to fill the same gap.
    private int refilling;

    // 1 while a sweep is running. Only one at a time, because sweeps share the `reaped` buffer.
    private int sweeping;

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

    /// <summary>
    /// The top-up the most recent sweep started, or null when it started none. Only tests read it: a top-up is
    /// deliberately something no caller waits on.
    /// </summary>
    internal Task LastRefill { get; private set; }

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

        // The slot is taken for the whole checkout, not just for a dial: a connection lifted out of `idle` is in
        // no set until it reaches `leased`, and a top-up that counted the sets alone would read that gap as spare
        // capacity and open one connection too many.
        lock (gate)
        {
            pending++;
        }

        // From here a permit and a slot are both held; every path must either return a lease (which releases the
        // permit on dispose) or give both back before throwing, or the pool loses capacity for good.
        bool reserved = true;
        try
        {
            // Disposal may have started while this caller waited for the permit.
            ThrowIfDisposed();

            PooledConnection connection = TakeReusableIdle()
                ?? new PooledConnection(await DialAsync(cancellationToken).ConfigureAwait(false), time);

            connection.OnRented();
            lock (gate)
            {
                // The slot gives way to the entry in `leased` under one lock, so the total this checkout
                // contributes never dips in between and lets a top-up through.
                pending--;
                reserved = false;

                // Re-checked here as well as above, and under the same lock as the add: opening a connection can
                // take up to DialTimeout, long enough for disposal to run its whole drain in between. Disposal sets
                // the flag before it takes this lock and empties `leased` strictly after that, so seeing 0 here
                // proves this add lands before the drain. An add after the drain would never be closed.
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
            // Only the permit and the slot are undone. Every throw either precedes the add to `leased` or, in the
            // disposed case above, has already closed the connection it opened. Nothing else is outstanding.
            if (reserved)
            {
                lock (gate)
                {
                    pending--;
                }
            }

            permits.Release();
            throw;
        }
    }

    /// <summary>
    /// Closes the pool: every idle connection at once, then the ones still out as their operations finish. Waits up
    /// to <see cref="ClickHouseTcpClientOptions.PoolTimeout"/> for that, then aborts whatever is left. An operation
    /// still running at that point fails, rather than holding disposal open indefinitely.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Idempotent: only the first caller tears the pool down. Marking disposed first means a rent already
        // waiting for a permit, once admitted, observes it and fails cleanly rather than taking a connection
        // that is about to be closed.
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            // Waited on rather than returned from at once. The first caller can still be draining, and its
            // connections are open and may yet be aborted, so returning here would report a closing pool as closed.
            await teardown.Task.ConfigureAwait(false);
            return;
        }

        try
        {
            await TearDownAsync().ConfigureAwait(false);
        }
        finally
        {
            // Completed rather than faulted even when the teardown threw: that failure belongs to the caller who
            // caused it, and a task no one may await must not carry an unobserved exception.
            teardown.TrySetResult();
        }
    }

    /// <summary>The teardown itself, run once, by whichever caller won the disposal race.</summary>
    private async Task TearDownAsync()
    {
        if (sweeper is not null)
        {
            // Awaited rather than disposed synchronously so a sweep already running finishes before the drain.
            await sweeper.DisposeAsync().ConfigureAwait(false);
        }

        // A dial in flight holds a permit the drain below is about to wait for, and a checkout's dial is not in
        // `leased` for the abort at the end to find. Cancelling here means disposal waits out one aborted connect
        // rather than the whole DialTimeout, and leaves no socket behind it.
        await shutdown.CancelAsync().ConfigureAwait(false);

        // Nothing can re-enter the idle set after this: `Return` re-reads `disposed` under `gate`, and the
        // Interlocked.Exchange above happens before this lock is taken, so a returner either added before the
        // drain (and is closed by it) or observes the flag and closes its own connection.
        CloseAll(TakeAllIdle());

        // Wait for the operations still running to give their connections back, by acquiring every permit. Each one
        // finds the pool disposed and closes its connection rather than pooling it. Bounded by PoolTimeout for the
        // same reason a checkout is: an operation that never releases its connection, such as an `await foreach`
        // whose enumerator is never disposed, must not turn disposal into a hang.
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
            // still out, since nothing else can. The pool holds the only other reference to it, and the caller has
            // evidently lost theirs. Aborting closes the transport only, which frees an operation parked on a read
            // that will never arrive, and leaves alone the buffers that operation may still be using.
            List<PooledConnection> stragglers = TakeAllLeased();
            if (stragglers is not null)
            {
                foreach (PooledConnection connection in stragglers)
                {
                    connection.Abort();
                }
            }
        }

        // The factory owns what outlives a single connection — today the TLS certificate authorities. Established
        // connections no longer read it, so a timed-out lease does not delay this release. A dial that ignored the
        // shutdown token does: it is counted separately and the last such dial performs the release as it leaves.
        RequestFactoryDisposal();

        // Neither the semaphore nor the shutdown source is disposed, for the same reason. Neither holds an
        // unmanaged resource here: the semaphore's AvailableWaitHandle is never touched, and the source has no
        // timer, because nothing calls CancelAfter on it. Disposing either would instead fault work still winding
        // down, such as a rent parked on the semaphore or a top-up still reading the token, which is meant to
        // finish and observe the pool as disposed.
    }

    /// <summary>
    /// The period between sweeps for the given options, or <see cref="TimeSpan.Zero"/> when there is nothing for
    /// a sweep to do.
    /// </summary>
    /// <param name="options">The client options holding the lifetime, idle, floor, and sweep-period settings.</param>
    /// <returns>The sweep period, or zero for no sweep.</returns>
    internal static TimeSpan SweepInterval(ClickHouseTcpClientOptions options)
    {
        TimeSpan shortestLimit = ShortestActiveLimit(options);

        // Nothing expires and no floor to hold, so there is no work for a sweep and no timer is created. That
        // matters beyond the wasted wake-ups: a live timer holds a reference to the pool, so a client that is
        // never disposed cannot be collected while one is running.
        if (shortestLimit == TimeSpan.Zero && options.MinPoolSize == 0)
        {
            return TimeSpan.Zero;
        }

        // An explicit period is used as given. Clamping it would silently ignore what the caller asked for, and
        // the caller is overriding the derivation below precisely because it does not suit their workload.
        if (options.SweepInterval is { } configured)
        {
            return configured;
        }

        // Nothing expires, but a floor has to be held, so there is no limit to take a fraction of.
        if (shortestLimit == TimeSpan.Zero)
        {
            return MaxSweepInterval;
        }

        // A fraction of the shortest limit in force, bounded at both ends.
        long ticks = shortestLimit.Ticks / SweepsPerLimit;
        return TimeSpan.FromTicks(Math.Clamp(ticks, MinSweepInterval.Ticks, MaxSweepInterval.Ticks));
    }

    /// <summary>
    /// The shorter of the two expiry limits that are switched on, or <see cref="TimeSpan.Zero"/> when neither is.
    /// Zero means "no limit" on both options, so it can stand for "neither applies" here too.
    /// </summary>
    /// <param name="options">The client options holding the lifetime and idle limits.</param>
    /// <returns>The shortest limit in force, or zero when nothing expires.</returns>
    private static TimeSpan ShortestActiveLimit(ClickHouseTcpClientOptions options)
    {
        TimeSpan lifetime = options.MaxConnectionLifetime;
        TimeSpan idle = options.IdleTimeout;
        bool hasLifetime = lifetime > TimeSpan.Zero;
        bool hasIdle = idle > TimeSpan.Zero;

        if (hasLifetime && hasIdle)
        {
            return lifetime < idle ? lifetime : idle;
        }

        if (hasLifetime)
        {
            return lifetime;
        }

        return hasIdle ? idle : TimeSpan.Zero;
    }

    /// <summary>
    /// Runs a sweep and swallows anything it throws. This is what the timer calls. A timer callback runs on a
    /// thread-pool thread with no one to catch for it, so an escaping exception would end the process, which is too
    /// high a price for a sweep that failed to close one socket. There is nothing to log to yet; observability
    /// lands in Epic P.
    /// </summary>
    internal void SweepQuietly()
    {
        try
        {
            Sweep();
        }
        catch (Exception e) when (e is not OutOfMemoryException and not StackOverflowException)
        {
        }
    }

    /// <summary>
    /// Closes every idle connection that has expired, by lifetime or by idle timeout, whatever the count, then tops
    /// the pool back up to <see cref="ClickHouseTcpClientOptions.MinPoolSize"/> in the background. Runs on the sweep
    /// timer, and is called directly by tests. Only one sweep runs at a time: a call made while another is in
    /// progress returns without doing anything.
    /// </summary>
    internal void Sweep()
    {
        if (Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        // One sweep at a time, because they share the `reaped` buffer: a second sweep would clear it while the first
        // was still closing from it. Timer callbacks can overlap, since closing sockets happens outside the lock and
        // can outlast a period as short as MinSweepInterval. Skipping is correct, because the running sweep reads
        // the same state this one would.
        if (Interlocked.CompareExchange(ref sweeping, 1, 0) != 0)
        {
            return;
        }

        try
        {
            lock (gate)
            {
                // Walked backwards so removing an entry cannot skip the next one. Neither limit yields to
                // MinPoolSize: the top-up below holds the floor by opening fresh connections. Holding it with
                // expired ones would keep the sockets open and still serve nobody, because a checkout refuses an
                // expired connection.
                for (int i = idle.Count - 1; i >= 0; i--)
                {
                    if (idle[i].IsExpired(options))
                    {
                        reaped.Add(idle[i]);
                        idle.RemoveAt(i);
                    }
                }
            }

            CloseAll(reaped);
            LastRefill = StartRefillIfBelowFloor();
        }
        finally
        {
            // Cleared before the flag is released, so the next sweep sees an empty buffer even on another thread:
            // its CompareExchange above pairs with this write.
            reaped.Clear();
            Volatile.Write(ref sweeping, 0);
        }
    }

    /// <summary>
    /// Starts topping the pool back up to <see cref="ClickHouseTcpClientOptions.MinPoolSize"/>, unless a top-up is
    /// already running or there is nothing to do. Returns without waiting, because opening a connection is slow and
    /// the sweep runs on a timer thread that must not block.
    /// </summary>
    /// <returns>The running top-up, for tests to await; null when none was started.</returns>
    internal Task StartRefillIfBelowFloor()
    {
        // The common case by far, the floor defaulting to off, and it costs nothing to leave.
        if (options.MinPoolSize == 0 || Volatile.Read(ref disposed) != 0 || !BelowFloor())
        {
            return null;
        }

        // One at a time. A sweep can fire again while the previous top-up is still dialing, and two of them would
        // race to fill the same gap and overshoot it.
        if (Interlocked.CompareExchange(ref refilling, 1, 0) != 0)
        {
            return null;
        }

        return Task.Run(RefillAsync);
    }

    /// <summary>
    /// Opens connections until the pool holds <see cref="ClickHouseTcpClientOptions.MinPoolSize"/> of them,
    /// stopping early if the pool fills up, runs out of free capacity, is disposed, or a dial fails.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every connection is opened while holding a permit, exactly as a checkout does. The permit is taken with a
    /// zero timeout, so this never queues ahead of a real caller: if every slot is busy the pool needs no warming
    /// and the top-up stops. A permit alone cannot keep the floor inside
    /// <see cref="ClickHouseTcpClientOptions.MaxPoolSize"/>, because an idle connection holds none, so each round
    /// also reserves a slot against the checkouts and dials already in flight.
    /// </para>
    /// <para>
    /// A dial that fails ends the round silently. Nobody is waiting on it, there is nothing to report it to until
    /// Epic P, and the next sweep tries again, so a server that is down costs one failed connect per sweep rather
    /// than a spin.
    /// </para>
    /// </remarks>
    private async Task RefillAsync()
    {
        try
        {
            while (Volatile.Read(ref disposed) == 0 && BelowFloor() && permits.Wait(0))
            {
                // Re-tested now the permit is held, because the check above it is only advisory: the floor may have
                // been met since, by a checkout in flight or by this round's own previous connection.
                if (!TryReserveSlot())
                {
                    permits.Release();
                    return;
                }

                bool reserved = true;
                bool pooled = false;
                try
                {
                    ClickHouseTcpConnection opened = await CreateConnectionAsync(shutdown.Token).ConfigureAwait(false);
                    var connection = new PooledConnection(opened, time);

                    lock (gate)
                    {
                        // Same reasoning as the checkout path. The slot becomes the pooled entry under one lock, and
                        // the disposed check shares that lock; otherwise a connection opened during disposal would
                        // land in a set nothing drains again.
                        pending--;
                        reserved = false;

                        if (Volatile.Read(ref disposed) == 0)
                        {
                            connection.OnReturned();
                            idle.Add(connection);
                            pooled = true;
                        }
                    }

                    if (!pooled)
                    {
                        connection.Close();
                        return;
                    }
                }
                finally
                {
                    if (reserved)
                    {
                        lock (gate)
                        {
                            pending--;
                        }
                    }

                    permits.Release();
                }
            }
        }
        catch (Exception e) when (e is not OutOfMemoryException and not StackOverflowException)
        {
            // A failed dial, or disposal cancelling one. Either way the next sweep reassesses.
        }
        finally
        {
            Volatile.Write(ref refilling, 0);
        }
    }

    /// <summary>
    /// The capacity the pool has accounted for: idle, in use, or held by a checkout or top-up in flight. Read under
    /// <c>gate</c>. It is deliberately an upper bound rather than a census, because it counts a checkout that holds
    /// a slot before it knows whether it will reuse or dial, so it can read one or two above the sockets actually
    /// open. Erring that way makes a top-up decline to dial rather than overshoot.
    /// </summary>
    private int AccountedConnections => idle.Count + leased.Count + pending;

    /// <summary>Whether the pool holds fewer connections than the floor asks for.</summary>
    private bool BelowFloor()
    {
        lock (gate)
        {
            return AccountedConnections < options.MinPoolSize;
        }
    }

    /// <summary>
    /// Reserves capacity for one more connection unless the floor is already met. The floor can never exceed
    /// <see cref="ClickHouseTcpClientOptions.MaxPoolSize"/> and the count it tests never reads low, so a reservation
    /// granted here cannot take the pool past the cap either.
    /// </summary>
    /// <returns>True when the caller may dial, having taken a slot it has to give back.</returns>
    private bool TryReserveSlot()
    {
        lock (gate)
        {
            if (AccountedConnections >= options.MinPoolSize)
            {
                return false;
            }

            pending++;
            return true;
        }
    }

    /// <summary>Opens a connection for a checkout, giving up if the pool is disposed while the dial runs.</summary>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The opened connection.</returns>
    /// <exception cref="ObjectDisposedException">The pool was disposed while the dial was in flight.</exception>
    /// <remarks>
    /// <para>
    /// A dial in flight is the one connection disposal cannot otherwise reach. It is not in <c>leased</c> yet, so the
    /// abort at the end of the drain does not see it, and the socket would outlive the pool by as much as
    /// <see cref="ClickHouseTcpClientOptions.DialTimeout"/>. The caller's own cancellation still surfaces as a
    /// cancellation; only disposal is reported as disposal.
    /// </para>
    /// <para>
    /// The filter reads its tokens when the exception is thrown, not when it was raised, so a dial deadline and a
    /// disposal that land in the same instant can each be reported as the other. Both answers are true of that
    /// instant, so telling them apart is not worth the cost.
    /// </para>
    /// </remarks>
    private async ValueTask<ClickHouseTcpConnection> DialAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, shutdown.Token);
        try
        {
            return await CreateConnectionAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new ObjectDisposedException(ClientTypeName);
        }
    }

    /// <summary>Uses the factory for one dial, keeping it alive until that dial has left the factory.</summary>
    private async ValueTask<ClickHouseTcpConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        lock (gate)
        {
            // A dial is either counted before disposal starts or refused after it. This closes the earlier window
            // between RentAsync's last check and entering the factory.
            ThrowIfDisposed();

            activeDials++;
        }

        try
        {
            return await factory.CreateAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReleaseFactoryUse();
        }
    }

    /// <summary>Requests one-time factory disposal, immediately unless a dial is still using it.</summary>
    private void RequestFactoryDisposal()
    {
        bool disposeFactory;
        lock (gate)
        {
            factoryDisposalRequested = true;
            disposeFactory = activeDials == 0 && !factoryDisposed;
            if (disposeFactory)
            {
                factoryDisposed = true;
            }
        }

        if (disposeFactory)
        {
            factory.Dispose();
        }
    }

    /// <summary>Ends one dial and performs a deferred factory disposal when this was the last one.</summary>
    private void ReleaseFactoryUse()
    {
        bool disposeFactory;
        lock (gate)
        {
            activeDials--;
            disposeFactory = activeDials == 0 && factoryDisposalRequested && !factoryDisposed;
            if (disposeFactory)
            {
                factoryDisposed = true;
            }
        }

        if (!disposeFactory)
        {
            return;
        }

        try
        {
            factory.Dispose();
        }
        catch (Exception e) when (e is not OutOfMemoryException and not StackOverflowException)
        {
            // This runs in a dial's finally block. A teardown failure must not replace that dial's result.
        }
    }

    /// <summary>
    /// Takes the next idle connection that can still be used, closing any that cannot on the way. Returns null when
    /// the idle set holds nothing usable, leaving the caller to open a connection. Callers hold a slot in
    /// <c>pending</c> throughout, which accounts for a connection while it is in neither set.
    /// </summary>
    /// <returns>A usable idle connection, or null.</returns>
    /// <remarks>
    /// This loop is the pool's whole answer to a connection that died while idle. It counts no retries: it discards
    /// candidates until one passes or the set is empty, and then a fresh connection is opened. A failure to open
    /// <i>that</i> connection is reported to the caller rather than retried, so a server that is down is reported as
    /// down instead of being hidden behind the pool timeout.
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

            // Tested outside the lock, because closing a socket must not hold up another caller's checkout.
            if (candidate.CanBeRented(options))
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
    /// Takes a returned connection back, keeping it for reuse when it is still usable and the pool is still open, and
    /// closing it otherwise. Releases the slot last, so a waiter woken by the release finds the connection already in
    /// the idle set.
    /// </summary>
    /// <param name="connection">The connection being returned.</param>
    private void Return(PooledConnection connection)
    {
        // The permit must come back whatever happens below. Otherwise the pool silently shrinks by one slot for the
        // rest of its life, and later callers are told they are running too much work at once.
        try
        {
            // The same test a checkout applies. A connection the next checkout would refuse anyway is better closed
            // now, which frees the socket sooner. It catches what a failed or abandoned operation left behind, either
            // terminated or out of step with the server, and a connection past its age limit. Idleness cannot fire
            // here, the connection having just carried an operation, but it is part of the one shared predicate
            // rather than a case this path has to remember to leave out.
            bool reusable = connection.CanBeRented(options);

            bool pooled = false;
            lock (gate)
            {
                leased.Remove(connection);

                // The disposed check belongs under the lock, because disposal marks the pool disposed and then drains
                // the idle set. A check made outside the lock could add an entry after that drain, and nothing would
                // ever close it. Stamping the idle clock here rather than before the lock keeps the list ordered by
                // that stamp under concurrent returns, which makes index 0 the longest idle.
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
    /// The lease handed to a caller. Disposing it returns the connection to the pool, which then decides whether to
    /// keep or close it. A second dispose does nothing.
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
            // Returns exactly once even if the caller disposes twice, such as an `await using` plus an explicit call.
            if (Interlocked.Exchange(ref returned, 1) == 0)
            {
                pool.Return(pooled);
            }

            return default;
        }
    }
}
