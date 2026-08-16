using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// A connection the pool owns, plus the bookkeeping the pool needs to decide whether to keep it: when it was
/// opened, when it last went idle, and how often it has been handed out. Timestamps come from a
/// <see cref="TimeProvider"/> and are monotonic, so the pool's clocks are unaffected by a wall-clock change.
/// </summary>
/// <remarks>
/// Not thread-safe, and does not need to be: the pool mutates it under its own lock or while it is checked out,
/// which is exclusive by construction — one connection carries one operation.
/// </remarks>
internal sealed class PooledConnection
{
    private readonly TimeProvider time;
    private readonly long openedAt;
    private long idleSince;
    private bool inUse;

    /// <summary>Wraps a freshly opened connection, starting its age and idle clocks now.</summary>
    /// <param name="connection">The connection, already handshaken and Ready.</param>
    /// <param name="time">The clock the pool measures age and idleness against.</param>
    internal PooledConnection(ClickHouseTcpConnection connection, TimeProvider time)
    {
        Connection = connection;
        this.time = time;
        openedAt = time.GetTimestamp();
        idleSince = openedAt;
    }

    /// <summary>The wrapped connection.</summary>
    internal ClickHouseTcpConnection Connection { get; }

    /// <summary>
    /// How many times this connection has been handed out. Carried for diagnostics — the pool branches on age
    /// and liveness, never on this — and it is what a pool-state metric will report in Epic P.
    /// </summary>
    internal int UsageCount { get; private set; }

    /// <summary>The protocol revision negotiated when this connection was opened.</summary>
    internal int ProtocolVersion => Connection.Protocol.Version;

    /// <summary>How long ago the connection was opened.</summary>
    internal TimeSpan Age => time.GetElapsedTime(openedAt);

    /// <summary>
    /// How long the connection has been sitting unused in the pool, or <see cref="TimeSpan.Zero"/> while it is
    /// checked out — a connection carrying an operation is not idle, however long that operation runs.
    /// </summary>
    internal TimeSpan IdleFor => inUse ? TimeSpan.Zero : time.GetElapsedTime(idleSince);

    /// <summary>Records a checkout, which counts a usage and stops the idle clock.</summary>
    /// <remarks>
    /// <para>
    /// The clock has to stop, not merely be ignored: <see cref="IdleFor"/> is read on the return path as well as
    /// by the sweep, and a clock that kept running would report an operation's own duration as idleness — so a
    /// query longer than <c>IdleTimeout</c> would retire the connection that just ran it successfully.
    /// </para>
    /// <para>
    /// This is the one mutation the pool makes outside its lock, which is safe only because of what the flag it
    /// sets means: a connection this has been called on is in neither the idle set nor anywhere else read under
    /// that lock, so nothing can observe the write concurrently. <b>Anything added later that reads
    /// <see cref="IdleFor"/> for a connection that is checked out — a pool-state metric walking the leased set,
    /// say — has to publish this write instead of relying on that.</b>
    /// </para>
    /// </remarks>
    internal void OnRented()
    {
        UsageCount++;
        inUse = true;
    }

    /// <summary>Records a return to the idle set, restarting the idle clock.</summary>
    internal void OnReturned()
    {
        inUse = false;
        idleSince = time.GetTimestamp();
    }

    /// <summary>Whether this connection may be handed out: inside both limits, and usable at the transport level.</summary>
    /// <param name="options">The client options holding the age and idle limits.</param>
    /// <returns>True when the connection can carry another operation.</returns>
    /// <remarks>
    /// The same question is asked at both ends of a lease — at checkout, of a candidate from the idle set, and on
    /// return, of a connection about to re-enter it — so both ends apply one predicate. Only the limits are
    /// time-based; <see cref="ClickHouseTcpConnection.IsReusable"/> is the transport test, and it is cheap enough
    /// to ask on both paths.
    /// </remarks>
    internal bool CanBeRented(ClickHouseTcpClientOptions options)
        => !IsExpired(options) && Connection.IsReusable;

    /// <summary>Whether either time limit has retired this connection.</summary>
    /// <param name="options">The client options holding the age and idle limits.</param>
    /// <returns>True when the connection must be closed rather than reused.</returns>
    /// <remarks>
    /// Both limits are read at checkout, on return, and by the sweep, and both override <c>MinPoolSize</c>: the
    /// floor is held by opening fresh connections, not by keeping expired ones, since a floor of connections a
    /// checkout would refuse is no floor at all.
    /// </remarks>
    internal bool IsExpired(ClickHouseTcpClientOptions options)
        => IsPastLifetime(options.MaxConnectionLifetime) || IsPastIdleTimeout(options.IdleTimeout);

    /// <summary>Whether the connection is too old to hand out.</summary>
    /// <param name="maxLifetime">The configured age limit, or <see cref="TimeSpan.Zero"/> for none.</param>
    /// <returns>True when the connection must be retired rather than reused.</returns>
    /// <remarks>
    /// Asked at checkout and again on return, both of which happen between operations. A connection running an
    /// operation is never retired under it: the pool reaches a connection only through the idle set, which a
    /// checkout removes it from. So an operation longer than the limit simply carries its connection past that
    /// limit and the connection is closed when it comes back, which is what clickhouse-go's ConnMaxLifetime
    /// does too.
    /// </remarks>
    internal bool IsPastLifetime(TimeSpan maxLifetime)
        => maxLifetime > TimeSpan.Zero && Age >= maxLifetime;

    /// <summary>Whether the connection has sat unused too long to be trusted.</summary>
    /// <param name="idleTimeout">The configured idle limit, or <see cref="TimeSpan.Zero"/> for none.</param>
    /// <returns>True when the connection must be retired rather than reused.</returns>
    /// <remarks>
    /// This bounds how long a connection may sit unused, and it is a liveness limit as much as a resource one: an
    /// idle connection is what a proxy or load balancer between client and server silently drops, and such a drop
    /// is invisible to <see cref="ClickHouseTcpConnection.IsReusable"/> when it arrives without a FIN. So a
    /// connection past this limit is not handed out, exactly as an over-age one is not — set the limit below the
    /// shortest idle timeout on the path to the server.
    /// </remarks>
    internal bool IsPastIdleTimeout(TimeSpan idleTimeout)
        => idleTimeout > TimeSpan.Zero && IdleFor >= idleTimeout;

    /// <summary>
    /// Closes the underlying connection and releases its buffers. Idempotent, and safe on an already-terminated
    /// connection, but only valid between operations — the pool reaches a connection through the idle set, which
    /// a checkout removes it from, so every caller of this holds one no operation is using.
    /// </summary>
    /// <remarks>
    /// Teardown can throw (a socket that fails to close), and the pool has nothing useful to do about it: the
    /// connection is being discarded either way, and letting it propagate would abandon the other connections in
    /// the same batch or fail an unrelated caller's checkout. So this reports nothing; the exception is
    /// swallowed here rather than at each of the four call sites.
    /// </remarks>
    internal void Close()
    {
        try
        {
            Connection.Terminate();
        }
        catch (Exception e) when (e is not OutOfMemoryException and not StackOverflowException)
        {
        }
    }

    /// <summary>
    /// Closes the transport of a connection whose operation is still running, the one teardown that is safe to
    /// race with it. See <see cref="ClickHouseTcpConnection.AbortTransport"/>.
    /// </summary>
    internal void Abort() => Connection.AbortTransport();
}
