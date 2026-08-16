using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// A connection the pool owns, with the bookkeeping the pool decides its fate from: when it was opened, when it
/// last went idle, and how often it has been handed out. Timestamps come from a <see cref="TimeProvider"/> and are
/// monotonic, so a wall-clock change does not affect these clocks.
/// </summary>
/// <remarks>
/// Not thread-safe, and does not need to be. The pool mutates it either under its own lock or while it is checked
/// out, and a checked-out connection carries exactly one operation.
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
    /// How many times this connection has been handed out. Diagnostic only: the pool never branches on it. Epic P
    /// is what reports it.
    /// </summary>
    internal int UsageCount { get; private set; }

    /// <summary>The protocol revision negotiated when this connection was opened.</summary>
    internal int ProtocolVersion => Connection.Protocol.Version;

    /// <summary>How long ago the connection was opened.</summary>
    internal TimeSpan Age => time.GetElapsedTime(openedAt);

    /// <summary>
    /// How long the connection has been sitting unused in the pool, or <see cref="TimeSpan.Zero"/> while it is
    /// checked out. Time spent running an operation is not idle time.
    /// </summary>
    internal TimeSpan IdleFor => inUse ? TimeSpan.Zero : time.GetElapsedTime(idleSince);

    /// <summary>Records a checkout, which counts a usage and stops the idle clock.</summary>
    /// <remarks>
    /// <para>
    /// The clock has to stop rather than merely be ignored, because <see cref="IdleFor"/> is read on the return
    /// path as well as by the sweep. A clock left running would report an operation's own duration as idleness, so
    /// a query longer than <c>IdleTimeout</c> would retire the connection that had just run it.
    /// </para>
    /// <para>
    /// This is the pool's one mutation made outside its lock. It is safe because of what the flag means: a
    /// connection this has been called on is in neither the idle set nor anywhere else read under that lock, so no
    /// other thread can observe the write. Code added later that reads <see cref="IdleFor"/> for a checked-out
    /// connection, such as a pool-state metric walking the leased set, must publish this write instead of relying
    /// on that.
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
    /// Both ends of a lease ask this: a checkout, of a candidate from the idle set, and a return, of a connection
    /// about to re-enter it. <see cref="ClickHouseTcpConnection.IsReusable"/> is the transport test, and it is
    /// cheap enough to ask on both paths.
    /// </remarks>
    internal bool CanBeRented(ClickHouseTcpClientOptions options)
        => !IsExpired(options) && Connection.IsReusable;

    /// <summary>Whether either time limit has retired this connection.</summary>
    /// <param name="options">The client options holding the age and idle limits.</param>
    /// <returns>True when the connection must be closed rather than reused.</returns>
    /// <remarks>
    /// Both limits are read at checkout, on return, and by the sweep, and both override <c>MinPoolSize</c>. The
    /// floor is held by opening fresh connections, because a checkout refuses an expired one: keeping expired
    /// connections to meet the count would hold sockets that serve nobody.
    /// </remarks>
    internal bool IsExpired(ClickHouseTcpClientOptions options)
        => IsPastLifetime(options.MaxConnectionLifetime) || IsPastIdleTimeout(options.IdleTimeout);

    /// <summary>Whether the connection is too old to hand out.</summary>
    /// <param name="maxLifetime">The configured age limit, or <see cref="TimeSpan.Zero"/> for none.</param>
    /// <returns>True when the connection must be retired rather than reused.</returns>
    /// <remarks>
    /// Read at checkout and on return, both of which fall between operations, so this never interrupts a running
    /// query: the pool reaches a connection only through the idle set, and a checkout removes it from there. An
    /// operation longer than the limit carries its connection past it, and the connection is closed when it comes
    /// back. clickhouse-go's ConnMaxLifetime behaves the same way.
    /// </remarks>
    internal bool IsPastLifetime(TimeSpan maxLifetime)
        => maxLifetime > TimeSpan.Zero && Age >= maxLifetime;

    /// <summary>Whether the connection has sat unused too long to be trusted.</summary>
    /// <param name="idleTimeout">The configured idle limit, or <see cref="TimeSpan.Zero"/> for none.</param>
    /// <returns>True when the connection must be retired rather than reused.</returns>
    /// <remarks>
    /// This bounds how long a connection may sit unused, and it is a liveness limit as much as a resource one. A
    /// proxy or load balancer between client and server drops idle connections on its own schedule, and such a drop
    /// can arrive without a FIN, which <see cref="ClickHouseTcpConnection.IsReusable"/> cannot detect. So a
    /// connection past this limit is not handed out. Set the limit below the shortest idle timeout on the path to
    /// the server.
    /// </remarks>
    internal bool IsPastIdleTimeout(TimeSpan idleTimeout)
        => idleTimeout > TimeSpan.Zero && IdleFor >= idleTimeout;

    /// <summary>
    /// Closes the underlying connection and releases its buffers. Idempotent and safe on an already-terminated
    /// connection, but valid only between operations. Every caller holds a connection taken out of the idle set,
    /// which is the pool's only route to one, so no operation is using it.
    /// </summary>
    /// <remarks>
    /// Teardown can throw, from a socket that fails to close, and the pool has nothing useful to do about it: the
    /// connection is being discarded either way, and an escaping exception would abandon the rest of the batch or
    /// fail an unrelated caller's checkout. So this reports nothing, and swallows the exception here rather than at
    /// every call site.
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
