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

    /// <summary>How long the connection has been sitting idle in the pool.</summary>
    internal TimeSpan IdleFor => time.GetElapsedTime(idleSince);

    /// <summary>Records a checkout, for diagnostics only.</summary>
    /// <remarks>
    /// The idle clock deliberately keeps running while the connection is out. Nothing reads
    /// <see cref="IdleFor"/> except the sweep, which only ever walks the idle set, so the value is meaningless
    /// for a checked-out connection either way — and stopping it here would cost a timestamp per checkout to
    /// maintain a number no one reads.
    /// </remarks>
    internal void OnRented() => UsageCount++;

    /// <summary>Records a return to the idle set, restarting the idle clock.</summary>
    internal void OnReturned() => idleSince = time.GetTimestamp();

    /// <summary>Whether this connection may be handed out: young enough, and usable at the transport level.</summary>
    /// <param name="maxLifetime">The configured age limit, or <see cref="TimeSpan.Zero"/> for none.</param>
    /// <returns>True when the connection can carry another operation.</returns>
    /// <remarks>
    /// Idleness is deliberately not part of this. <c>IdleTimeout</c> exists to release connections nobody is
    /// using, not to judge whether one works — that is <see cref="ClickHouseTcpConnection.IsReusable"/>'s job —
    /// so it is applied by the sweep, which honours <c>MinPoolSize</c>, and not at checkout, which cannot.
    /// </remarks>
    internal bool CanBeRented(TimeSpan maxLifetime)
        => !IsPastLifetime(maxLifetime) && Connection.IsReusable;

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

    /// <summary>Closes the underlying connection. Idempotent, and safe on an already-terminated connection.</summary>
    internal void Close() => Connection.Terminate();
}
