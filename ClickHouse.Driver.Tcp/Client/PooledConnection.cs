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

    /// <summary>Records a checkout: bumps the usage count and stops the idle clock.</summary>
    internal void OnRented() => UsageCount++;

    /// <summary>Records a return to the idle set, restarting the idle clock.</summary>
    internal void OnReturned() => idleSince = time.GetTimestamp();

    /// <summary>
    /// Whether this connection may still be handed out: usable at the transport level, and with enough of its
    /// lifetime left to derive a query deadline from.
    /// </summary>
    /// <param name="maxLifetime">The configured age limit, or <see cref="TimeSpan.Zero"/> for none.</param>
    /// <returns>True when the connection can carry another operation.</returns>
    /// <remarks>
    /// Idleness is deliberately not part of this. <c>IdleTimeout</c> exists to release connections nobody is
    /// using, not to judge whether one works — that is <see cref="ClickHouseTcpConnection.IsReusable"/>'s job —
    /// so it is applied by the sweep, which honours <c>MinPoolSize</c>, and not at checkout, which cannot.
    /// </remarks>
    internal bool CanBeRented(TimeSpan maxLifetime)
        => !IsPastLifetime(maxLifetime) && Connection.IsReusable;

    /// <summary>
    /// Whether the connection is too old to hand out. Retirement starts one
    /// <see cref="ConnectionLifetimeDeadline.RetirementFloor"/> early, so a connection is never leased with too
    /// little life left to bound a query inside it.
    /// </summary>
    /// <param name="maxLifetime">The configured age limit, or <see cref="TimeSpan.Zero"/> for none.</param>
    /// <returns>True when the connection must be retired rather than reused.</returns>
    internal bool IsPastLifetime(TimeSpan maxLifetime)
        => maxLifetime > TimeSpan.Zero && Age >= maxLifetime - ConnectionLifetimeDeadline.RetirementFloor;

    /// <summary>
    /// The life left before the connection reaches its age limit, or null when it has no age limit. This is what
    /// an operation's server-side deadline is derived from. Because <see cref="IsPastLifetime"/> stops leasing a
    /// retirement floor early, a leased connection always has more than that floor left, and so a derived
    /// deadline is always comfortably positive.
    /// </summary>
    /// <param name="maxLifetime">The configured age limit, or <see cref="TimeSpan.Zero"/> for none.</param>
    /// <returns>The remaining lifetime, or null when unlimited.</returns>
    internal TimeSpan? RemainingLifetime(TimeSpan maxLifetime)
        => maxLifetime > TimeSpan.Zero ? maxLifetime - Age : null;

    /// <summary>Closes the underlying connection. Idempotent, and safe on an already-terminated connection.</summary>
    internal void Close() => Connection.Terminate();
}
