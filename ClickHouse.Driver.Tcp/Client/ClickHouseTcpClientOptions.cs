using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Client-level configuration for a <see cref="ClickHouseTcpClient"/>: the server endpoint, credentials, and
/// the connection-lifetime knobs. Values are init-only; build one directly, parse a connection string with
/// <see cref="FromConnectionString"/>, or derive a variant of an existing instance with a <c>with</c> expression.
/// </summary>
/// <remarks>
/// Being a record, two instances compare equal when every property does. <see cref="CustomSettings"/> is compared
/// by reference, not by content, because the declared type is an interface with no value-equality contract: two
/// options that hold equal-but-distinct dictionaries are <b>not</b> equal. Do not use these options as a cache or
/// pool key; use a purpose-built key type.
/// </remarks>
public sealed record ClickHouseTcpClientOptions
{
    internal const string DefaultHost = "localhost";
    internal const int DefaultPort = 9000;
    internal const string DefaultUsername = "default";
    internal const string DefaultDatabase = "default";
    internal const int DefaultMaxSendBufferBytes = 10 * 1024 * 1024;
    internal const int DefaultMinPoolSize = 0;
    internal const int DefaultMaxPoolSize = 20;
    internal const ClickHouseTcpPoolReusePolicy DefaultPoolReusePolicy = ClickHouseTcpPoolReusePolicy.Lifo;
    internal static readonly TimeSpan DefaultDialTimeout = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan DefaultReadTimeout = TimeSpan.FromSeconds(300);
    internal static readonly TimeSpan DefaultPoolTimeout = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan DefaultMaxConnectionLifetime = TimeSpan.FromMinutes(30);
    internal static readonly TimeSpan DefaultIdleTimeout = TimeSpan.FromMinutes(5);

    /// <summary>The server host name or address. Defaults to <c>localhost</c>.</summary>
    public string Host { get; init; } = DefaultHost;

    /// <summary>The server's native-protocol port. Defaults to <c>9000</c>.</summary>
    public int Port { get; init; } = DefaultPort;

    /// <summary>The user to authenticate as. Defaults to <c>default</c>.</summary>
    public string Username { get; init; } = DefaultUsername;

    /// <summary>
    /// The password, sent to the server in plaintext during the handshake. This client's native-protocol
    /// transport is not encrypted, so the password (and all data) travels in the clear — use it only over a
    /// trusted network. Defaults to empty.
    /// </summary>
    public string Password { get; init; } = string.Empty;

    /// <summary>The default database for queries. Defaults to <c>default</c>.</summary>
    public string Database { get; init; } = DefaultDatabase;

    /// <summary>The keyed-quota resource key, or empty when the client uses no keyed quota.</summary>
    public string QuotaKey { get; init; } = string.Empty;

    /// <summary>
    /// Client-level settings applied to every query and insert (a per-query
    /// <see cref="ClickHouseTcpQueryOptions.Settings"/> value overrides the client-level one). Populated from
    /// the <c>set_&lt;name&gt;</c> keys of a connection string, or set directly. Null means none.
    /// </summary>
    public IReadOnlyDictionary<string, string> CustomSettings { get; init; }

    /// <summary>
    /// The soft cap, in bytes, on the client's send buffer during an insert: while a wire block is written, the
    /// buffered bytes are flushed to the socket whenever they exceed this, bounding peak memory for a large
    /// insert (a single column larger than the cap still buffers in full). Independent of the block-split target,
    /// so blocks stay their natural size and simply stream out within this cap. Defaults to 10 MiB.
    /// </summary>
    public int MaxSendBufferBytes { get; init; } = DefaultMaxSendBufferBytes;

    /// <summary>The deadline for establishing a connection (socket connect plus handshake). Defaults to 30s.</summary>
    public TimeSpan DialTimeout { get; init; } = DefaultDialTimeout;

    /// <summary>
    /// The idle deadline for reading a response — reset each time a packet arrives — so a long streaming query
    /// is not killed for taking a long time overall. Defaults to 300s. <b>Stored but not yet enforced</b>; the
    /// idle-deadline read loop lands in a later change.
    /// </summary>
    public TimeSpan ReadTimeout { get; init; } = DefaultReadTimeout;

    /// <summary>
    /// The number of connections the pool keeps open when it can, counting both idle and in-use ones. Defaults
    /// to 0. Neither <see cref="MaxConnectionLifetime"/> nor <see cref="IdleTimeout"/> respects this floor: an
    /// expired connection is retired whatever the count, and the floor is then restored by opening a fresh one.
    /// So a quiet pool rotates its connections rather than holding the same ones, which is what makes the floor
    /// worth having — a checkout refuses an expired connection, so keeping one to meet the count would only hold
    /// a socket that serves nobody.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The floor is maintained by the same sweep that closes expired connections, so a pool below it is topped up
    /// within one sweep interval rather than at construction — a burst arriving before the first sweep still
    /// opens its own connections. Topping up never takes a slot from a waiting caller: it only uses capacity
    /// that is free at that moment, so a pool already at <see cref="MaxPoolSize"/> simply skips it.
    /// </para>
    /// <para>
    /// Because the rotation is what holds the floor, a pool that carries no traffic at all still reconnects: this
    /// many connections per <see cref="IdleTimeout"/>, indefinitely. At the defaults that is nothing, the floor
    /// being 0. Raising the floor while lowering <see cref="IdleTimeout"/> multiplies the two, so a floor of 10
    /// against a 5-second idle limit is 10 connects, handshakes and authentications every 5 seconds from an
    /// application issuing no queries. Size the pair together.
    /// </para>
    /// </remarks>
    public int MinPoolSize { get; init; } = DefaultMinPoolSize;

    /// <summary>
    /// The hard cap on connections the pool opens, and so on operations that run at once — one connection
    /// carries one query, the protocol having no multiplexing. Defaults to 20. Further callers queue for up to
    /// <see cref="PoolTimeout"/>.
    /// </summary>
    /// <remarks>
    /// Each connection running an insert buffers up to <see cref="MaxSendBufferBytes"/>, so the client's peak
    /// send-buffer memory is about that times this cap when every connection inserts at once.
    /// </remarks>
    public int MaxPoolSize { get; init; } = DefaultMaxPoolSize;

    /// <summary>
    /// How long an operation waits for a connection when <see cref="MaxPoolSize"/> are already in use, after
    /// which it throws a <see cref="TimeoutException"/>. Defaults to 30s. Waiters are not served in a
    /// guaranteed order.
    /// </summary>
    /// <remarks>
    /// This bounds the wait for a free slot only. Opening the connection that follows is bounded separately by
    /// <see cref="DialTimeout"/>, so a checkout can take up to the sum of the two.
    /// </remarks>
    public TimeSpan PoolTimeout { get; init; } = DefaultPoolTimeout;

    /// <summary>
    /// How long after it was opened a connection may still be handed out, after which the pool retires it.
    /// Defaults to 30 minutes; <see cref="TimeSpan.Zero"/> disables the limit and lets a connection live until
    /// it fails.
    /// </summary>
    /// <remarks>
    /// The age is read at checkout and at return, both of which fall between operations, so this never
    /// interrupts a running query: an operation longer than the limit carries its connection past it, and the
    /// connection is closed when it comes back rather than reused.
    /// </remarks>
    public TimeSpan MaxConnectionLifetime { get; init; } = DefaultMaxConnectionLifetime;

    /// <summary>
    /// How long a connection may sit unused before the pool retires it. Defaults to 5 minutes;
    /// <see cref="TimeSpan.Zero"/> keeps idle connections until they expire by age.
    /// </summary>
    /// <remarks>
    /// This releases sockets nobody is using, and it is also the client's defence against a connection killed
    /// while idle. A proxy or load balancer between client and server drops an idle connection on its own
    /// schedule, and such a drop can arrive without a FIN, in which case the transport still looks alive and the
    /// next operation over it stalls until TCP gives up. So a connection past this limit is not handed out either,
    /// exactly as an over-age one is not: set it below the shortest idle timeout on the path to the server.
    /// </remarks>
    public TimeSpan IdleTimeout { get; init; } = DefaultIdleTimeout;

    /// <summary>Which idle connection the pool hands out next. Defaults to <see cref="ClickHouseTcpPoolReusePolicy.Lifo"/>.</summary>
    public ClickHouseTcpPoolReusePolicy PoolReusePolicy { get; init; } = DefaultPoolReusePolicy;

    /// <summary>
    /// These options with <see cref="CustomSettings"/> replaced by a private snapshot, or this instance when there
    /// are none to copy. A client holds its options for its lifetime and merges the settings on every operation, so
    /// it must own them: keeping the caller's dictionary would let a later mutation of it fault or partially apply
    /// mid-merge. Every other property is init-only and so cannot change after construction.
    /// </summary>
    /// <remarks>
    /// The <c>with</c> expression carries every other property across, so a property added later needs no change
    /// here. Keep it that way — a hand-written copy is what silently drops a new property.
    /// </remarks>
    internal ClickHouseTcpClientOptions WithOwnedCustomSettings()
        => CustomSettings is null
            ? this
            : this with { CustomSettings = new Dictionary<string, string>(CustomSettings, StringComparer.Ordinal) };

    /// <summary>Parses a ClickHouse native-protocol connection string into options.</summary>
    /// <param name="connectionString">The connection string (keys such as <c>Host</c>, <c>Port</c>, <c>Username</c>, <c>set_&lt;name&gt;</c>).</param>
    /// <returns>The parsed options.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
    public static ClickHouseTcpClientOptions FromConnectionString(string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        return new ClickHouseTcpConnectionStringBuilder(connectionString).ToOptions();
    }

    /// <summary>Validates the options, throwing if any value is unusable. Runs at client construction.</summary>
    /// <exception cref="ArgumentException"><see cref="Host"/>, <see cref="Username"/>, or <see cref="Database"/> is null or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="Port"/> is out of range, or a timeout / buffer size is not positive.</exception>
    internal void Validate()
    {
        if (string.IsNullOrEmpty(Host))
        {
            throw new ArgumentException("Host must not be null or empty.", nameof(Host));
        }

        if (string.IsNullOrEmpty(Username))
        {
            throw new ArgumentException("Username must not be null or empty.", nameof(Username));
        }

        if (string.IsNullOrEmpty(Database))
        {
            throw new ArgumentException("Database must not be null or empty.", nameof(Database));
        }

        if (Port is < 1 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(Port), Port, "Port must be between 1 and 65535.");
        }

        RequireUsableTimeout(DialTimeout, nameof(DialTimeout));

        RequireUsableTimeout(ReadTimeout, nameof(ReadTimeout));

        if (MaxSendBufferBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxSendBufferBytes), MaxSendBufferBytes, "MaxSendBufferBytes must be positive.");
        }

        RequireUsableTimeout(PoolTimeout, nameof(PoolTimeout));

        if (MaxConnectionLifetime < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxConnectionLifetime), MaxConnectionLifetime, "MaxConnectionLifetime must not be negative; use TimeSpan.Zero to disable the limit.");
        }

        if (IdleTimeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(IdleTimeout), IdleTimeout, "IdleTimeout must not be negative; use TimeSpan.Zero to disable idle closing.");
        }

        if (MaxPoolSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxPoolSize), MaxPoolSize, "MaxPoolSize must be at least 1.");
        }

        if (MinPoolSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MinPoolSize), MinPoolSize, "MinPoolSize must not be negative.");
        }

        if (MinPoolSize > MaxPoolSize)
        {
            throw new ArgumentOutOfRangeException(nameof(MinPoolSize), MinPoolSize, $"MinPoolSize must not exceed MaxPoolSize ({MaxPoolSize}).");
        }

        if (!Enum.IsDefined(PoolReusePolicy))
        {
            throw new ArgumentOutOfRangeException(nameof(PoolReusePolicy), PoolReusePolicy, "PoolReusePolicy is not a defined value.");
        }

        if (CustomSettings is not null)
        {
            foreach (KeyValuePair<string, string> setting in CustomSettings)
            {
                // An empty setting name would collide with the empty key that terminates the settings list on the
                // wire, silently truncating the rest; a null value cannot be written. Reject both up front.
                if (string.IsNullOrEmpty(setting.Key))
                {
                    throw new ArgumentException("A custom setting name must not be null or empty.", nameof(CustomSettings));
                }

                if (setting.Value is null)
                {
                    throw new ArgumentException($"Custom setting '{setting.Key}' has a null value; use an empty string for a flag-style setting.", nameof(CustomSettings));
                }
            }
        }
    }

    /// <summary>
    /// Rejects a deadline that cannot be armed. The timer APIs these feed (<c>CancelAfter</c>,
    /// <c>SemaphoreSlim.WaitAsync</c>) take a millisecond count as an <see cref="int"/>, so a span beyond about
    /// 24.85 days would throw from inside every operation instead of at construction.
    /// </summary>
    /// <param name="value">The configured deadline.</param>
    /// <param name="name">The option's name, for the exception.</param>
    private static void RequireUsableTimeout(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(name, value, $"{name} must be positive.");
        }

        if (value.TotalMilliseconds > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(name, value, $"{name} must not exceed {TimeSpan.FromMilliseconds(int.MaxValue)} (about 24.8 days).");
        }
    }

    /// <summary>Builds the handshake input for a connection from these options. The password is copied, not retained.</summary>
    internal ClientHandshakeParameters ToHandshakeParameters()
        => new()
        {
            Username = Username,
            Password = Password ?? string.Empty,
            Database = string.IsNullOrEmpty(Database) ? DefaultDatabase : Database,
            QuotaKey = QuotaKey ?? string.Empty,
        };

    /// <summary>The endpoint and user this client connects as. Never includes <see cref="Password"/>.</summary>
    /// <remarks>
    /// A record's generated <c>ToString</c> prints every property, which would put the plaintext password into any
    /// log line that formats these options. This override names the safe properties explicitly instead, so a secret
    /// added later is left out until someone chooses to include it.
    /// </remarks>
    public override string ToString()
        => $"{nameof(ClickHouseTcpClientOptions)} {{ Host = {Host}, Port = {Port}, Username = {Username}, Database = {Database} }}";
}
