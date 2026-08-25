using System;
using System.Collections.Generic;
using System.Net.Security;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp.Protocol;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Client-level configuration for a <see cref="ClickHouseTcpClient"/>: the server endpoint, credentials, and
/// the connection-lifetime knobs. Values are init-only; build one directly, parse a connection string with
/// <see cref="FromConnectionString"/>, or derive a variant of an existing instance with a <c>with</c> expression.
/// </summary>
/// <remarks>
/// Being a record, two instances compare equal when every property does. <see cref="CustomSettings"/>,
/// <see cref="Compressor"/>, <see cref="LoggerFactory"/> and <see cref="ConfigureTls"/> are compared by
/// reference, not by content: the first three are interfaces with no value-equality contract, the last a
/// delegate. Two options that hold equal-but-distinct dictionaries, or equivalent-but-distinct lambdas, are
/// <b>not</b> equal. Do not use these options as a cache or pool key; use a purpose-built key type.
/// </remarks>
public sealed record ClickHouseTcpClientOptions
{
    internal const string DefaultHost = "localhost";
    internal const int DefaultPort = 9000;
    internal const int DefaultTlsPort = 9440;
    internal const string DefaultUsername = "default";
    internal const string DefaultDatabase = "default";
    internal const int DefaultMaxSendBufferBytes = 10 * 1024 * 1024;
    internal const int DefaultMinPoolSize = 0;
    internal const int DefaultMaxPoolSize = 20;
    internal const ClickHouseTcpPoolReusePolicy DefaultPoolReusePolicy = ClickHouseTcpPoolReusePolicy.Lifo;
    internal const int DefaultStatementMaxLength = 300;

    /// <summary>
    /// The <c>Compression</c> connection-string value used when the key is absent. LZ4 is what
    /// <c>clickhouse-client</c> uses on this protocol: the cheapest codec in CPU and the lightest on the
    /// server. Pass <c>Compression=none</c>, or a null <see cref="Compressor"/>, to turn it off.
    /// </summary>
    internal const string DefaultCompression = CompressionLz4;

    internal const string CompressionNone = "none";
    internal const string CompressionLz4 = "lz4";
    internal const string CompressionZstd = "zstd";
    internal static readonly TimeSpan DefaultDialTimeout = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan DefaultReadTimeout = TimeSpan.FromSeconds(300);
    internal static readonly TimeSpan DefaultPoolTimeout = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan DefaultMaxConnectionLifetime = TimeSpan.FromMinutes(30);
    internal static readonly TimeSpan DefaultIdleTimeout = TimeSpan.FromMinutes(5);

    /// <summary>The server host name or address. Defaults to <c>localhost</c>.</summary>
    public string Host { get; init; } = DefaultHost;

    /// <summary>
    /// The server's native-protocol port. Null, the default, derives the port from <see cref="UseTls"/>:
    /// <c>9440</c> for the secure native port, <c>9000</c> for the plaintext one. An explicit value is always
    /// used as given, so a server on a non-standard port needs no other change.
    /// </summary>
    public int? Port { get; init; }

    /// <summary>The port a connection actually dials: <see cref="Port"/> when set, otherwise derived from <see cref="UseTls"/>.</summary>
    internal int ResolvedPort => Port ?? (UseTls ? DefaultTlsPort : DefaultPort);

    /// <summary>The user to authenticate as. Defaults to <c>default</c>.</summary>
    public string Username { get; init; } = DefaultUsername;

    /// <summary>
    /// The password, sent to the server in plaintext during the handshake. Without <see cref="UseTls"/> the
    /// native-protocol transport is not encrypted, so the password (and all data) travels in the clear — over an
    /// untrusted network, enable TLS. Defaults to empty.
    /// </summary>
    public string Password { get; init; } = string.Empty;

    /// <summary>
    /// Whether to encrypt the transport with TLS, negotiated before the handshake. Defaults to false. The
    /// protocol bytes are identical either way, so this changes nothing above the transport — but the handshake
    /// carries <see cref="Password"/> in plaintext, so TLS is the only thing protecting credentials in transit.
    /// </summary>
    /// <remarks>
    /// The server must be listening for secure native connections (<c>tcp_port_secure</c>, conventionally 9440);
    /// TLS is not negotiated in-band, so pointing a TLS client at a plaintext port fails the TLS handshake rather
    /// than falling back. With <see cref="Port"/> left null the correct port is chosen for you.
    /// </remarks>
    public bool UseTls { get; init; }

    /// <summary>
    /// The host name to present as SNI and to match the server certificate against. Null, the default, uses
    /// <see cref="Host"/>. Set it when <see cref="Host"/> is an address or an internal alias that the certificate
    /// does not name.
    /// </summary>
    public string TlsServerName { get; init; }

    /// <summary>
    /// Whether to accept a server certificate that fails validation — an expired or self-signed certificate, or
    /// one naming another host. Defaults to false, and should stay false outside development.
    /// </summary>
    /// <remarks>
    /// This stops the client from making sure the peer is the server it asked for. An attacker who can intercept
    /// the connection can then present any certificate and read the handshake password. To trust a private
    /// certificate authority and keep that check, use <see cref="TlsCaCertificatePath"/>.
    /// </remarks>
    public bool TlsAllowInvalidCertificates { get; init; }

    /// <summary>
    /// Path to a PEM file of certificate authorities to validate the server against, instead of the host's trust
    /// store. Null, the default, uses the host's trust store. The file must contain at least one self-issued root;
    /// it may also contain intermediate certificates needed to build the server's chain. The host name is still
    /// matched either way.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Instead of, not as well as: the server must chain to one of these authorities, and a certificate the
    /// host's trust store would accept on its own is refused. That is the point of naming an authority — an
    /// additive check would still take a certificate mis-issued by any public authority.
    /// </para>
    /// <para>
    /// The file is read once, when the client is constructed, so a missing or malformed file fails there rather
    /// than on the first connection. Self-issued certificates are the trust anchors; other certificates in the
    /// file are available only to build a chain to one of those anchors. Cannot be combined with
    /// <see cref="TlsAllowInvalidCertificates"/>.
    /// </para>
    /// </remarks>
    public string TlsCaCertificatePath { get; init; }

    /// <summary>
    /// A hook to adjust the TLS client options before the handshake, applied last and so able to override
    /// everything the properties above set. Null, the default, leaves them as configured.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the escape hatch for anything the declarative properties do not cover — client certificates, a
    /// protocol-version floor, cipher suites, a bespoke validation callback. It runs once per connection.
    /// </para>
    /// <para>
    /// Because it runs last it can also weaken the transport, and in two ways that are easy to miss: replacing
    /// <c>RemoteCertificateValidationCallback</c> drops the check configured above, and clearing
    /// <c>TargetHost</c> stops the server name being matched at all while chain validation still appears to run.
    /// </para>
    /// <para>
    /// With <see cref="TlsCaCertificatePath"/> set, a <c>CertificateChainPolicy</c> is already in place, and the
    /// hook receives it and may edit it. In that case .NET ignores <c>CertificateRevocationCheckMode</c>; configure
    /// revocation through the policy's own <c>RevocationMode</c> property instead.
    /// </para>
    /// </remarks>
    public Action<SslClientAuthenticationOptions> ConfigureTls { get; init; }

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
    /// The number of connections the pool keeps open when it can, counting both idle and in-use ones. Defaults to 0.
    /// Neither <see cref="MaxConnectionLifetime"/> nor <see cref="IdleTimeout"/> respects this floor: an expired
    /// connection is retired whatever the count, and the floor is then restored by opening a fresh one. So a quiet
    /// pool rotates its connections rather than holding the same ones. That is what makes the floor useful, because a
    /// checkout refuses an expired connection: keeping one to meet the count would hold a socket that serves nobody.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The floor is maintained by the same sweep that closes expired connections, so a pool below it is topped up
    /// within one sweep period rather than at construction, and a burst arriving before the first sweep still opens
    /// its own connections. Topping up never takes a slot from a waiting caller: it uses only capacity that is free
    /// at that moment, so a pool already at <see cref="MaxPoolSize"/> skips it.
    /// </para>
    /// <para>
    /// Because the rotation is what holds the floor, a pool carrying no traffic at all still reconnects: this many
    /// connections per <see cref="IdleTimeout"/>, indefinitely. At the defaults that is nothing, the floor being 0.
    /// Raising the floor while lowering <see cref="IdleTimeout"/> multiplies the two, so a floor of 10 against a
    /// 5-second idle limit is 10 connects, handshakes and authentications every 5 seconds from an application issuing
    /// no queries. Size the pair together.
    /// </para>
    /// </remarks>
    public int MinPoolSize { get; init; } = DefaultMinPoolSize;

    /// <summary>
    /// The hard cap on connections the pool opens, and so on operations that run at once, one connection carrying one
    /// query. Defaults to 20. Further callers queue for up to <see cref="PoolTimeout"/>.
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
    /// This releases sockets nobody is using, and it is also the client's defence against a connection killed while
    /// idle. A proxy or load balancer between client and server drops an idle connection on its own schedule, and
    /// such a drop can arrive without a FIN, in which case the transport still looks alive and the next operation over
    /// it stalls until TCP gives up. So a connection past this limit is not handed out, just as an over-age one is
    /// not. Set it below the shortest idle timeout on the path to the server.
    /// </remarks>
    public TimeSpan IdleTimeout { get; init; } = DefaultIdleTimeout;

    /// <summary>
    /// How often the pool looks for connections to retire and tops itself back up to <see cref="MinPoolSize"/>.
    /// Null, the default, derives the period from the limits in force. An explicit value is used as given.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The derived period is a quarter of the shorter of <see cref="MaxConnectionLifetime"/> and
    /// <see cref="IdleTimeout"/>, held between 1 and 30 seconds. The fraction keeps the delay in noticing an
    /// expiry proportional to the limit being enforced, which one fixed period cannot do for limits that range
    /// from seconds to hours. The lower bound stops a short limit making the sweep spin; the upper bound still
    /// releases idle sockets in good time when both limits are long. At the default limits the period is 30
    /// seconds.
    /// </para>
    /// <para>
    /// Set this only for a workload the derivation does not suit. The value is used unclamped, so a very short period
    /// costs a wake-up that often for as long as the client lives. It must be positive. When both limits are zero and
    /// there is no floor to hold, the pool schedules no sweep at all, whatever this says.
    /// </para>
    /// </remarks>
    public TimeSpan? SweepInterval { get; init; }

    /// <summary>Which idle connection the pool hands out next. Defaults to <see cref="ClickHouseTcpPoolReusePolicy.Lifo"/>.</summary>
    public ClickHouseTcpPoolReusePolicy PoolReusePolicy { get; init; } = DefaultPoolReusePolicy;

    /// <summary>
    /// Codec for the native protocol's compression frames, or <see langword="null"/> to exchange blocks
    /// uncompressed. Use <see cref="Lz4Compressor"/> (cheapest, lowest server-side load) or
    /// <see cref="ZstdCompressor"/> (smaller, more CPU); a custom <see cref="IClickHouseCompressor"/> works if
    /// it implements the native block path.
    /// <para>
    /// Compression is requested per query, so this is the default for every query the client runs. It governs
    /// both directions: the server compresses the blocks it sends and expects the client's own blocks framed
    /// the same way. Null means the request carries no compression at all, which is not the same as a frame
    /// whose method byte is NONE.
    /// </para>
    /// <para>
    /// A codec chooses the method byte and the body encoding, but never the decoding: the server picks its own
    /// codec, so a client that asks for LZ4 can still be sent ZSTD and must decode whatever arrives.
    /// </para>
    /// </summary>
    public IClickHouseCompressor Compressor { get; init; } = ResolveCompressor(DefaultCompression);

    /// <summary>
    /// Where the client gets its loggers, or null to log nothing. Cannot be set from a connection string.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The client logs its own lifecycle — connects, handshakes, pool checkouts and retirements, operation
    /// outcomes — under the <c>ClickHouse.Driver.Tcp.*</c> categories. It does <b>not</b> log what the
    /// <i>server</i> reports: server log lines go to
    /// <see cref="ClickHouseTcpQueryCallbacks.OnServerLog"/>, where the caller decides whether they are worth
    /// logging. Nothing is formatted while the matching category and level are disabled.
    /// </para>
    /// <para>
    /// <b>At Debug the statement text is logged</b>, up to <see cref="StatementMaxLength"/> characters. That is
    /// deliberate — a driver log without the statement is hard to use — but it is not the same policy as
    /// <see cref="IncludeSqlInActivityTags"/>, which keeps the statement out of traces unless asked. Enable
    /// Debug on <c>ClickHouse.Driver.Tcp.Client</c> only where the statements may be recorded, or set
    /// <see cref="StatementMaxLength"/> to zero to keep them out of both channels.
    /// </para>
    /// </remarks>
    public ILoggerFactory LoggerFactory { get; init; }

    /// <summary>
    /// Whether to put the statement text in the <c>db.query.text</c> span attribute. Defaults to false, because
    /// a statement can carry data a trace is not meant to hold.
    /// </summary>
    public bool IncludeSqlInActivityTags { get; init; }

    /// <summary>
    /// How much of the statement may leave the client as telemetry, in characters; longer text is truncated.
    /// Defaults to 300.
    /// </summary>
    /// <remarks>
    /// It caps both channels — the <c>db.query.text</c> span attribute and the <c>Debug</c> log line — so zero
    /// or less keeps the statement text out of telemetry altogether, whatever
    /// <see cref="IncludeSqlInActivityTags"/> says.
    /// </remarks>
    public int StatementMaxLength { get; init; } = DefaultStatementMaxLength;

    /// <summary>
    /// These options with <see cref="CustomSettings"/> replaced by a private snapshot, or this instance when there
    /// are none to copy. A client holds its options for its lifetime and merges the settings on every operation, so
    /// it must own them: keeping the caller's dictionary would let a later mutation of it fault or partially apply
    /// mid-merge. Every other property is init-only and so cannot change after construction.
    /// </summary>
    /// <remarks>
    /// The <c>with</c> expression carries every other property across, so a property added later needs no change
    /// here. Keep it that way: a hand-written copy is what silently drops a new property.
    /// </remarks>
    /// <summary>
    /// The statement text as telemetry may carry it: truncated to <see cref="StatementMaxLength"/>, or empty
    /// when that allows none.
    /// </summary>
    /// <param name="sql">The statement.</param>
    /// <returns>The text to report, possibly empty, never null.</returns>
    internal string StatementForTelemetry(string sql)
    {
        if (sql is null || StatementMaxLength <= 0)
        {
            return string.Empty;
        }

        return sql.Length <= StatementMaxLength ? sql : sql[..StatementMaxLength];
    }

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

    /// <summary>
    /// Maps a <c>Compression</c> connection-string value to its codec. <c>none</c> yields
    /// <see langword="null"/>, meaning the query is not compressed at all.
    /// </summary>
    /// <param name="name">The codec name, case-insensitive. Null or empty means none.</param>
    /// <returns>The codec, or null for no compression.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> names no known codec.</exception>
    internal static IClickHouseCompressor ResolveCompressor(string name) => name?.Trim().ToLowerInvariant() switch
    {
        null or "" or CompressionNone => null,
        CompressionLz4 => Lz4Compressor.Default,
        CompressionZstd => ZstdCompressor.Default,
        _ => throw new ArgumentException(
            $"Compression '{name}' is not a known codec; expected '{CompressionLz4}', '{CompressionZstd}' or '{CompressionNone}'.",
            nameof(name)),
    };

    /// <summary>Validates the options, throwing if any value is unusable. Runs at client construction.</summary>
    /// <exception cref="ArgumentException"><see cref="Host"/>, <see cref="Username"/>, or <see cref="Database"/> is null or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="Port"/> is out of range, or a timeout / buffer size is not positive.</exception>
    internal void Validate()
    {
        // A codec that only implements the HTTP body path cannot frame a block. Refuse it here rather than
        // mid-query, where the Query packet has already promised the server compressed blocks.
        if (Compressor is not null)
        {
            try
            {
                _ = Compressor.MethodByte;
            }
            catch (NotSupportedException e)
            {
                throw new ArgumentException(
                    $"{Compressor.GetType().Name} does not support the native block path, so it cannot frame a block; use {nameof(Lz4Compressor)} or {nameof(ZstdCompressor)}.",
                    nameof(Compressor),
                    e);
            }
        }

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

        // Only when set. Null is not a value here but a request to derive the port, which cannot be out of range.
        if (Port is { } port && port is < 1 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(Port), port, "Port must be between 1 and 65535.");
        }

        if (TlsCaCertificatePath is not null && string.IsNullOrWhiteSpace(TlsCaCertificatePath))
        {
            throw new ArgumentException(
                $"{nameof(TlsCaCertificatePath)} must be null or a non-empty path.",
                nameof(TlsCaCertificatePath));
        }

        RequireTlsFor(!string.IsNullOrEmpty(TlsServerName), nameof(TlsServerName));
        RequireTlsFor(TlsAllowInvalidCertificates, nameof(TlsAllowInvalidCertificates));
        RequireTlsFor(TlsCaCertificatePath is not null, nameof(TlsCaCertificatePath));
        RequireTlsFor(ConfigureTls is not null, nameof(ConfigureTls));

        // Contradictory: with validation off no certificate is checked against anything, so the authority would
        // be read from disk and never consulted. Refusing beats a precedence rule nobody reads.
        if (TlsAllowInvalidCertificates && TlsCaCertificatePath is not null)
        {
            throw new ArgumentException(
                $"{nameof(TlsAllowInvalidCertificates)} and {nameof(TlsCaCertificatePath)} cannot both be set: with certificate validation off the authority is never consulted.",
                nameof(TlsCaCertificatePath));
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

        // Only when set. Null is not a value here but a request to derive the period, which cannot be out of range.
        if (SweepInterval is { } sweepInterval)
        {
            RequireUsableTimeout(sweepInterval, nameof(SweepInterval));
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
    /// Rejects a TLS property configured on a client that does not use TLS. Such a property has no effect, and
    /// silently ignoring it would let a connection meant to be encrypted run in the clear — the exact mistake a
    /// forgotten <see cref="UseTls"/> makes, with a configured certificate authority as the evidence.
    /// </summary>
    /// <param name="isSet">Whether the property carries a value.</param>
    /// <param name="name">The property's name, for the exception.</param>
    private void RequireTlsFor(bool isSet, string name)
    {
        if (isSet && !UseTls)
        {
            throw new ArgumentException($"{name} is set but {nameof(UseTls)} is false, so the connection would not be encrypted. Set {nameof(UseTls)} to true, or remove {name}.", name);
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
    /// added later is left out until someone chooses to include it. The port shown is the resolved one, which is
    /// what a connection dials.
    /// </remarks>
    public override string ToString()
        => $"{nameof(ClickHouseTcpClientOptions)} {{ Host = {Host}, Port = {ResolvedPort}, Username = {Username}, Database = {Database}, UseTls = {UseTls} }}";
}
