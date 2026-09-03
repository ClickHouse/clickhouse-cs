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
    internal const int DefaultMaxSendBufferBytes = Format.BlockWriter.DefaultFlushThresholdBytes;
    internal static readonly TimeSpan DefaultDialTimeout = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan DefaultReadTimeout = TimeSpan.FromSeconds(300);

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
    /// buffered bytes are flushed to the socket once they pass this, bounding peak memory for a large insert.
    /// Independent of the block-split target, so blocks stay their natural size and simply stream out within this
    /// cap. Defaults to 1 MiB.
    ///
    /// <para>
    /// The cap is checked after each column, so peak buffered bytes are the cap plus the column that crossed it,
    /// and a single column larger than the cap buffers in full. Raising it also raises what the send buffer keeps:
    /// it grows by doubling, and every size it passes through stays in the array pool.
    /// </para>
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

        if (DialTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(DialTimeout), DialTimeout, "DialTimeout must be positive.");
        }

        if (ReadTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ReadTimeout), ReadTimeout, "ReadTimeout must be positive.");
        }

        if (MaxSendBufferBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxSendBufferBytes), MaxSendBufferBytes, "MaxSendBufferBytes must be positive.");
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
