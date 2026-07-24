using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Client-level configuration for a <see cref="ClickHouseTcpClient"/>: the server endpoint, credentials, and
/// the connection-lifetime knobs. Values are init-only; build one directly or parse a connection string with
/// <see cref="FromConnectionString"/>.
/// </summary>
public sealed class ClickHouseTcpClientOptions
{
    internal const string DefaultHost = "localhost";
    internal const int DefaultPort = 9000;
    internal const string DefaultUsername = "default";
    internal const string DefaultDatabase = "default";
    internal const int DefaultMaxSendBufferBytes = 10 * 1024 * 1024;
    internal static readonly TimeSpan DefaultDialTimeout = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan DefaultReadTimeout = TimeSpan.FromSeconds(300);

    /// <summary>The server host name or address. Defaults to <c>localhost</c>.</summary>
    public string Host { get; init; } = DefaultHost;

    /// <summary>The server's native-protocol port. Defaults to <c>9000</c>.</summary>
    public int Port { get; init; } = DefaultPort;

    /// <summary>The user to authenticate as. Defaults to <c>default</c>.</summary>
    public string Username { get; init; } = DefaultUsername;

    /// <summary>The password, sent in plaintext and protected only by TLS. Defaults to empty.</summary>
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
}
