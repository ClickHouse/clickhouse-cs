namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// The subset of client identity that a connection retains after authentication to populate the ClientInfo
/// block of every Query packet. Deliberately excludes the password (and default database): those are needed
/// only during the handshake, so keeping them alive for the connection's lifetime would widen the window in
/// which the plaintext password sits in memory for no benefit. The quota key is kept because every query
/// carries it.
/// </summary>
internal sealed class ClientMetadata
{
    /// <summary>Informational client identifier, pinned to the same value the handshake reports.</summary>
    public string ClientName => ClientHandshakeParameters.DefaultClientName;

    /// <summary>Informational client major version.</summary>
    public int VersionMajor { get; init; }

    /// <summary>Informational client minor version.</summary>
    public int VersionMinor { get; init; }

    /// <summary>Username, echoed as the initiating user in ClientInfo.</summary>
    required public string Username { get; init; }

    /// <summary>Keyed-quota resource key, sent with every query. Empty when the client uses no keyed quota.</summary>
    public string QuotaKey { get; init; } = string.Empty;

    /// <summary>Copies the query-time fields out of the handshake input, leaving its secrets behind.</summary>
    /// <param name="parameters">The full handshake parameters supplied at connect time.</param>
    /// <returns>The non-secret query metadata.</returns>
    public static ClientMetadata FromHandshake(ClientHandshakeParameters parameters) => new()
    {
        VersionMajor = parameters.VersionMajor,
        VersionMinor = parameters.VersionMinor,
        Username = parameters.Username,
        QuotaKey = parameters.QuotaKey,
    };
}
