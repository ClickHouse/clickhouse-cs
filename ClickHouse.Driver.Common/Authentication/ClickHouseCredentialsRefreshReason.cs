namespace ClickHouse.Driver;

/// <summary>
/// Why a client asks its <see cref="ClickHouseCredentialsProvider"/> for credentials.
/// </summary>
public enum ClickHouseCredentialsRefreshReason
{
    /// <summary>
    /// The client has no credentials from the provider yet.
    /// </summary>
    Initial,

    /// <summary>
    /// The cached credentials expire soon or have expired, or their expiry is not known.
    /// </summary>
    Expiring,

    /// <summary>
    /// The server rejected the credentials that the provider gave last. A provider that keeps its own
    /// cache must not return the same credentials again.
    /// </summary>
    Rejected,
}
