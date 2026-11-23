using System;
using ClickHouse.Driver.Authentication;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// The credentials from the client's <see cref="ClickHouseTcpClientOptions.CredentialsProvider"/> that one
/// connection authenticated with, and the cache they came from.
/// </summary>
/// <remarks>
/// It keeps the credentials object, secret included, for the life of the connection, because the cache identifies
/// rejected credentials by reference. The cache keeps the same object while the credentials are cached.
/// </remarks>
internal sealed class ConnectionCredentials
{
    private readonly CredentialsCache cache;

    /// <summary>Records the credentials that a new connection authenticated with.</summary>
    /// <param name="cache">The cache of the client that opened the connection.</param>
    /// <param name="credentials">The credentials that <paramref name="cache"/> returned for the connection.</param>
    internal ConnectionCredentials(CredentialsCache cache, ClickHouseCredentials credentials)
    {
        this.cache = cache;
        Credentials = credentials;
    }

    /// <summary>The credentials the connection authenticated with.</summary>
    internal ClickHouseCredentials Credentials { get; }

    /// <summary>
    /// Whether the connection must no longer be handed out, because its credentials expire within
    /// <see cref="CredentialsCache.ExpiredMargin"/>. Always false when the expiry is unknown.
    /// </summary>
    /// <param name="now">The current time.</param>
    /// <returns>True when the credentials have expired.</returns>
    internal bool IsExpired(DateTimeOffset now) => CredentialsCache.IsExpired(Credentials.EffectiveExpiresAt, now);

    /// <summary>Tells the cache that the server rejected these credentials, so the next connection gets new ones.</summary>
    internal void Reject() => cache.Invalidate(Credentials);
}
