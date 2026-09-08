using System;

namespace ClickHouse.Driver;

/// <summary>
/// An immutable set of credentials returned by <see cref="ADO.ClickHouseClientSettings.CredentialsProvider"/>.
/// Represents either basic authentication (username and password) or bearer token authentication.
/// Create instances via <see cref="CreateBasic"/> or <see cref="CreateBearer"/>.
/// </summary>
public sealed class ClickHouseCredentials
{
    private ClickHouseCredentials(string username, string password, string bearerToken)
    {
        Username = username;
        Password = password;
        BearerToken = bearerToken;
    }

    /// <summary>
    /// Gets the username for basic authentication. Null when these credentials represent a bearer token.
    /// </summary>
    public string Username { get; }

    /// <summary>
    /// Gets the password for basic authentication. Null when these credentials represent a bearer token.
    /// </summary>
    public string Password { get; }

    /// <summary>
    /// Gets the bearer token. Null when these credentials represent basic authentication.
    /// </summary>
    public string BearerToken { get; }

    /// <summary>
    /// Creates credentials for basic (username and password) authentication.
    /// </summary>
    /// <param name="username">The username. Must not be null or empty.</param>
    /// <param name="password">The password. A null value is treated as an empty password.</param>
    /// <returns>Credentials carrying the username and password.</returns>
    public static ClickHouseCredentials CreateBasic(string username, string password)
    {
        if (string.IsNullOrEmpty(username))
            throw new ArgumentException("Username must not be null or empty", nameof(username));

        return new ClickHouseCredentials(username, password ?? string.Empty, bearerToken: null);
    }

    /// <summary>
    /// Creates credentials for bearer token (JWT) authentication.
    /// The token should be provided as-is (already encoded if required by your auth provider).
    /// </summary>
    /// <param name="bearerToken">The bearer token. Must not be null or empty.</param>
    /// <returns>Credentials carrying the bearer token.</returns>
    public static ClickHouseCredentials CreateBearer(string bearerToken)
    {
        if (string.IsNullOrEmpty(bearerToken))
            throw new ArgumentException("Bearer token must not be null or empty", nameof(bearerToken));

        return new ClickHouseCredentials(username: null, password: null, bearerToken);
    }
}
