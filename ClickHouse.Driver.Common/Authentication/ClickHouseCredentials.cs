using System;
using System.Globalization;
using ClickHouse.Driver.Authentication;

namespace ClickHouse.Driver;

/// <summary>
/// Credentials that an <see cref="IClickHouseCredentialsProvider"/> returns: a username and a password,
/// or a bearer token (a JWT). Create an instance with <see cref="CreateBasic"/> or
/// <see cref="CreateBearer"/>.
/// </summary>
/// <remarks>
/// <see cref="ToString"/> never shows the password or the token.
/// </remarks>
public sealed class ClickHouseCredentials
{
    private ClickHouseCredentials(string username, string password, string bearerToken, DateTimeOffset? expiresAt)
    {
        Username = username;
        Password = password;
        BearerToken = bearerToken;
        ExpiresAt = expiresAt;
        EffectiveExpiresAt = expiresAt ?? (bearerToken is null ? null : JwtExpiry.TryRead(bearerToken));
    }

    /// <summary>
    /// Gets the username, or <see langword="null"/> for a bearer token.
    /// </summary>
    public string Username { get; }

    /// <summary>
    /// Gets the password, or <see langword="null"/> for a bearer token.
    /// </summary>
    public string Password { get; }

    /// <summary>
    /// Gets the bearer token, or <see langword="null"/> for a username and a password.
    /// </summary>
    public string BearerToken { get; }

    /// <summary>
    /// Gets the time at which the credentials expire, as the provider gave it. When it is
    /// <see langword="null"/> and <see cref="BearerToken"/> is a JWT, the client uses the token's
    /// <c>exp</c> claim instead.
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; }

    /// <summary>
    /// Gets the expiry that the client uses: <see cref="ExpiresAt"/>, else the <c>exp</c> claim of a JWT
    /// bearer token, else <see langword="null"/> (unknown).
    /// </summary>
    internal DateTimeOffset? EffectiveExpiresAt { get; }

    internal bool IsBearer => BearerToken is not null;

    /// <summary>
    /// Creates credentials for a username and a password.
    /// </summary>
    /// <param name="username">The ClickHouse user name.</param>
    /// <param name="password">The password. <see langword="null"/> is the same as an empty password.</param>
    /// <returns>The credentials. Their expiry is unknown, so the client does not cache them.</returns>
    public static ClickHouseCredentials CreateBasic(string username, string password)
    {
        ArgumentNullException.ThrowIfNull(username);

        return new ClickHouseCredentials(username, password ?? string.Empty, bearerToken: null, expiresAt: null);
    }

    /// <summary>
    /// Creates credentials for a bearer token (a JWT).
    /// </summary>
    /// <param name="token">The token, as the server expects it.</param>
    /// <param name="expiresAt">
    /// The time at which the token expires. When it is <see langword="null"/>, the client reads the
    /// <c>exp</c> claim of a JWT (without a signature check). If the expiry is not known, the client
    /// does not cache the token and calls the provider again for each use.
    /// </param>
    /// <returns>The credentials.</returns>
    public static ClickHouseCredentials CreateBearer(string token, DateTimeOffset? expiresAt = null)
    {
        if (string.IsNullOrEmpty(token))
        {
            throw new ArgumentException("The bearer token cannot be null or empty.", nameof(token));
        }

        return new ClickHouseCredentials(username: null, password: null, token, expiresAt);
    }

    /// <inheritdoc />
    public override string ToString()
    {
        if (!IsBearer)
        {
            return $"{nameof(ClickHouseCredentials)} {{ Type = Basic, Username = {Username} }}";
        }

        var expiry = EffectiveExpiresAt?.ToString("O", CultureInfo.InvariantCulture) ?? "unknown";
        return $"{nameof(ClickHouseCredentials)} {{ Type = Bearer, ExpiresAt = {expiry} }}";
    }
}
