using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json.Serialization;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Refreshes a JWT with a credentials provider. The provider does an OAuth 2.0 refresh-token grant
/// (RFC 6749, section 6) with <see cref="HttpClient"/>, and the client calls it again before the token expires.
/// </summary>
public static class TokenRefresh
{
    public static async Task Run()
    {
        // In production, these come from your identity provider and your secret store.
        var tokenEndpoint = Environment.GetEnvironmentVariable("OAUTH_TOKEN_ENDPOINT");
        var clientId = Environment.GetEnvironmentVariable("OAUTH_CLIENT_ID");
        var clientSecret = Environment.GetEnvironmentVariable("OAUTH_CLIENT_SECRET");
        var refreshToken = Environment.GetEnvironmentVariable("OAUTH_REFRESH_TOKEN");
        var host = Environment.GetEnvironmentVariable("CLICKHOUSE_CLOUD_HOST");

        if (string.IsNullOrEmpty(tokenEndpoint) || string.IsNullOrEmpty(clientId) ||
            string.IsNullOrEmpty(refreshToken) || string.IsNullOrEmpty(host))
        {
            Console.WriteLine("Skipping the token refresh example: set OAUTH_TOKEN_ENDPOINT, OAUTH_CLIENT_ID, " +
                              "OAUTH_REFRESH_TOKEN (and OAUTH_CLIENT_SECRET if your client has one) and CLICKHOUSE_CLOUD_HOST " +
                              "(a ClickHouse Cloud service with a JWT provider for your identity provider)");
            return;
        }

        // The client does not limit how long the provider takes, so give the token request its own timeout.
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };

        var settings = new ClickHouseClientSettings
        {
            Host = host,
            Port = 8443,
            Protocol = "https",
            CredentialsProvider = async (request, cancellationToken) =>
            {
                // Initial: the first call. Expiring: the token expires soon. Rejected: the server rejected the token.
                // A refresh-token grant always returns a new token, so this provider handles the three in one way.
                Console.WriteLine($"   Provider called: {request.Reason}");

                var form = new Dictionary<string, string>
                {
                    ["grant_type"] = "refresh_token",
                    ["refresh_token"] = refreshToken,
                };
                using var tokenRequest = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint);
                if (string.IsNullOrEmpty(clientSecret))
                {
                    // A public client names itself in the body.
                    form["client_id"] = clientId;
                }
                else
                {
                    // A confidential client authenticates with HTTP Basic (RFC 6749, section 2.3.1), which every
                    // identity provider supports. The ID and the secret are form-encoded first.
                    var basic = $"{WebUtility.UrlEncode(clientId)}:{WebUtility.UrlEncode(clientSecret)}";
                    tokenRequest.Headers.Authorization =
                        new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(basic)));
                }

                tokenRequest.Content = new FormUrlEncodedContent(form);

                // expires_in counts from the response; take the time before the request, so the expiry is not late.
                var requestedAt = DateTimeOffset.UtcNow;
                using var response = await http.SendAsync(tokenRequest, cancellationToken);
                response.EnsureSuccessStatusCode();
                var token = await response.Content.ReadFromJsonAsync<TokenResponse>(cancellationToken)
                            ?? throw new InvalidOperationException("The token endpoint returned no token.");
                if (!string.Equals(token.TokenType, "Bearer", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException($"Expected a Bearer token, got the token type '{token.TokenType}'.");
                }

                // Some identity providers give a new refresh token with each grant, and the old one stops working.
                // One client calls its provider at most once at a time, so the closure can keep it without a lock. Add a
                // lock (for example a SemaphoreSlim) if several clients share this provider.
                if (!string.IsNullOrEmpty(token.RefreshToken))
                {
                    refreshToken = token.RefreshToken;
                }

                // ClickHouse Cloud accepts only a JWT access token, from the identity provider of its JWT provider.
                // expires_in is the lifetime in seconds. Without it, the client reads the exp claim of the JWT.
                DateTimeOffset? expiresAt = token.ExpiresIn > 0 ? requestedAt.AddSeconds(token.ExpiresIn.Value) : null;
                return ClickHouseCredentials.CreateBearer(token.AccessToken, expiresAt);
            },
        };

        using var client = new ClickHouseClient(settings);

        // The first query calls the provider. The second one uses the cached token: the client calls the provider
        // again only from 60 seconds before the token expires, or after the server rejects it.
        Console.WriteLine("1. First query:");
        var user = await client.ExecuteScalarAsync("SELECT currentUser()");
        Console.WriteLine($"   Current user: {user}");

        Console.WriteLine("\n2. Second query:");
        user = await client.ExecuteScalarAsync("SELECT currentUser()");
        Console.WriteLine($"   Current user: {user}");

        Console.WriteLine("\nToken refresh example completed successfully!");
    }

    private sealed class TokenResponse
    {
        [JsonPropertyName("access_token")]
        public string AccessToken { get; set; } = string.Empty;

        [JsonPropertyName("token_type")]
        public string? TokenType { get; set; }

        [JsonPropertyName("expires_in")]
        public int? ExpiresIn { get; set; }

        [JsonPropertyName("refresh_token")]
        public string? RefreshToken { get; set; }
    }
}
