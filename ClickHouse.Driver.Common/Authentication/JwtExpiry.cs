using System;
using System.Text.Json;

namespace ClickHouse.Driver.Authentication;

/// <summary>
/// Reads the <c>exp</c> claim of a JWT in compact form (three base64url segments). There is no
/// signature check: the value only tells the client when to ask for a new token.
/// </summary>
internal static class JwtExpiry
{
    // The Unix-time range that DateTimeOffset can hold.
    private const double MinUnixSeconds = -62135596800;
    private const double MaxUnixSeconds = 253402300799;

    /// <summary>
    /// Returns the <c>exp</c> claim of <paramref name="token"/>, or <see langword="null"/> when the token
    /// is not a JWT or has no numeric <c>exp</c> claim.
    /// </summary>
    internal static DateTimeOffset? TryRead(string token)
    {
        if (string.IsNullOrEmpty(token))
        {
            return null;
        }

        var first = token.IndexOf('.');
        if (first <= 0)
        {
            return null;
        }

        var second = token.IndexOf('.', first + 1);
        if (second < 0 || second == first + 1 || token.IndexOf('.', second + 1) >= 0)
        {
            return null;
        }

        var payload = DecodeBase64Url(token.AsSpan(first + 1, second - first - 1));
        if (payload is null)
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(payload);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object ||
                !root.TryGetProperty("exp", out var exp) ||
                exp.ValueKind != JsonValueKind.Number ||
                !exp.TryGetDouble(out var seconds) ||
                double.IsNaN(seconds) ||
                seconds < MinUnixSeconds ||
                seconds > MaxUnixSeconds)
            {
                return null;
            }

            return DateTimeOffset.FromUnixTimeMilliseconds((long)Math.Floor(seconds * 1000));
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static byte[] DecodeBase64Url(ReadOnlySpan<char> segment)
    {
        if (segment.Length % 4 == 1)
        {
            return null;
        }

        var padded = new char[segment.Length + ((4 - (segment.Length % 4)) % 4)];
        for (var i = 0; i < segment.Length; i++)
        {
            var c = segment[i];
            padded[i] = c switch
            {
                '-' => '+',
                '_' => '/',
                '+' or '/' or '=' => '\0', // Not base64url: refuse the segment below.
                _ => c,
            };
            if (padded[i] == '\0')
            {
                return null;
            }
        }

        for (var i = segment.Length; i < padded.Length; i++)
        {
            padded[i] = '=';
        }

        var bytes = new byte[(padded.Length / 4) * 3];
        return Convert.TryFromBase64Chars(padded, bytes, out var written) ? bytes.AsSpan(0, written).ToArray() : null;
    }
}
