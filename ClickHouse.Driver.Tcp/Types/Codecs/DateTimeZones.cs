using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Timezone resolution shared by the <c>DateTime</c> / <c>DateTime64</c> codecs. A ClickHouse timezone-bearing
/// type stores a UTC instant on the wire; the timezone (from the type string, or the session's when the type
/// omits one) only determines the offset a value is <i>presented</i> with. This resolves that timezone once,
/// at codec construction, and converts UTC instants into offsets at read time.
/// </summary>
internal static class DateTimeZones
{
    // ClickHouse emits synthetic fixed-offset timezone names like "Fixed/UTC+05:30:00" for a column declared
    // with a numeric UTC offset. These are not IANA ids, so FindSystemTimeZoneById cannot resolve them; they
    // are parsed here into a custom fixed-offset zone instead. Minutes and seconds are restricted to 00-59 so
    // a malformed name falls through rather than being misread as a different valid offset.
    private static readonly Regex FixedUtcOffsetRegex = new(
        @"^Fixed/UTC([+-])(\d{2}):([0-5]\d):([0-5]\d)$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Resolves the timezone a codec should present values in: the type string's explicit timezone when given,
    /// otherwise the server/session timezone, otherwise UTC.
    /// </summary>
    /// <param name="explicitTimezone">The timezone from the type string (e.g. <c>Europe/London</c>), or null/empty.</param>
    /// <param name="serverTimezone">The session's timezone, or null/empty when unknown (e.g. the write path).</param>
    /// <returns>The resolved timezone info.</returns>
    /// <exception cref="FormatException">The named timezone is not known to the platform.</exception>
    public static TimeZoneInfo Resolve(string explicitTimezone, string serverTimezone)
    {
        string id = !string.IsNullOrEmpty(explicitTimezone) ? explicitTimezone
            : !string.IsNullOrEmpty(serverTimezone) ? serverTimezone
            : null;

        if (id is null)
        {
            return TimeZoneInfo.Utc;
        }

        Match fixedOffset = FixedUtcOffsetRegex.Match(id);
        if (fixedOffset.Success)
        {
            return CreateFixedOffsetZone(id, fixedOffset);
        }

        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(id);
        }
        catch (TimeZoneNotFoundException ex)
        {
            throw new FormatException($"Timezone '{id}' is not known to this platform.", ex);
        }
        catch (InvalidTimeZoneException ex)
        {
            throw new FormatException($"Timezone '{id}' is invalid.", ex);
        }
    }

    private static TimeZoneInfo CreateFixedOffsetZone(string id, Match match)
    {
        int sign = match.Groups[1].Value == "+" ? 1 : -1;
        int hours = int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture);
        int minutes = int.Parse(match.Groups[3].Value, CultureInfo.InvariantCulture);
        int seconds = int.Parse(match.Groups[4].Value, CultureInfo.InvariantCulture);
        int totalSeconds = sign * ((hours * 3600) + (minutes * 60) + seconds);

        // A custom TimeZoneInfo base offset must be within ±14 hours and a whole number of minutes; ClickHouse
        // fixed offsets in practice satisfy both, but reject anything that does not rather than let the BCL
        // throw an opaque ArgumentException.
        if (Math.Abs(totalSeconds) > 14 * 3600 || totalSeconds % 60 != 0)
        {
            throw new FormatException(
                $"Timezone '{id}' has an offset outside the representable range (±14 hours, whole minutes).");
        }

        return TimeZoneInfo.CreateCustomTimeZone(id, TimeSpan.FromSeconds(totalSeconds), id, id);
    }

    /// <summary>Extracts the single-quoted timezone argument from a type node's arguments, or null when absent.</summary>
    /// <param name="argument">The argument node whose <see cref="TypeNode.Name"/> is a quoted timezone token, or null.</param>
    /// <returns>The unquoted timezone id, or null.</returns>
    public static string UnquoteTimezone(TypeNode argument)
    {
        if (argument is null)
        {
            return null;
        }

        string raw = argument.Name.Trim();
        if (raw.Length >= 2 && raw[0] == '\'' && raw[^1] == '\'')
        {
            return raw.Substring(1, raw.Length - 2);
        }

        return raw;
    }
}
