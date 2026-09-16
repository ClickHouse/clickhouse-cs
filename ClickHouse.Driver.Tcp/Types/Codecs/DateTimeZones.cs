using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A column's timezone, or the reason this platform cannot represent the one the header named. A
/// <c>DateTime</c> column carries a plain count that needs no zone, so an unrepresentable name is reported to a
/// caller asking for a calendar value and not to the block read, which would lose every column and every row.
/// </summary>
internal sealed class ResolvedTimeZone
{
    /// <summary>UTC: the zone when neither the type string nor the session names one.</summary>
    public static readonly ResolvedTimeZone Utc = new(TimeZoneInfo.Utc);

    private readonly TimeZoneInfo zone;
    private readonly string failure;
    private readonly Exception cause;

    /// <summary>Initializes a resolved zone.</summary>
    /// <param name="zone">The zone.</param>
    public ResolvedTimeZone(TimeZoneInfo zone) => this.zone = zone ?? throw new ArgumentNullException(nameof(zone));

    private ResolvedTimeZone(string failure, Exception cause)
    {
        this.failure = failure;
        this.cause = cause;
    }

    /// <summary>Whether a zone resolved, i.e. whether <see cref="Value"/> returns rather than throws.</summary>
    public bool IsResolved => zone is not null;

    /// <summary>The zone the column's counts are presented in.</summary>
    /// <exception cref="FormatException">The header named a timezone this platform cannot represent.</exception>
    public TimeZoneInfo Value => zone ?? throw new FormatException(failure, cause);

    /// <summary>Records a timezone name no zone could be built from.</summary>
    /// <param name="failure">The message <see cref="Value"/> throws with.</param>
    /// <param name="cause">The underlying exception, when there was one.</param>
    /// <returns>The unresolved timezone.</returns>
    public static ResolvedTimeZone Unrepresentable(string failure, Exception cause = null) => new(failure, cause);
}

/// <summary>
/// Resolves explicit and session timezones for the DateTime codecs. They control calendar projections and the
/// interpretation of unspecified <see cref="DateTime"/> values; wire values remain UTC instants.
/// </summary>
internal static class DateTimeZones
{
    // ClickHouse emits synthetic fixed-offset timezone names like "Fixed/UTC+05:30:00" for a column declared
    // with a numeric UTC offset. These are not IANA ids, so FindSystemTimeZoneById cannot resolve them; they
    // are parsed here into a custom fixed-offset zone instead. The three components are summed as written,
    // which is what the server does: 26.6 applies Fixed/UTC+05:00:60 as +05:01:00 and Fixed/UTC+05:70:00 as
    // +06:10:00, keeping the name it was given.
    private static readonly Regex FixedUtcOffsetRegex = new(
        @"^Fixed/UTC([+-])(\d{2}):(\d{2}):(\d{2})$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Resolves the timezone a codec should present values in: the type string's explicit timezone when given,
    /// otherwise the server/session timezone, otherwise UTC.
    /// </summary>
    /// <param name="explicitTimezone">The timezone from the type string (e.g. <c>Europe/London</c>), or null/empty.</param>
    /// <param name="serverTimezone">The session's timezone, or null/empty when unknown.</param>
    /// <returns>
    /// The zone, or the reason it cannot be represented. Nothing here throws: a caller that needs the zone gets
    /// the failure from <see cref="ResolvedTimeZone.Value"/>, so a name this platform cannot express does not
    /// take a whole block's read with it.
    /// </returns>
    public static ResolvedTimeZone Resolve(string explicitTimezone, string serverTimezone)
    {
        string id = !string.IsNullOrEmpty(explicitTimezone) ? explicitTimezone
            : !string.IsNullOrEmpty(serverTimezone) ? serverTimezone
            : null;

        if (id is null)
        {
            return ResolvedTimeZone.Utc;
        }

        Match fixedOffset = FixedUtcOffsetRegex.Match(id);
        if (fixedOffset.Success)
        {
            return FixedOffsetZone(id, fixedOffset);
        }

        try
        {
            return new ResolvedTimeZone(TimeZoneInfo.FindSystemTimeZoneById(id));
        }
        catch (TimeZoneNotFoundException ex)
        {
            return ResolvedTimeZone.Unrepresentable($"Timezone '{id}' is not known to this platform.", ex);
        }
        catch (InvalidTimeZoneException ex)
        {
            return ResolvedTimeZone.Unrepresentable($"Timezone '{id}' is invalid.", ex);
        }
    }

    private static ResolvedTimeZone FixedOffsetZone(string id, Match match)
    {
        int sign = match.Groups[1].Value == "+" ? 1 : -1;
        int hours = int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture);
        int minutes = int.Parse(match.Groups[3].Value, CultureInfo.InvariantCulture);
        int seconds = int.Parse(match.Groups[4].Value, CultureInfo.InvariantCulture);
        int totalSeconds = sign * ((hours * 3600) + (minutes * 60) + seconds);

        // A custom TimeZoneInfo base offset must be within ±14 hours and a whole number of minutes. The server
        // goes further — 26.6 accepts and applies Fixed/UTC+19:00:00 and Fixed/UTC+05:30:15 — so an offset it
        // uses and .NET cannot express is reported to whoever asks for a calendar value.
        if (Math.Abs(totalSeconds) > 14 * 3600 || totalSeconds % 60 != 0)
        {
            return ResolvedTimeZone.Unrepresentable(
                $"Timezone '{id}' has an offset of {OffsetText(totalSeconds)}, which .NET's TimeZoneInfo cannot represent " +
                "(it allows ±14 hours, in whole minutes). A DateTime or DateTime64 column with this timezone still reads " +
                "as its raw counts; only a calendar value needs the zone.");
        }

        return new ResolvedTimeZone(TimeZoneInfo.CreateCustomTimeZone(id, TimeSpan.FromSeconds(totalSeconds), id, id));
    }

    private static string OffsetText(int totalSeconds)
    {
        int magnitude = Math.Abs(totalSeconds);
        return string.Format(
            CultureInfo.InvariantCulture,
            "{0}{1:00}:{2:00}:{3:00}",
            totalSeconds < 0 ? '-' : '+',
            magnitude / 3600,
            magnitude / 60 % 60,
            magnitude % 60);
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
