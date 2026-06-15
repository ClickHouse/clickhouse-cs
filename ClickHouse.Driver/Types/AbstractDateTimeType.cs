using System;
using System.Globalization;
using System.Text.RegularExpressions;
using NodaTime;

namespace ClickHouse.Driver.Types;

internal static class DateTimeConversions
{
    public static readonly DateTime DateTimeEpochStart = DateTimeOffset.FromUnixTimeSeconds(0).UtcDateTime;

#if NET6_0_OR_GREATER
    public static readonly DateOnly DateOnlyEpochStart = new(1970, 1, 1);
#endif

    public static int ToUnixTimeDays(this DateTimeOffset dto)
    {
        return (int)(dto.Date - DateTimeEpochStart.Date).TotalDays;
    }

    public static DateTime FromUnixTimeDays(int days) => DateTimeEpochStart.AddDays(days);
}

internal abstract class AbstractDateTimeType : ParameterizedType
{
    // ClickHouse emits synthetic fixed-offset timezone names like "Fixed/UTC+05:30:00" for columns
    // declared with a fixed UTC offset. These names are not in the IANA TZDB so GetZoneOrNull
    // returns null for them. This regex parses them into a NodaTime fixed-offset zone.
    private static readonly Regex FixedUtcOffsetRegex = new(
        @"^Fixed/UTC([+-])(\d{2}):(\d{2}):(\d{2})$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Resolves a ClickHouse timezone name to a <see cref="DateTimeZone"/>.
    /// Handles both standard IANA names and ClickHouse's synthetic
    /// <c>Fixed/UTC±HH:MM:SS</c> fixed-offset names.
    /// Returns <see langword="null"/> if the name cannot be resolved.
    /// </summary>
    internal static DateTimeZone ResolveTimezone(string timeZoneName)
    {
        var zone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName);
        if (zone != null)
            return zone;

        var match = FixedUtcOffsetRegex.Match(timeZoneName);
        if (match.Success)
        {
            var sign = match.Groups[1].Value == "+" ? 1 : -1;
            var hours = int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture);
            var minutes = int.Parse(match.Groups[3].Value, CultureInfo.InvariantCulture);
            var seconds = int.Parse(match.Groups[4].Value, CultureInfo.InvariantCulture);
            var totalSeconds = sign * (hours * 3600 + minutes * 60 + seconds);
            // NodaTime Offset is capped at ±18 h (±64,800 s); return null for out-of-range values.
            const int maxValidSeconds = 18 * 3600;
            if (Math.Abs(totalSeconds) <= maxValidSeconds)
                return DateTimeZone.ForOffset(Offset.FromSeconds(totalSeconds));
        }

        return null;
    }

    public DateTimeOffset CoerceToDateTimeOffset(object value)
    {
        return value switch
        {
#if NET6_0_OR_GREATER
            DateOnly date => new DateTimeOffset(date.Year, date.Month, date.Day, 0, 0, 0, TimeSpan.Zero),
#endif
            DateTimeOffset v => v,
            // UTC DateTime represents a specific instant - preserve it exactly
            DateTime { Kind: DateTimeKind.Utc } dt => new DateTimeOffset(dt),
            // Local DateTime: convert to UTC using system timezone (preserves instant)
            DateTime { Kind: DateTimeKind.Local } dt => new DateTimeOffset(dt),
            // Unspecified DateTime: treat as wall-clock time in target column timezone
            DateTime dt => TimeZoneOrUtc.AtLeniently(LocalDateTime.FromDateTime(dt)).ToDateTimeOffset(),
            OffsetDateTime o => o.ToDateTimeOffset(),
            ZonedDateTime z => z.ToDateTimeOffset(),
            Instant i => ToDateTimeOffset(i),
            _ => throw new NotSupportedException()
        };
    }

    public override Type FrameworkType => typeof(DateTime);

    public DateTimeZone TimeZone { get; set; }

    public DateTimeZone TimeZoneOrUtc => TimeZone ?? DateTimeZone.Utc;

    public override string ToString() => TimeZone == null ? $"{Name}" : $"{Name}('{TimeZone.Id}')";

    private DateTimeOffset ToDateTimeOffset(Instant instant) => instant.InZone(TimeZoneOrUtc).ToDateTimeOffset();

    public DateTime ToDateTime(Instant instant)
    {
        // If no timezone is specified on the column, return Unspecified to preserve wall-clock time
        if (TimeZone == null)
            return instant.InZone(DateTimeZone.Utc).ToDateTimeUnspecified();

        var zonedDateTime = instant.InZone(TimeZone);
        if (zonedDateTime.Offset.Ticks == 0)
            return zonedDateTime.ToDateTimeUtc();
        else
            return zonedDateTime.ToDateTimeUnspecified();
    }
}
