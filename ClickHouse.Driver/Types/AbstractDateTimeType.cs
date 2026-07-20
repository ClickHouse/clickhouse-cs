using System;
using System.Globalization;
using System.Text.RegularExpressions;
using ClickHouse.Driver.Formats;
using NodaTime;

namespace ClickHouse.Driver.Types;

internal static class DateTimeConversions
{
    public static readonly DateTime DateTimeEpochStart = DateTimeOffset.FromUnixTimeSeconds(0).UtcDateTime;

    public static readonly DateOnly DateOnlyEpochStart = new(1970, 1, 1);

    public static int ToUnixTimeDays(this DateTimeOffset dto)
    {
        return (int)(dto.Date - DateTimeEpochStart.Date).TotalDays;
    }

    public static DateTime FromUnixTimeDays(int days) => DateTimeEpochStart.AddDays(days);
}

internal abstract class AbstractDateTimeType : ParameterizedType,
    ITypedWriter<DateTime>, ITypedWriter<DateTimeOffset>, ITypedWriter<DateOnly>,
    ITypedReader<DateTime>, ITypedReader<DateTimeOffset>, ITypedReader<DateOnly>
{
    // ClickHouse emits synthetic fixed-offset timezone names like "Fixed/UTC+05:30:00" for columns
    // declared with a fixed UTC offset. These names are not in the IANA TZDB so GetZoneOrNull
    // returns null for them. This regex parses them into a NodaTime fixed-offset zone.
    // Minutes and seconds are restricted to 00-59 so a malformed name falls through to the null
    // fallback instead of being misread as a different valid offset (e.g. 60 minutes as +1 h);
    // out-of-range hours are rejected by the +/-18 h cap in ResolveTimezone.
    private static readonly Regex FixedUtcOffsetRegex = new(
        @"^Fixed/UTC([+-])(\d{2}):([0-5]\d):([0-5]\d)$",
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
            DateOnly date => CoerceToDateTimeOffset(date),
            DateTimeOffset v => CoerceToDateTimeOffset(v),
            DateTime dt => CoerceToDateTimeOffset(dt),
            OffsetDateTime o => o.ToDateTimeOffset(),
            ZonedDateTime z => z.ToDateTimeOffset(),
            Instant i => ToDateTimeOffset(i),
            _ => throw new NotSupportedException()
        };
    }

    /// <summary>
    /// Box-free coercion of a <see cref="DateTime"/> to <see cref="DateTimeOffset"/>, identical to the
    /// <see cref="DateTime"/> branches of <see cref="CoerceToDateTimeOffset(object)"/> (which delegates here).
    /// Used by the POCO insert fast path so a <see cref="DateTime"/> property is never boxed.
    /// </summary>
    public DateTimeOffset CoerceToDateTimeOffset(DateTime value)
    {
        return value.Kind switch
        {
            // UTC DateTime represents a specific instant - preserve it exactly
            DateTimeKind.Utc => new DateTimeOffset(value),
            // Local DateTime: convert to UTC using system timezone (preserves instant)
            DateTimeKind.Local => new DateTimeOffset(value),
            // Unspecified DateTime: treat as wall-clock time in target column timezone
            _ => TimeZoneOrUtc.AtLeniently(LocalDateTime.FromDateTime(value)).ToDateTimeOffset(),
        };
    }

    // Box-free overloads mirroring the corresponding branches of CoerceToDateTimeOffset(object).
    public DateTimeOffset CoerceToDateTimeOffset(DateTimeOffset value) => value;

    public DateTimeOffset CoerceToDateTimeOffset(DateOnly value) => new(value.Year, value.Month, value.Day, 0, 0, 0, TimeSpan.Zero);

    public override void Write(ExtendedBinaryWriter writer, object value) => WriteChecked(writer, CoerceToDateTimeOffset(value), value);

    public void WriteValue(ExtendedBinaryWriter writer, DateTime value) => WriteChecked(writer, CoerceToDateTimeOffset(value), value);

    public void WriteValue(ExtendedBinaryWriter writer, DateTimeOffset value) => WriteChecked(writer, CoerceToDateTimeOffset(value), value);

    public void WriteValue(ExtendedBinaryWriter writer, DateOnly value) => WriteChecked(writer, CoerceToDateTimeOffset(value), value);

    /// <summary>
    /// Serializes the coerced <paramref name="dto"/>. The generic <paramref name="original"/> carries the
    /// caller's un-coerced value so an out-of-range error can report it exactly as the boxed path did; being
    /// a generic parameter, a value-type <paramref name="original"/> is NOT boxed on the hot path — the box
    /// only happens if an <see cref="ArgumentOutOfRangeException"/> is actually thrown.
    /// </summary>
    protected abstract void WriteChecked<T>(ExtendedBinaryWriter writer, DateTimeOffset dto, T original);

    public override Type FrameworkType => typeof(DateTime);

    // The boxed read returns the canonical DateTime, historically the only representation. The typed read
    // fast path can additionally produce DateTimeOffset / DateOnly from the same wire value (explicit impls
    // because they differ only by return type). Each is byte-identical to the boxed path by construction.
    public override object Read(ExtendedBinaryReader reader) => ReadDateTime(reader);

    DateTime ITypedReader<DateTime>.ReadValue(ExtendedBinaryReader reader) => ReadDateTime(reader);

    DateTimeOffset ITypedReader<DateTimeOffset>.ReadValue(ExtendedBinaryReader reader) => ReadDateTimeOffset(reader);

    DateOnly ITypedReader<DateOnly>.ReadValue(ExtendedBinaryReader reader) => ReadDateOnly(reader);

    // Decodes the wire form into a DateTime — the historical Read result. Subclasses implement per their
    // on-wire encoding (seconds / ticks / days).
    protected abstract DateTime ReadDateTime(ExtendedBinaryReader reader);

    // Default derives an offset-0 value from the decoded DateTime; correct for the date-only subtypes, whose
    // DateTime is UTC-kind. Timezone-aware subtypes override to derive the offset from the source instant.
    protected virtual DateTimeOffset ReadDateTimeOffset(ExtendedBinaryReader reader) => new(ReadDateTime(reader), TimeSpan.Zero);

    protected virtual DateOnly ReadDateOnly(ExtendedBinaryReader reader) => DateOnly.FromDateTime(ReadDateTime(reader));

    public DateTimeZone TimeZone { get; set; }

    public DateTimeZone TimeZoneOrUtc => TimeZone ?? DateTimeZone.Utc;

    public override string ToString() => TimeZone == null ? $"{Name}" : $"{Name}('{TimeZone.Id}')";

    protected DateTimeOffset ToDateTimeOffset(Instant instant) => instant.InZone(TimeZoneOrUtc).ToDateTimeOffset();

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
