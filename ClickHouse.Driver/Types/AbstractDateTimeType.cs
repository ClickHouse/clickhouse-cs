using System;
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

    public override string ToString() => TimeZone == null ? $"{Name}" : $"{Name}({TimeZone.Id})";

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
