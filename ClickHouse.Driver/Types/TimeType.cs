using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Represents the ClickHouse Time data type.
/// Time stores a time value with hour, minute, and second components,
/// independent of any calendar date. Internally stored as a signed 32-bit integer
/// representing seconds, with a range of [-999:59:59, 999:59:59].
/// <para>
/// Converted to a <see cref="TimeSpan">TimeSpan</see> type. Note that the ClickHouse Time type has lower precision than TimeSpan.
/// </para>
/// <para>
/// At the moment, the option enable_time_time64_type must be set to 1 to use Time or Time64.
/// </para>
/// </summary>
internal class TimeType : ClickHouseType
{
    // Range: [-999:59:59, 999:59:59] in seconds
    internal const int MinSeconds = -3599999; // -999:59:59
    internal const int MaxSeconds = 3599999;  // 999:59:59

    public override Type FrameworkType => typeof(TimeSpan);

    public override string ToString() => "Time";

    public override object Read(ExtendedBinaryReader reader)
    {
        var seconds = reader.ReadInt32();
        return TimeSpan.FromSeconds(seconds);
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var seconds = CoerceToSeconds(value);

        // Apply saturation to the valid range
        seconds = Math.Max(MinSeconds, Math.Min(MaxSeconds, seconds));

        writer.Write(seconds);
    }

    private static int CoerceToSeconds(object value)
    {
        return value switch
        {
            TimeSpan ts => (int)Math.Round(ts.TotalSeconds),
            int i => i,
            _ => throw new NotSupportedException($"Cannot convert {value?.GetType().Name ?? "null"} to Time")
        };
    }

    /// <summary>
    /// Formats a TimeSpan as a time string in the format [-]HHH:MM:SS
    /// </summary>
    public static string FormatTimeString(TimeSpan timeSpan)
    {
        return FormatTimeString((int)Math.Round(timeSpan.TotalSeconds));
    }

    /// <summary>
    /// Formats seconds as a time string in the format [-]HHH:MM:SS
    /// </summary>
    public static string FormatTimeString(int totalSeconds)
    {
        var isNegative = totalSeconds < 0;
        var absSeconds = Math.Abs(totalSeconds);

        var hours = absSeconds / 3600;
        var minutes = (absSeconds % 3600) / 60;
        var seconds = absSeconds % 60;

        return isNegative
            ? $"-{hours}:{minutes:D2}:{seconds:D2}"
            : $"{hours}:{minutes:D2}:{seconds:D2}";
    }
}
