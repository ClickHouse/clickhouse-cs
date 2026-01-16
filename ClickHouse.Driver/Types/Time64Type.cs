using System;
using System.Globalization;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Represents the ClickHouse Time64 data type.
/// Time64 stores a time-of-day with fractional seconds, with configurable precision.
/// It has no calendar date components (day, month, year). Internally stored as a signed
/// 64-bit decimal (Decimal64) number of fractional seconds.
/// <para>
/// Converted to a <see cref="TimeSpan">TimeSpan</see> type.
/// </para>
/// <para>
/// <b>Precision limitations:</b> TimeSpan has 100-nanosecond (10^-7) precision.
/// Time64 supports precision levels 0-9 (up to 1 nanosecond). When precision is 8 or 9,
/// values will be rounded to the nearest 100 nanoseconds due to TimeSpan's limitations.
/// For most use cases (milliseconds=3, microseconds=6), no precision is lost.
/// </para>
/// <para>
/// At the moment, the option enable_time_time64_type must be set to 1 to use Time or Time64.
/// </para>
/// </summary>
internal class Time64Type : ParameterizedType
{
    // Range: [-999:59:59.xxx, 999:59:59.xxx] in seconds with fractional part
    internal const decimal MinSeconds = -3599999.999999999m; // -999:59:59.999999999
    internal const decimal MaxSeconds = 3599999.999999999m; // 999:59:59.999999999

    // Cached format strings for each scale (0-9) to avoid allocation on every format call
    private static readonly string[] CachedFormatStrings =
    [
        "{0:00}",           // Scale 0
        "{0:00.0}",         // Scale 1
        "{0:00.00}",        // Scale 2
        "{0:00.000}",       // Scale 3
        "{0:00.0000}",      // Scale 4
        "{0:00.00000}",     // Scale 5
        "{0:00.000000}",    // Scale 6
        "{0:00.0000000}",   // Scale 7
        "{0:00.00000000}",  // Scale 8
        "{0:00.000000000}", // Scale 9
    ];

    private Decimal64Type decimalType;

    /// <summary>
    /// Gets or sets the precision (scale) of the fractional seconds.
    /// Valid range: 0-9. Common values: 3 (milliseconds), 6 (microseconds), 9 (nanoseconds).
    /// </summary>
    public int Scale
    {
        get;
        init
        {
            field = value;
            decimalType = new Decimal64Type
            {
                Scale = value,
            };
        }
    }

    public override string Name => "Time64";

    public override Type FrameworkType => typeof(TimeSpan);

    public override string ToString() => $"Time64({Scale})";

    /// <summary>
    /// Converts ClickHouse fractional seconds (with given scale) to TimeSpan.
    /// Note: This will round values when Scale > 7 (TimeSpan is 100ns precision)
    /// </summary>
    internal static TimeSpan FromClickHouseDecimal(decimal fractionalSeconds)
    {
        // ClickHouse stores as fractional seconds with Scale decimal places
        // TimeSpan.FromSeconds handles the conversion to ticks (100ns precision)
        var seconds = (double)fractionalSeconds;
        return TimeSpan.FromSeconds(seconds);
    }

    /// <summary>
    /// Converts TimeSpan to ClickHouse fractional seconds (with given scale).
    /// </summary>
    internal decimal ToClickHouseDecimal(TimeSpan timeSpan)
    {
        // Convert TimeSpan to decimal seconds
        var seconds = (decimal)timeSpan.TotalSeconds;

        // Round to the appropriate precision for the scale
        // This ensures we don't send more precision than ClickHouse expects
        return Math.Round(seconds, Scale);
    }

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        if (node.ChildNodes.Count == 0)
            throw new ArgumentException("Time64 requires a precision parameter (0-9)", nameof(node));

        var scale = int.Parse(node.ChildNodes[0].Value, CultureInfo.InvariantCulture);

        if (scale < 0 || scale > 9)
            throw new ArgumentOutOfRangeException(nameof(node), $"Time64 precision must be between 0 and 9, got {scale}");

        return new Time64Type
        {
            Scale = scale,
        };
    }

    public override object Read(ExtendedBinaryReader reader)
    {
        // Read as Decimal64 (like DecimalType does)
        // Time64 is stored as Decimal64(Scale) internally
        var fractionalSeconds = (decimal)decimalType.Read(reader);

        return Time64Type.FromClickHouseDecimal(fractionalSeconds);
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var timeSpan = CoerceToTimeSpan(value);
        var fractionalSeconds = ToClickHouseDecimal(timeSpan);

        // Clamp to valid range, as the db does
        fractionalSeconds = Math.Max(MinSeconds, Math.Min(MaxSeconds, fractionalSeconds));

        // Write as Decimal64
        ClickHouseDecimal clickHouseDecimal = fractionalSeconds;
        decimalType.Write(writer, clickHouseDecimal);
    }

    private static TimeSpan CoerceToTimeSpan(object value)
    {
        return value switch
        {
            TimeSpan ts => ts,
            decimal d => TimeSpan.FromSeconds((double)d),
            double db => TimeSpan.FromSeconds(db),
            float f => TimeSpan.FromSeconds(f),
            int i => TimeSpan.FromSeconds(i),
            long l => TimeSpan.FromSeconds(l),
            string s => ParseTime64String(s),
            _ => throw new NotSupportedException($"Cannot convert {value?.GetType().Name ?? "null"} to Time64")
        };
    }

    /// <summary>
    /// Parses a time string in the format [-]HHH:MM:SS[.fraction]
    /// </summary>
    private static TimeSpan ParseTime64String(string timeString)
    {
        if (string.IsNullOrWhiteSpace(timeString))
            throw new ArgumentException("Time64 string cannot be null or empty", nameof(timeString));

        var span = timeString.AsSpan().Trim();
        var isNegative = false;

        if (span.Length > 0 && span[0] == '-')
        {
            isNegative = true;
            span = span.Slice(1);
        }

        // Split by ':' to get hours, minutes, seconds
        var str = span.ToString();
        var parts = str.Split(':');
        if (parts.Length != 3)
            throw new FormatException($"Invalid Time64 format: {timeString}. Expected format: [-]HHH:MM:SS[.fraction]");

        if (!int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var hours))
            throw new FormatException($"Invalid hours in Time64: {timeString}");

        if (!int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var minutes))
            throw new FormatException($"Invalid minutes in Time64: {timeString}");

        // Seconds might have fractional part
        if (!decimal.TryParse(parts[2], NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var seconds))
            throw new FormatException($"Invalid seconds in Time64: {timeString}");

        var totalSeconds = (hours * 3600m) + (minutes * 60m) + seconds;
        var result = TimeSpan.FromSeconds((double)(isNegative ? -totalSeconds : totalSeconds));

        return result;
    }

    /// <summary>
    /// Formats a decimal seconds value as a time string in the format [-]HHH:MM:SS.fraction
    /// </summary>
    private string FormatTime64String(decimal totalSeconds)
    {
        var isNegative = totalSeconds < 0;
        var absSeconds = Math.Abs(totalSeconds);

        var hours = (int)(absSeconds / 3600m);
        var remainder = absSeconds % 3600m;
        var minutes = (int)(remainder / 60m);
        var seconds = remainder % 60m;

        // Use cached format string for the current scale (avoids string allocation)
        var secondsStr = string.Format(CultureInfo.InvariantCulture, CachedFormatStrings[Scale], seconds);

        return isNegative
            ? $"-{hours}:{minutes:D2}:{secondsStr}"
            : $"{hours}:{minutes:D2}:{secondsStr}";
    }

    /// <summary>
    /// Formats a TimeSpan as a time string in the format [-]HHH:MM:SS.fraction
    /// </summary>
    public string FormatTime64String(TimeSpan timeSpan)
    {
        var totalSeconds = ToClickHouseDecimal(timeSpan);
        return FormatTime64String(totalSeconds);
    }
}
