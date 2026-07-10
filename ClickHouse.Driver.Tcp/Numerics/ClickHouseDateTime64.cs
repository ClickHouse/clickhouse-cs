using System;
using System.Globalization;

namespace ClickHouse.Driver.Tcp.Numerics;

/// <summary>
/// A ClickHouse <c>DateTime64(scale)</c> value: a signed count of ticks at <c>10^-scale</c> seconds since the
/// Unix epoch, together with its scale (0–9) and the offset from UTC it is presented with. This is the CLR
/// representation of <c>DateTime64</c> columns, retaining the exact wire value so no precision is lost —
/// including scales 8 (10 ns) and 9 (1 ns), which are finer than a .NET tick (100 ns).
///
/// <para>
/// <see cref="ToDateTimeOffset"/> is a convenience view at .NET's 100 ns tick resolution and is therefore lossy
/// for scales finer than 7; <see cref="Count"/> and <see cref="ToUnixTimeNanoseconds"/> give the exact value.
/// Equality and comparison are by instant (the offset and scale a value carries do not affect them), so the
/// same instant expressed at different scales compares equal.
/// </para>
/// </summary>
public readonly struct ClickHouseDateTime64 : IEquatable<ClickHouseDateTime64>, IComparable<ClickHouseDateTime64>, IFormattable
{
    /// <summary>The largest scale ClickHouse <c>DateTime64</c> supports (nanoseconds).</summary>
    public const int MaxScale = 9;

    private const long NanosecondsPerSecond = 1_000_000_000L;
    private const int DotNetTickScale = 7; // .NET tick = 100 ns = 10^-7 s.

    private readonly long count;
    private readonly byte scale;
    private readonly int offsetSeconds;

    /// <summary>Initializes a value from a raw tick count at <paramref name="scale"/>, and its presentation offset.</summary>
    /// <param name="count">The signed count of ticks at <c>10^-scale</c> seconds since the Unix epoch.</param>
    /// <param name="scale">The number of fractional-second digits (0–9).</param>
    /// <param name="offset">The offset from UTC the value is presented with.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="scale"/> is outside 0–9.</exception>
    public ClickHouseDateTime64(long count, int scale, TimeSpan offset)
    {
        if (scale is < 0 or > MaxScale)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), scale, $"Scale must be in the range 0..{MaxScale}.");
        }

        this.count = count;
        this.scale = (byte)scale;
        offsetSeconds = (int)(offset.Ticks / TimeSpan.TicksPerSecond);
    }

    /// <summary>The raw signed count of ticks at <c>10^-Scale</c> seconds since the Unix epoch.</summary>
    public long Count => count;

    /// <summary>The number of fractional-second digits (0–9).</summary>
    public int Scale => scale;

    /// <summary>The offset from UTC this value is presented with.</summary>
    public TimeSpan Offset => TimeSpan.FromSeconds(offsetSeconds);

    /// <summary>Builds a value from an instant, taking the count at <paramref name="scale"/> and keeping the offset.</summary>
    /// <param name="value">The instant to convert.</param>
    /// <param name="scale">The target scale (0–9).</param>
    /// <returns>The equivalent <see cref="ClickHouseDateTime64"/> at the given scale.</returns>
    /// <remarks>At scales 7 and finer the conversion is always exact (a <see cref="DateTimeOffset"/> holds only
    /// 100 ns ticks). At coarser scales it is exact only when the instant lands on a whole unit of that scale;
    /// otherwise it would drop non-zero sub-scale digits and throws rather than truncate silently.</remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="scale"/> is outside 0–9.</exception>
    /// <exception cref="ArgumentException">The instant cannot be represented exactly at <paramref name="scale"/>.</exception>
    public static ClickHouseDateTime64 FromDateTimeOffset(DateTimeOffset value, int scale)
    {
        if (scale is < 0 or > MaxScale)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), scale, $"Scale must be in the range 0..{MaxScale}.");
        }

        long dotNetTicksSinceEpoch = value.UtcDateTime.Ticks - DateTime.UnixEpoch.Ticks;
        int places = scale - DotNetTickScale;
        long count;
        if (places >= 0)
        {
            count = ShiftDecimalPlaces(dotNetTicksSinceEpoch, places);
        }
        else
        {
            long factor = Pow10(-places);
            if (dotNetTicksSinceEpoch % factor != 0)
            {
                throw new ArgumentException(
                    $"{value:o} cannot be represented at DateTime64 scale {scale} without losing precision.",
                    nameof(value));
            }

            count = dotNetTicksSinceEpoch / factor;
        }

        return new ClickHouseDateTime64(count, scale, value.Offset);
    }

    /// <summary>The exact instant as a count of nanoseconds since the Unix epoch (no precision loss for any scale).</summary>
    /// <returns>The signed nanosecond count.</returns>
    public Int128 ToUnixTimeNanoseconds() => (Int128)count * Pow10(MaxScale - scale);

    /// <summary>
    /// Converts to a <see cref="DateTimeOffset"/> at .NET's 100 ns tick resolution, presented with <see cref="Offset"/>.
    /// </summary>
    /// <returns>The equivalent <see cref="DateTimeOffset"/>.</returns>
    /// <remarks>Lossy for scales finer than 7 (the sub-100 ns digits are truncated toward zero); use
    /// <see cref="Count"/> or <see cref="ToUnixTimeNanoseconds"/> for the exact value.</remarks>
    public DateTimeOffset ToDateTimeOffset()
    {
        long dotNetTicks = ShiftDecimalPlaces(count, DotNetTickScale - scale);
        return new DateTimeOffset(DateTime.UnixEpoch.Ticks + dotNetTicks, TimeSpan.Zero).ToOffset(Offset);
    }

    /// <inheritdoc/>
    public bool Equals(ClickHouseDateTime64 other) => ToUnixTimeNanoseconds() == other.ToUnixTimeNanoseconds();

    /// <inheritdoc/>
    public override bool Equals(object obj) => obj is ClickHouseDateTime64 other && Equals(other);

    /// <inheritdoc/>
    public int CompareTo(ClickHouseDateTime64 other) => ToUnixTimeNanoseconds().CompareTo(other.ToUnixTimeNanoseconds());

    /// <inheritdoc/>
    public override int GetHashCode() => ToUnixTimeNanoseconds().GetHashCode();

    /// <inheritdoc/>
    public override string ToString() => ToString(null, CultureInfo.InvariantCulture);

    /// <inheritdoc/>
    public string ToString(string format, IFormatProvider formatProvider)
    {
        // A fixed-point, always-invariant rendering: the calendar in the presented offset, then exactly `scale`
        // fractional-second digits taken from the exact nanosecond value, then the offset. The whole-second
        // calendar is split off with floored arithmetic so pre-epoch (negative) instants render correctly.
        Int128 nanos = ToUnixTimeNanoseconds();
        Int128 wholeSeconds = FloorDiv(nanos, NanosecondsPerSecond, out Int128 fractionNanos);
        DateTimeOffset calendar = DateTimeOffset.FromUnixTimeSeconds((long)wholeSeconds).ToOffset(Offset);

        string whole = calendar.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        string zone = calendar.ToString("zzz", CultureInfo.InvariantCulture);
        if (scale == 0)
        {
            return $"{whole} {zone}";
        }

        long fraction = (long)fractionNanos / Pow10(MaxScale - scale);
        return $"{whole}.{fraction.ToString(CultureInfo.InvariantCulture).PadLeft(scale, '0')} {zone}";
    }

    public static bool operator ==(ClickHouseDateTime64 left, ClickHouseDateTime64 right) => left.Equals(right);

    public static bool operator !=(ClickHouseDateTime64 left, ClickHouseDateTime64 right) => !left.Equals(right);

    public static bool operator <(ClickHouseDateTime64 left, ClickHouseDateTime64 right) => left.CompareTo(right) < 0;

    public static bool operator >(ClickHouseDateTime64 left, ClickHouseDateTime64 right) => left.CompareTo(right) > 0;

    public static bool operator <=(ClickHouseDateTime64 left, ClickHouseDateTime64 right) => left.CompareTo(right) <= 0;

    public static bool operator >=(ClickHouseDateTime64 left, ClickHouseDateTime64 right) => left.CompareTo(right) >= 0;

    // Scales `value` by 10^places: checked multiply when positive, truncating divide toward zero when negative.
    private static long ShiftDecimalPlaces(long value, int places)
    {
        if (places == 0)
        {
            return value;
        }

        long factor = Pow10(Math.Abs(places));
        return places > 0 ? checked(value * factor) : value / factor;
    }

    private static long Pow10(int exponent)
    {
        long result = 1;
        for (int i = 0; i < exponent; i++)
        {
            result *= 10;
        }

        return result;
    }

    // Floored division (quotient toward negative infinity) with a non-negative remainder, so a negative instant
    // splits into a whole-second part and a fractional part in [0, divisor).
    private static Int128 FloorDiv(Int128 dividend, Int128 divisor, out Int128 remainder)
    {
        Int128 quotient = dividend / divisor;
        remainder = dividend - (quotient * divisor);
        if (remainder < Int128.Zero)
        {
            quotient -= Int128.One;
            remainder += divisor;
        }

        return quotient;
    }
}
