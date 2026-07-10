using System;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Power-of-ten scaling shared by the fixed-point temporal codecs (<c>DateTime64</c>, <c>Time64</c>), which
/// convert between a wire tick count at 10^-scale seconds and a .NET 100 ns tick.
/// </summary>
internal static class FixedPointScaling
{
    /// <summary>
    /// Scales <paramref name="value"/> by 10^<paramref name="places"/>: multiplying (checked) when positive,
    /// truncating-divide toward zero when negative, identity at zero.
    /// </summary>
    public static long ShiftDecimalPlaces(long value, int places)
    {
        if (places == 0)
        {
            return value;
        }

        long factor = Pow10(Math.Abs(places));
        return places > 0 ? checked(value * factor) : value / factor;
    }

    /// <summary>Returns 10^<paramref name="exponent"/> as a <see cref="long"/> (caller keeps the exponent small).</summary>
    public static long Pow10(int exponent)
    {
        long result = 1;
        for (int i = 0; i < exponent; i++)
        {
            result *= 10;
        }

        return result;
    }
}
