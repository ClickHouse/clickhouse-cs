using System;
using System.Globalization;
using System.Numerics;

namespace ClickHouse.Driver.Tcp.Numerics;

/// <summary>
/// A fixed-point decimal whose unscaled value (mantissa) is a signed 256-bit integer with an associated scale
/// (the number of fractional digits): the CLR representation of ClickHouse <c>Decimal128</c> / <c>Decimal256</c>
/// values that exceed the range of <see cref="decimal"/>. The value is <c>mantissa / 10^scale</c>.
///
/// <para>
/// Equality and comparison are value-based: <c>1.0</c> and <c>1.00</c> compare equal despite different scales.
/// </para>
/// </summary>
public readonly struct ClickHouseDecimal : IEquatable<ClickHouseDecimal>, IComparable<ClickHouseDecimal>, IFormattable
{
    private readonly Int256 mantissa;
    private readonly int scale;

    /// <summary>Initializes a value from a 256-bit mantissa and a scale.</summary>
    /// <param name="mantissa">The unscaled value.</param>
    /// <param name="scale">The number of fractional digits; must be non-negative.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="scale"/> is negative.</exception>
    public ClickHouseDecimal(Int256 mantissa, int scale)
    {
        if (scale < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), scale, "Scale must be non-negative.");
        }

        this.mantissa = mantissa;
        this.scale = scale;
    }

    /// <summary>Initializes a value from a 128-bit mantissa (sign-extended to 256 bits) and a scale.</summary>
    /// <param name="mantissa">The unscaled value.</param>
    /// <param name="scale">The number of fractional digits; must be non-negative.</param>
    public ClickHouseDecimal(Int128 mantissa, int scale)
        : this(Int256.FromBigInteger(mantissa), scale)
    {
    }

    /// <summary>Initializes a value from an arbitrary-precision mantissa and a scale.</summary>
    /// <param name="mantissa">The unscaled value; must fit in a signed 256-bit integer.</param>
    /// <param name="scale">The number of fractional digits; must be non-negative.</param>
    public ClickHouseDecimal(BigInteger mantissa, int scale)
        : this(Int256.FromBigInteger(mantissa), scale)
    {
    }

    /// <summary>The unscaled value.</summary>
    public Int256 Mantissa => mantissa;

    /// <summary>The number of fractional digits.</summary>
    public int Scale => scale;

    /// <summary>The sign of the value: -1, 0, or 1.</summary>
    public int Sign => mantissa == Int256.Zero ? 0 : mantissa.IsNegative ? -1 : 1;

    /// <summary>Builds a value from a <see cref="decimal"/>, preserving its scale exactly.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns>The equivalent <see cref="ClickHouseDecimal"/>.</returns>
    public static ClickHouseDecimal FromDecimal(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        int decimalScale = (bits[3] >> 16) & 0xFF;
        bool negative = (bits[3] & unchecked((int)0x80000000)) != 0;

        BigInteger magnitude = ((BigInteger)(uint)bits[2] << 64) | ((BigInteger)(uint)bits[1] << 32) | (uint)bits[0];
        BigInteger mantissa = negative ? -magnitude : magnitude;
        return new ClickHouseDecimal(mantissa, decimalScale);
    }

    /// <summary>Converts this value to a <see cref="decimal"/>.</summary>
    /// <returns>The equivalent <see cref="decimal"/>.</returns>
    /// <exception cref="OverflowException">The value is outside the range of <see cref="decimal"/>.</exception>
    public decimal ToDecimal()
    {
        if (!TryToDecimal(out decimal value))
        {
            throw new OverflowException("The value cannot be represented as a System.Decimal.");
        }

        return value;
    }

    /// <summary>Attempts to convert this value to a <see cref="decimal"/> without throwing.</summary>
    /// <param name="value">The converted value, or zero when out of range.</param>
    /// <returns><see langword="true"/> if the value fits in a <see cref="decimal"/>.</returns>
    public bool TryToDecimal(out decimal value)
    {
        value = default;

        // System.Decimal holds a 96-bit unsigned mantissa and a scale of 0..28.
        if (scale > 28)
        {
            return false;
        }

        BigInteger m = mantissa.ToBigInteger();
        bool negative = m.Sign < 0;
        BigInteger abs = negative ? -m : m;

        byte[] bytes = abs.ToByteArray(isUnsigned: true, isBigEndian: false);
        if (bytes.Length > 12)
        {
            return false;
        }

        Span<byte> data = stackalloc byte[12];
        bytes.AsSpan().CopyTo(data);

        int lo = BitConverter.ToInt32(data.Slice(0, 4));
        int mid = BitConverter.ToInt32(data.Slice(4, 4));
        int hi = BitConverter.ToInt32(data.Slice(8, 4));

        value = new decimal(lo, mid, hi, negative, (byte)scale);
        return true;
    }

    /// <inheritdoc/>
    public bool Equals(ClickHouseDecimal other) => CompareTo(other) == 0;

    /// <inheritdoc/>
    public override bool Equals(object obj) => obj is ClickHouseDecimal other && Equals(other);

    /// <inheritdoc/>
    public int CompareTo(ClickHouseDecimal other)
    {
        // Equal-scale is the common case and stays off BigInteger: the mantissas order directly.
        if (scale == other.scale)
        {
            return mantissa.CompareTo(other.mantissa);
        }

        // Differing scales: align to the larger scale before comparing so equal values with different scales
        // compare equal. Scaling by 10^Δ can exceed 256 bits, so promote to BigInteger to avoid overflow.
        BigInteger a = mantissa.ToBigInteger();
        BigInteger b = other.mantissa.ToBigInteger();
        if (scale < other.scale)
        {
            a *= BigInteger.Pow(10, other.scale - scale);
        }
        else
        {
            b *= BigInteger.Pow(10, scale - other.scale);
        }

        return a.CompareTo(b);
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        // Hash the normalized value (trailing fractional zeros removed) so equal values hash identically.
        BigInteger m = mantissa.ToBigInteger();
        int s = scale;
        if (m.IsZero)
        {
            s = 0;
        }

        while (s > 0)
        {
            BigInteger quotient = BigInteger.DivRem(m, 10, out BigInteger remainder);
            if (!remainder.IsZero)
            {
                break;
            }

            m = quotient;
            s--;
        }

        return HashCode.Combine(m, s);
    }

    /// <inheritdoc/>
    public override string ToString() => ToString(null, CultureInfo.InvariantCulture);

    /// <inheritdoc/>
    public string ToString(string format, IFormatProvider formatProvider)
    {
        // A fixed-point rendering, always invariant: sign, integer part, then a '.' and exactly `scale` digits.
        BigInteger m = mantissa.ToBigInteger();
        bool negative = m.Sign < 0;
        BigInteger abs = negative ? -m : m;
        string digits = abs.ToString(CultureInfo.InvariantCulture);
        string sign = negative ? "-" : string.Empty;

        if (scale == 0)
        {
            return sign + digits;
        }

        if (digits.Length <= scale)
        {
            digits = digits.PadLeft(scale + 1, '0');
        }

        int pointIndex = digits.Length - scale;
        return string.Concat(sign.AsSpan(), digits.AsSpan(0, pointIndex), ".".AsSpan(), digits.AsSpan(pointIndex));
    }

    public static bool operator ==(ClickHouseDecimal left, ClickHouseDecimal right) => left.Equals(right);

    public static bool operator !=(ClickHouseDecimal left, ClickHouseDecimal right) => !left.Equals(right);

    public static bool operator <(ClickHouseDecimal left, ClickHouseDecimal right) => left.CompareTo(right) < 0;

    public static bool operator >(ClickHouseDecimal left, ClickHouseDecimal right) => left.CompareTo(right) > 0;

    public static bool operator <=(ClickHouseDecimal left, ClickHouseDecimal right) => left.CompareTo(right) <= 0;

    public static bool operator >=(ClickHouseDecimal left, ClickHouseDecimal right) => left.CompareTo(right) >= 0;

    public static explicit operator decimal(ClickHouseDecimal value) => value.ToDecimal();

    public static explicit operator ClickHouseDecimal(decimal value) => FromDecimal(value);
}
