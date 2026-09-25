using System;
using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for a ClickHouse <c>Decimal(P, S)</c> column (and the <c>Decimal32/64/128/256</c> aliases). The value
/// is a signed two's-complement integer mantissa of a precision-dependent backing width, read little-endian, and
/// the logical value is <c>mantissa / 10^S</c>. Backing width follows the precision <c>P</c>: 1–9 → 4 bytes,
/// 10–18 → 8 bytes, 19–38 → 16 bytes, 39–76 → 32 bytes.
///
/// <para>
/// The 4- and 8-byte widths surface as <see cref="decimal"/>; the wider ones, which exceed the range of
/// <see cref="decimal"/>, surface as <see cref="ClickHouseDecimal"/>. The mantissa bytes are read in one bulk
/// transfer, then converted to the CLR value.
/// </para>
/// </summary>
/// <typeparam name="TMantissa">The unmanaged backing integer (int, long, Int128, or Int256).</typeparam>
/// <typeparam name="TValue">The CLR value type (decimal or ClickHouseDecimal).</typeparam>
internal sealed class DecimalColumnCodec<TMantissa, TValue> : IColumnCodec
    where TMantissa : unmanaged, IComparable<TMantissa>
{
    private readonly int precision;
    private readonly int scale;
    private readonly TMantissa minMantissa;
    private readonly TMantissa maxMantissa;
    private readonly Func<TMantissa, int, TValue> decode;
    private readonly Func<TValue, int, TMantissa> encode;

    /// <summary>Initializes a decimal codec of a fixed backing width, precision, and scale.</summary>
    public DecimalColumnCodec(
        string typeName,
        int precision,
        int scale,
        TMantissa minMantissa,
        TMantissa maxMantissa,
        Func<TMantissa, int, TValue> decode,
        Func<TValue, int, TMantissa> encode)
    {
        TypeName = typeName;
        this.precision = precision;
        this.scale = scale;
        this.minMantissa = minMantissa;
        this.maxMantissa = maxMantissa;
        this.decode = decode;
        this.encode = encode;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(TValue);

    /// <inheritdoc/>
    public object NullPlaceholder => default(TValue);

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        int s = scale;
        Func<TMantissa, int, TValue> decodeValue = decode;
        return ArrayColumn<TValue>.ReadAsync(reader, columnName, columnType, rowCount, checked(rowCount * Unsafe.SizeOf<TMantissa>()), Fill, cancellationToken);

        void Fill(ReadOnlySpan<byte> source, Span<TValue> destination)
        {
            ReadOnlySpan<TMantissa> mantissas = MemoryMarshal.Cast<byte, TMantissa>(source);
            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = decodeValue(mantissas[i], s);
            }
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<TValue>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (length == 0)
        {
            return;
        }

        // Read per element through the indexer so a scattered write-path view writes with no materialized copy.
        // Rent the mantissa scratch rather than allocating per call; the rented array may be larger, so only the
        // populated prefix is written.
        var typed = (IColumn<TValue>)column;
        TMantissa[] mantissas = ArrayPool<TMantissa>.Shared.Rent(length);
        try
        {
            for (int i = 0; i < length; i++)
            {
                TMantissa mantissa = encode(typed[start + i], scale);
                if (mantissa.CompareTo(minMantissa) < 0 || mantissa.CompareTo(maxMantissa) > 0)
                {
                    throw new OverflowException($"Value at index {i} exceeds the declared precision {precision} of decimal type '{TypeName}'.");
                }

                mantissas[i] = mantissa;
            }

            writer.WriteBytes(MemoryMarshal.AsBytes<TMantissa>(mantissas.AsSpan(0, length)));
        }
        finally
        {
            ArrayPool<TMantissa>.Shared.Return(mantissas);
        }
    }
}

/// <summary>Builds the decimal codec for a <c>Decimal(...)</c> type node, selecting the backing width from precision.</summary>
internal static class DecimalColumnCodec
{
    /// <summary>Builds the codec for a <c>Decimal</c> or <c>Decimal32/64/128/256</c> type node.</summary>
    /// <param name="node">The parsed decimal type node.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The precision/scale arguments are missing, malformed, or out of range.</exception>
    public static IColumnCodec Create(TypeNode node)
    {
        int precision;
        int scale;
        int width;
        switch (node.Name)
        {
            case "Decimal":
                precision = ParseArg(node, 0, "precision");
                scale = ParseArg(node, 1, "scale");
                width = WidthForPrecision(node, precision);
                break;
            case "Decimal32":
                precision = 9;
                scale = ParseArg(node, 0, "scale");
                width = 4;
                break;
            case "Decimal64":
                precision = 18;
                scale = ParseArg(node, 0, "scale");
                width = 8;
                break;
            case "Decimal128":
                precision = 38;
                scale = ParseArg(node, 0, "scale");
                width = 16;
                break;
            case "Decimal256":
                precision = 76;
                scale = ParseArg(node, 0, "scale");
                width = 32;
                break;
            default:
                throw new FormatException($"'{node.Name}' is not a decimal type.");
        }

        if (scale is < 0 || scale > precision)
        {
            throw new FormatException($"Decimal type '{node}' has scale {scale} outside the supported range 0..{precision}.");
        }

        string typeName = node.ToString();
        BigInteger maxMantissa = BigInteger.Pow(10, precision) - BigInteger.One;
        BigInteger minMantissa = -maxMantissa;
        return width switch
        {
            4 => new DecimalColumnCodec<int, decimal>(typeName, precision, scale, (int)minMantissa, (int)maxMantissa, DecimalConvert.DecodeInt32, DecimalConvert.EncodeInt32),
            8 => new DecimalColumnCodec<long, decimal>(typeName, precision, scale, (long)minMantissa, (long)maxMantissa, DecimalConvert.DecodeInt64, DecimalConvert.EncodeInt64),
            16 => new DecimalColumnCodec<Int128, ClickHouseDecimal>(typeName, precision, scale, Int128.CreateChecked(minMantissa), Int128.CreateChecked(maxMantissa), DecimalConvert.DecodeInt128, DecimalConvert.EncodeInt128),
            _ => new DecimalColumnCodec<Int256, ClickHouseDecimal>(typeName, precision, scale, Int256.FromBigInteger(minMantissa), Int256.FromBigInteger(maxMantissa), DecimalConvert.DecodeInt256, DecimalConvert.EncodeInt256),
        };
    }

    private static int WidthForPrecision(TypeNode node, int precision) => precision switch
    {
        >= 1 and <= 9 => 4,
        >= 10 and <= 18 => 8,
        >= 19 and <= 38 => 16,
        >= 39 and <= 76 => 32,
        _ => throw new FormatException($"Decimal type '{node}' has precision {precision} outside the supported range 1..76."),
    };

    private static int ParseArg(TypeNode node, int index, string what)
    {
        if (node.Arguments.Count <= index || !int.TryParse(node.Arguments[index].Name.Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int value))
        {
            throw new FormatException($"Decimal type '{node}' is missing its {what} argument or it is not an integer.");
        }

        return value;
    }
}

/// <summary>Mantissa↔CLR-value conversions for the decimal codecs, keyed by backing width.</summary>
internal static class DecimalConvert
{
    public static decimal DecodeInt32(int mantissa, int scale) => MakeDecimal(mantissa, scale);

    public static decimal DecodeInt64(long mantissa, int scale) => MakeDecimal(mantissa, scale);

    public static int EncodeInt32(decimal value, int scale)
    {
        long m = EncodeDecimalMantissa(value, scale);
        if (m is < int.MinValue or > int.MaxValue)
        {
            throw new OverflowException($"Value {value} does not fit in a Decimal32 (scale {scale}).");
        }

        return (int)m;
    }

    public static long EncodeInt64(decimal value, int scale) => EncodeDecimalMantissa(value, scale);

    public static ClickHouseDecimal DecodeInt128(Int128 mantissa, int scale) => new(mantissa, scale);

    public static ClickHouseDecimal DecodeInt256(Int256 mantissa, int scale) => new(mantissa, scale);

    public static Int128 EncodeInt128(ClickHouseDecimal value, int scale) => Int128.CreateChecked(RescaleMantissa(value, scale));

    public static Int256 EncodeInt256(ClickHouseDecimal value, int scale) => Int256.FromBigInteger(RescaleMantissa(value, scale));

    private static decimal MakeDecimal(long mantissa, int scale)
    {
        bool negative = mantissa < 0;
        ulong magnitude = negative ? (ulong)(-(BigInteger)mantissa) : (ulong)mantissa;
        int lo = unchecked((int)(magnitude & 0xFFFFFFFF));
        int mid = unchecked((int)(magnitude >> 32));
        return new decimal(lo, mid, 0, negative, (byte)scale);
    }

    private static long EncodeDecimalMantissa(decimal value, int scale)
    {
        // Shift the value to an integer at the column's scale; reject a value that carries more fractional
        // precision than the column can hold rather than silently truncating it.
        decimal factor = 1m;
        for (int i = 0; i < scale; i++)
        {
            factor *= 10m;
        }

        decimal shifted = value * factor;
        decimal truncated = Math.Truncate(shifted);
        if (truncated != shifted)
        {
            throw new ArgumentException($"Value {value} cannot be represented exactly at scale {scale}.");
        }

        return (long)truncated;
    }

    private static BigInteger RescaleMantissa(ClickHouseDecimal value, int scale)
    {
        BigInteger mantissa = value.Mantissa.ToBigInteger();
        int diff = scale - value.Scale;
        if (diff == 0)
        {
            return mantissa;
        }

        if (diff > 0)
        {
            return mantissa * BigInteger.Pow(10, diff);
        }

        BigInteger quotient = BigInteger.DivRem(mantissa, BigInteger.Pow(10, -diff), out BigInteger remainder);
        if (!remainder.IsZero)
        {
            throw new ArgumentException($"Value {value} cannot be represented exactly at scale {scale}.");
        }

        return quotient;
    }
}
