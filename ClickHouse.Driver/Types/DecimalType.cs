using System;
using System.Globalization;
using System.Numerics;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class DecimalType : ParameterizedType, ITypedWriter<decimal>, ITypedWriter<ClickHouseDecimal>
{
    private int scale;

    public virtual int Precision { get; init; }

    /// <summary>
    /// Gets or sets the decimal 'scale' (precision) in ClickHouse
    /// </summary>
    public int Scale
    {
        get => scale;
        set
        {
            scale = value;
            Exponent = BigInteger.Pow(10, value);
        }
    }

    /// <summary>
    /// Gets decimal exponent value based on Scale
    /// </summary>
    public BigInteger Exponent { get; private set; }

    public override string Name => "Decimal";

    /// <summary>
    /// Gets size of type in bytes
    /// </summary>
    public virtual int Size => GetSizeFromPrecision(Precision);

    public override Type FrameworkType => UseBigDecimal ? typeof(ClickHouseDecimal) : typeof(decimal);

    public ClickHouseDecimal MaxValue => new(BigInteger.Pow(10, Precision) - 1, Scale);

    public ClickHouseDecimal MinValue => new(1 - BigInteger.Pow(10, Precision), Scale);

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        var precision = int.Parse(node.ChildNodes[0].Value, CultureInfo.InvariantCulture);
        var scale = int.Parse(node.ChildNodes[1].Value, CultureInfo.InvariantCulture);

        var size = GetSizeFromPrecision(precision);

        return size switch
        {
            4 => new Decimal32Type { Precision = precision, Scale = scale, UseBigDecimal = settings.useBigDecimal },
            8 => new Decimal64Type { Precision = precision, Scale = scale, UseBigDecimal = settings.useBigDecimal },
            16 => new Decimal128Type { Precision = precision, Scale = scale, UseBigDecimal = settings.useBigDecimal },
            32 => new Decimal256Type { Precision = precision, Scale = scale, UseBigDecimal = settings.useBigDecimal },
            _ => new DecimalType { Precision = precision, Scale = scale, UseBigDecimal = settings.useBigDecimal },
        };
    }

    public override object Read(ExtendedBinaryReader reader)
    {
        if (UseBigDecimal)
        {
            var mantissa = Size switch
            {
                4 => (BigInteger)reader.ReadInt32(),
                8 => (BigInteger)reader.ReadInt64(),
                _ => new BigInteger(reader.ReadBytes(Size)),
            };
            return new ClickHouseDecimal(mantissa, Scale);
        }
        else
        {
            var mantissa = Size switch
            {
                4 => reader.ReadInt32(),
                8 => reader.ReadInt64(),
                _ => (decimal)new BigInteger(reader.ReadBytes(Size)),
            };
            return mantissa / (decimal)Exponent;
        }
    }

    public override string ToString() => $"{Name}({Precision}, {Scale})";

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        try
        {
            ClickHouseDecimal @decimal = value is ClickHouseDecimal chd ? chd : Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            WriteScaled(writer, @decimal);
        }
        catch (OverflowException)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, $"Value cannot be represented");
        }
    }

    public void WriteValue(ExtendedBinaryWriter writer, decimal value) => WriteValue(writer, (ClickHouseDecimal)value);

    public void WriteValue(ExtendedBinaryWriter writer, ClickHouseDecimal value)
    {
        try
        {
            WriteScaled(writer, value);
        }
        catch (OverflowException)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, $"Value cannot be represented");
        }
    }

    private void WriteScaled(ExtendedBinaryWriter writer, ClickHouseDecimal value)
    {
        var mantissa = ClickHouseDecimal.ScaleMantissa(value, Scale);
        WriteBigInteger(writer, mantissa);
    }

    internal virtual bool UseBigDecimal { get; init; }

    private static int GetSizeFromPrecision(int precision) => precision switch
    {
        int p when p >= 1 && p <= 9 => 4,
        int p when p >= 10 && p <= 18 => 8,
        int p when p >= 19 && p <= 38 => 16,
        int p when p >= 39 && p <= 76 => 32,
        _ => throw new ArgumentOutOfRangeException(nameof(precision)),
    };

    private void WriteBigInteger(ExtendedBinaryWriter writer, BigInteger value)
    {
        // Size is always one of {4, 8, 16, 32} (see GetSizeFromPrecision), so a stack
        // buffer avoids both the BigInteger.ToByteArray() and the new byte[Size] allocation.
        Span<byte> decimalBytes = stackalloc byte[Size];

        if (!value.TryWriteBytes(decimalBytes, out int bytesWritten))
            throw new OverflowException($"Trying to write {value.GetByteCount()} bytes, at most {Size} expected");

        // Fill the bytes past the mantissa: 0xFF to sign-extend a negative value, 0x00 for a
        // positive one. The positive case is explicit rather than relying on the stackalloc being
        // zero-initialized (guaranteed only while the compiler emits localsinit; a future
        // [SkipLocalsInit] would leave stack garbage in the high bytes and corrupt the value).
        decimalBytes.Slice(bytesWritten).Fill(value.Sign < 0 ? (byte)0xFF : (byte)0x00);
        writer.Write(decimalBytes);
    }
}
