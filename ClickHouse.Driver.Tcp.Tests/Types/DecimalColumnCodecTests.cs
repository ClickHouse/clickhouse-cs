using System;
using System.Numerics;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class DecimalColumnCodecTests
{
    [TestCase("Decimal(9, 2)", 4)]
    [TestCase("Decimal(18, 4)", 8)]
    [TestCase("Decimal(38, 10)", 16)]
    [TestCase("Decimal(76, 20)", 32)]
    [TestCase("Decimal32(2)", 4)]
    [TestCase("Decimal64(4)", 8)]
    [TestCase("Decimal128(10)", 16)]
    [TestCase("Decimal256(20)", 32)]
    public async Task WriteColumn_BackingWidthMatchesPrecision(string type, int bytesPerValue)
    {
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        IColumn column = bytesPerValue <= 8
            ? new ArrayColumn<decimal>("c", type, new[] { 1m })
            : new ArrayColumn<ClickHouseDecimal>("c", type, new[] { new ClickHouseDecimal(BigInteger.One, 0) });

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));

        Assert.That(bytes.Length, Is.EqualTo(bytesPerValue));
    }

    [Test]
    public void Create_PrecisionOutOfRange_Throws()
        => Assert.Throws<FormatException>(() => DecimalColumnCodec.Create(TypeParser.Parse("Decimal(0, 0)")));

    [TestCase("Decimal(9, -1)")]
    [TestCase("Decimal32(-1)")]
    [TestCase("Decimal(9, 10)")]
    [TestCase("Decimal32(10)")]
    [TestCase("Decimal64(19)")]
    [TestCase("Decimal128(39)")]
    [TestCase("Decimal256(77)")]
    public void Create_ScaleOutsidePrecisionRange_ThrowsFormatException(string type)
        => Assert.Throws<FormatException>(() => DecimalColumnCodec.Create(TypeParser.Parse(type)));

    [TestCase("Decimal(9, 9)")]
    [TestCase("Decimal32(9)")]
    [TestCase("Decimal64(18)")]
    [TestCase("Decimal128(38)")]
    [TestCase("Decimal256(76)")]
    public void Create_ScaleEqualsPrecision_CreatesCodec(string type)
        => Assert.DoesNotThrow(() => DecimalColumnCodec.Create(TypeParser.Parse(type)));

    [TestCase("Decimal(9, 2)")]
    [TestCase("Decimal(18, 6)")]
    public async Task RoundTrip_SmallDecimal_PreservesValue(string type)
    {
        var values = new[] { 0m, 1.23m, -1.23m, 100.5m };
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));

        using var column = (IColumn<decimal>)await RoundTripAsync(codec, new ArrayColumn<decimal>("c", type, values), type, values.Length);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }

    [Test]
    public async Task RoundTrip_WideDecimal_PreservesValueAndSign()
    {
        const string type = "Decimal(38, 4)";
        var values = new[]
        {
            new ClickHouseDecimal(BigInteger.Zero, 4),
            new ClickHouseDecimal(new BigInteger(123456789), 4),
            new ClickHouseDecimal(new BigInteger(-123456789), 4),
        };
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));

        using var column = (IColumn<ClickHouseDecimal>)await RoundTripAsync(codec, new ArrayColumn<ClickHouseDecimal>("c", type, values), type, values.Length);

        Assert.Multiple(() =>
        {
            for (int i = 0; i < values.Length; i++)
            {
                Assert.That(column[i], Is.EqualTo(values[i]));
            }
        });
    }

    [Test]
    public async Task RoundTrip_NegativeWideDecimal_SignExtendsAcrossFullWidth()
    {
        // A negative Decimal256 mantissa must sign-extend into the high limbs, not zero-fill.
        const string type = "Decimal(76, 0)";
        var value = new ClickHouseDecimal(-BigInteger.Pow(2, 200), 0);
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));

        using var column = (IColumn<ClickHouseDecimal>)await RoundTripAsync(codec, new ArrayColumn<ClickHouseDecimal>("c", type, new[] { value }), type, 1);

        Assert.That(column[0], Is.EqualTo(value));
    }

    [Test]
    public void WriteColumn_ValueTooPreciseForScale_Throws()
    {
        const string type = "Decimal(9, 2)";
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        var column = new ArrayColumn<decimal>("c", type, new[] { 1.234m }); // 3 fractional digits into a scale-2 column

        Assert.ThrowsAsync<ArgumentException>(async () => await WriteAsync(w => codec.WriteColumn(w, column)));
    }

    [TestCase("Decimal(1, 0)", 99L)]
    [TestCase("Decimal32(0)", 1_000_000_000L)]
    [TestCase("Decimal64(0)", 1_000_000_000_000_000_000L)]
    public void WriteColumn_ValueExceedsDeclaredPrecision_ThrowsOverflowException(string type, long value)
    {
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        var positive = new ArrayColumn<decimal>("c", type, new[] { (decimal)value });
        var negative = new ArrayColumn<decimal>("c", type, new[] { -(decimal)value });

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<OverflowException>(async () => await WriteAsync(w => codec.WriteColumn(w, positive)));
            Assert.ThrowsAsync<OverflowException>(async () => await WriteAsync(w => codec.WriteColumn(w, negative)));
        });
    }

    [Test]
    public void WriteColumn_ScaledValueExceedsDeclaredPrecision_ThrowsOverflowException()
    {
        const string type = "Decimal(3, 2)";
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        var column = new ArrayColumn<decimal>("c", type, new[] { 10.00m });

        Assert.ThrowsAsync<OverflowException>(async () => await WriteAsync(w => codec.WriteColumn(w, column)));
    }

    [Test]
    public async Task WriteColumn_WideValueDownscaledToPrecisionBoundary_WritesValue()
    {
        const string type = "Decimal(19, 2)";
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        BigInteger boundary = BigInteger.Pow(10, 19) - BigInteger.One;
        var value = new ClickHouseDecimal(boundary * 10, 3);
        var column = new ArrayColumn<ClickHouseDecimal>("c", type, new[] { value });

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));

        Assert.That(bytes, Has.Length.EqualTo(16));
    }

    [Test]
    public void WriteColumn_WideValueDownscaledBeyondDeclaredPrecision_ThrowsOverflowException()
    {
        const string type = "Decimal(19, 2)";
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        var value = new ClickHouseDecimal(BigInteger.Pow(10, 20), 3);
        var column = new ArrayColumn<ClickHouseDecimal>("c", type, new[] { value });

        Assert.ThrowsAsync<OverflowException>(async () => await WriteAsync(w => codec.WriteColumn(w, column)));
    }

    [Test]
    public void WriteColumn_WideValueCannotBeDownscaledExactly_ThrowsArgumentException()
    {
        const string type = "Decimal(19, 2)";
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        var value = new ClickHouseDecimal(BigInteger.One, 3);
        var column = new ArrayColumn<ClickHouseDecimal>("c", type, new[] { value });

        Assert.ThrowsAsync<ArgumentException>(async () => await WriteAsync(w => codec.WriteColumn(w, column)));
    }

    [TestCase("Decimal(19, 0)", 19)]
    [TestCase("Decimal128(0)", 38)]
    [TestCase("Decimal256(0)", 76)]
    public void WriteColumn_WideValueExceedsDeclaredPrecision_ThrowsOverflowException(string type, int precision)
    {
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        BigInteger mantissa = BigInteger.Pow(10, precision);
        var positive = new ArrayColumn<ClickHouseDecimal>("c", type, new[] { new ClickHouseDecimal(mantissa, 0) });
        var negative = new ArrayColumn<ClickHouseDecimal>("c", type, new[] { new ClickHouseDecimal(-mantissa, 0) });

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<OverflowException>(async () => await WriteAsync(w => codec.WriteColumn(w, positive)));
            Assert.ThrowsAsync<OverflowException>(async () => await WriteAsync(w => codec.WriteColumn(w, negative)));
        });
    }

    [TestCase("Decimal(1, 0)", 1, 4)]
    [TestCase("Decimal32(0)", 9, 4)]
    [TestCase("Decimal64(0)", 18, 8)]
    public async Task WriteColumn_ValueAtDeclaredPrecisionBoundary_WritesValue(string type, int precision, int bytesPerValue)
    {
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        decimal boundary = decimal.Parse(new string('9', precision), System.Globalization.CultureInfo.InvariantCulture);
        var column = new ArrayColumn<decimal>("c", type, new[] { boundary, -boundary });

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));

        Assert.That(bytes, Has.Length.EqualTo(2 * bytesPerValue));
    }

    [TestCase("Decimal(19, 0)", 19, 16)]
    [TestCase("Decimal128(0)", 38, 16)]
    [TestCase("Decimal256(0)", 76, 32)]
    public async Task WriteColumn_WideValueAtDeclaredPrecisionBoundary_WritesValue(string type, int precision, int bytesPerValue)
    {
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        BigInteger boundary = BigInteger.Pow(10, precision) - BigInteger.One;
        var column = new ArrayColumn<ClickHouseDecimal>(
            "c",
            type,
            new[] { new ClickHouseDecimal(boundary, 0), new ClickHouseDecimal(-boundary, 0) });

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));

        Assert.That(bytes, Has.Length.EqualTo(2 * bytesPerValue));
    }

    [Test]
    public void CanWrite_MatchesValueType()
    {
        IColumnCodec small = DecimalColumnCodec.Create(TypeParser.Parse("Decimal(9, 2)"));
        IColumnCodec wide = DecimalColumnCodec.Create(TypeParser.Parse("Decimal(38, 2)"));

        Assert.Multiple(() =>
        {
            Assert.That(small.CanWrite(new ArrayColumn<decimal>("c", "Decimal(9, 2)", Array.Empty<decimal>())), Is.True);
            Assert.That(small.CanWrite(new ArrayColumn<ClickHouseDecimal>("c", "Decimal(9, 2)", Array.Empty<ClickHouseDecimal>())), Is.False);
            Assert.That(wide.CanWrite(new ArrayColumn<ClickHouseDecimal>("c", "Decimal(38, 2)", Array.Empty<ClickHouseDecimal>())), Is.True);
            Assert.That(wide.CanWrite(new ArrayColumn<decimal>("c", "Decimal(38, 2)", Array.Empty<decimal>())), Is.False);
        });
    }
}
