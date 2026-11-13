using System;
using System.Numerics;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

// The ArrayColumn<decimal> (Decimal32/64) and the ArrayColumn<ClickHouseTcpDecimal> (Decimal128/256) are the storage of
// the decimal codecs, so the codec writes them, with its own scale and precision checks.
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
            : new ArrayColumn<ClickHouseTcpDecimal>("c", type, new[] { new ClickHouseTcpDecimal(BigInteger.One, 0) });

        byte[] bytes = await WriteStoredAsync(codec, column, 0, column.RowCount);

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

    [Test]
    public async Task WriteColumn_NegativeWideDecimal_SignExtendsAcrossFullWidth()
    {
        // A negative Decimal256 mantissa must sign-extend into the high limbs, not zero-fill. Asserted on the
        // encoded bytes rather than through a read-back: a read-back gives the same value when the write and the read
        // both invert the sign.
        // The value round-trip against a real server is the Decimal(76, 20) case in InsertRoundTripCase. Two's
        // complement: -2^200 is 2^256 - 2^200 = (2^56 - 1) << 200, so every bit from 200 up is set, i.e. bytes 0..24
        // are zero and bytes 25..31 are 0xFF in the little-endian 32-byte limb.
        const string type = "Decimal(76, 0)";
        var value = new ClickHouseTcpDecimal(-BigInteger.Pow(2, 200), 0);
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));

        byte[] bytes = await WriteStoredAsync(codec, new ArrayColumn<ClickHouseTcpDecimal>("c", type, new[] { value }), 0, 1);

        var expected = new byte[32];
        expected.AsSpan(25).Fill(0xFF);
        CollectionAssert.AreEqual(expected, bytes);
    }

    [Test]
    public void WriteColumn_ValueTooPreciseForScale_Throws()
    {
        const string type = "Decimal(9, 2)";
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        var column = new ArrayColumn<decimal>("c", type, new[] { 1.234m }); // 3 fractional digits into a scale-2 column

        Assert.ThrowsAsync<ArgumentException>(() => WriteStoredAsync(codec, column, 0, column.RowCount));
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
            Assert.ThrowsAsync<OverflowException>(() => WriteStoredAsync(codec, positive, 0, positive.RowCount));
            Assert.ThrowsAsync<OverflowException>(() => WriteStoredAsync(codec, negative, 0, negative.RowCount));
        });
    }

    [Test]
    public void WriteColumn_ScaledValueExceedsDeclaredPrecision_ThrowsOverflowException()
    {
        const string type = "Decimal(3, 2)";
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        var column = new ArrayColumn<decimal>("c", type, new[] { 10.00m });

        Assert.ThrowsAsync<OverflowException>(() => WriteStoredAsync(codec, column, 0, column.RowCount));
    }

    [Test]
    public void WriteColumn_WideValueDownscaledBeyondDeclaredPrecision_ThrowsOverflowException()
    {
        const string type = "Decimal(19, 2)";
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        var value = new ClickHouseTcpDecimal(BigInteger.Pow(10, 20), 3);
        var column = new ArrayColumn<ClickHouseTcpDecimal>("c", type, new[] { value });

        Assert.ThrowsAsync<OverflowException>(() => WriteStoredAsync(codec, column, 0, column.RowCount));
    }

    [Test]
    public void WriteColumn_WideValueCannotBeDownscaledExactly_ThrowsArgumentException()
    {
        const string type = "Decimal(19, 2)";
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        var value = new ClickHouseTcpDecimal(BigInteger.One, 3);
        var column = new ArrayColumn<ClickHouseTcpDecimal>("c", type, new[] { value });

        Assert.ThrowsAsync<ArgumentException>(() => WriteStoredAsync(codec, column, 0, column.RowCount));
    }

    [TestCase("Decimal(19, 0)", 19)]
    [TestCase("Decimal128(0)", 38)]
    [TestCase("Decimal256(0)", 76)]
    public void WriteColumn_WideValueExceedsDeclaredPrecision_ThrowsOverflowException(string type, int precision)
    {
        IColumnCodec codec = DecimalColumnCodec.Create(TypeParser.Parse(type));
        BigInteger mantissa = BigInteger.Pow(10, precision);
        var positive = new ArrayColumn<ClickHouseTcpDecimal>("c", type, new[] { new ClickHouseTcpDecimal(mantissa, 0) });
        var negative = new ArrayColumn<ClickHouseTcpDecimal>("c", type, new[] { new ClickHouseTcpDecimal(-mantissa, 0) });

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<OverflowException>(() => WriteStoredAsync(codec, positive, 0, positive.RowCount));
            Assert.ThrowsAsync<OverflowException>(() => WriteStoredAsync(codec, negative, 0, negative.RowCount));
        });
    }
}
