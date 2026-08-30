using System;
using System.IO;
using System.Numerics;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Tests.Types;

public class DecimalTypeTests
{
    private static byte[] Write(DecimalType type, ClickHouseDecimal value)
    {
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        type.Write(writer, value);
        writer.Flush();
        return stream.ToArray();
    }

    private static object Read(DecimalType type, byte[] bytes)
    {
        using var readStream = new MemoryStream(bytes);
        using var reader = new ExtendedBinaryReader(readStream);
        return type.Read(reader);
    }

    private static DecimalType CreateType(int size, int precision, int scale, bool useBigDecimal) => size switch
    {
        4 => new Decimal32Type { Precision = precision, Scale = scale, UseBigDecimal = useBigDecimal },
        8 => new Decimal64Type { Precision = precision, Scale = scale, UseBigDecimal = useBigDecimal },
        16 => new Decimal128Type { Precision = precision, Scale = scale, UseBigDecimal = useBigDecimal },
        32 => new Decimal256Type { Precision = precision, Scale = scale, UseBigDecimal = useBigDecimal },
        _ => throw new ArgumentOutOfRangeException(nameof(size)),
    };

    [Test]
    public void Write_WithNegativeOneScaleZero_FillsBufferWithSignBytes()
    {
        // -1 needs a single 0xFF byte; the remaining 15 bytes must be sign-extended to 0xFF
        var type = new Decimal128Type { Precision = 20, Scale = 0, UseBigDecimal = true };

        var result = Write(type, new ClickHouseDecimal(BigInteger.MinusOne, 0));

        Assert.That(result.Length, Is.EqualTo(16));
        Assert.That(result, Is.EqualTo(new byte[16]
        {
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        }));
    }

    [Test]
    public void Write_WithPositiveValueShorterThanSize_ZeroPadsHighBytes()
    {
        // 1 is a single 0x01 byte; a positive value must not be sign-extended
        var type = new Decimal128Type { Precision = 20, Scale = 0, UseBigDecimal = true };

        var result = Write(type, new ClickHouseDecimal(BigInteger.One, 0));

        Assert.That(result.Length, Is.EqualTo(16));
        Assert.That(result[0], Is.EqualTo(0x01));
        for (int i = 1; i < 16; i++)
            Assert.That(result[i], Is.EqualTo(0x00), $"byte {i}");
    }

    [Test]
    public void Write_WithPositiveValueWhoseTopBitIsSet_DoesNotSignExtend()
    {
        // 0xFF (255) has its high bit set; a naive path would emit a leading 0x00 sign byte.
        // The high bytes must stay zero (the write path explicitly zero-fills them for positives).
        var type = new Decimal128Type { Precision = 20, Scale = 0, UseBigDecimal = true };

        var result = Write(type, new ClickHouseDecimal(new BigInteger(255), 0));

        Assert.That(result.Length, Is.EqualTo(16));
        Assert.That(result[0], Is.EqualTo(0xFF));
        for (int i = 1; i < 16; i++)
            Assert.That(result[i], Is.EqualTo(0x00), $"byte {i}");
    }

    [Test]
    public void Write_WithZero_ProducesAllZeroBuffer()
    {
        var type = new Decimal128Type { Precision = 20, Scale = 0, UseBigDecimal = true };

        var result = Write(type, new ClickHouseDecimal(BigInteger.Zero, 0));

        Assert.That(result, Is.EqualTo(new byte[16]));
    }

    [TestCase(4, 9, 3)]   // Decimal32
    [TestCase(8, 18, 5)]  // Decimal64
    [TestCase(16, 38, 9)] // Decimal128
    [TestCase(32, 76, 9)] // Decimal256
    public void WriteThenRead_RoundTripsNegativeValue_PreservesValue(int size, int precision, int scale)
    {
        DecimalType type = size switch
        {
            4 => new Decimal32Type { Precision = precision, Scale = scale, UseBigDecimal = true },
            8 => new Decimal64Type { Precision = precision, Scale = scale, UseBigDecimal = true },
            16 => new Decimal128Type { Precision = precision, Scale = scale, UseBigDecimal = true },
            _ => new Decimal256Type { Precision = precision, Scale = scale, UseBigDecimal = true },
        };

        var value = new ClickHouseDecimal(new BigInteger(-1234567), scale);

        var bytes = Write(type, value);
        Assert.That(bytes.Length, Is.EqualTo(size));

        using var readStream = new MemoryStream(bytes);
        using var reader = new ExtendedBinaryReader(readStream);
        var roundTripped = type.Read(reader);

        Assert.That(roundTripped, Is.EqualTo(value));
    }

    [Test]
    public void Write_WithValueExceedingSize_ThrowsArgumentOutOfRange()
    {
        // A value larger than Decimal32's precision overflows the 4-byte buffer
        var type = new Decimal32Type { Precision = 9, Scale = 0, UseBigDecimal = true };

        var tooBig = new ClickHouseDecimal(BigInteger.Pow(10, 30), 0);

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        Assert.Throws<ArgumentOutOfRangeException>(() => type.Write(writer, tooBig));
    }

    [TestCase(4, 9, 3)]   // Decimal32
    [TestCase(8, 18, 5)]  // Decimal64
    [TestCase(16, 38, 9)] // Decimal128
    [TestCase(32, 76, 9)] // Decimal256
    public void WriteThenRead_RoundTripsPositiveValue_PreservesValue(int size, int precision, int scale)
    {
        var type = CreateType(size, precision, scale, useBigDecimal: true);

        var value = new ClickHouseDecimal(new BigInteger(1234567), scale);

        var bytes = Write(type, value);
        Assert.That(bytes.Length, Is.EqualTo(size));

        Assert.That(Read(type, bytes), Is.EqualTo(value));
    }

    [TestCase(4, 9, 3)]   // Decimal32
    [TestCase(8, 18, 5)]  // Decimal64
    [TestCase(16, 38, 9)] // Decimal128
    [TestCase(32, 76, 9)] // Decimal256
    public void WriteThenRead_RoundTripsZero_PreservesValue(int size, int precision, int scale)
    {
        var type = CreateType(size, precision, scale, useBigDecimal: true);

        var value = new ClickHouseDecimal(BigInteger.Zero, scale);

        var bytes = Write(type, value);
        Assert.That(bytes.Length, Is.EqualTo(size));

        Assert.That(Read(type, bytes), Is.EqualTo(value));
    }

    // The precision-defined max/min (±(10^precision - 1)) is the largest/smallest number the type
    // is defined to hold. Its mantissa fills the buffer right up to bytesWritten == Size, so the
    // sign-fill slice is empty and no byte is written past the mantissa. This is a legal value, not
    // an overflow, and must round-trip exactly.
    [TestCase(4, 9, 0)]   // Decimal32
    [TestCase(8, 18, 0)]  // Decimal64
    [TestCase(16, 38, 0)] // Decimal128
    [TestCase(32, 76, 0)] // Decimal256
    public void WriteThenRead_RoundTripsPrecisionCeilingMaxValue_PreservesValue(int size, int precision, int scale)
    {
        var type = CreateType(size, precision, scale, useBigDecimal: true);

        var value = type.MaxValue;

        var bytes = Write(type, value);
        Assert.That(bytes.Length, Is.EqualTo(size));

        Assert.That(Read(type, bytes), Is.EqualTo(value));
    }

    [TestCase(4, 9, 0)]   // Decimal32
    [TestCase(8, 18, 0)]  // Decimal64
    [TestCase(16, 38, 0)] // Decimal128
    [TestCase(32, 76, 0)] // Decimal256
    public void WriteThenRead_RoundTripsPrecisionFloorMinValue_PreservesValue(int size, int precision, int scale)
    {
        var type = CreateType(size, precision, scale, useBigDecimal: true);

        var value = type.MinValue;

        var bytes = Write(type, value);
        Assert.That(bytes.Length, Is.EqualTo(size));

        Assert.That(Read(type, bytes), Is.EqualTo(value));
    }

    // With UseBigDecimal = false the Read path divides the mantissa by the scale exponent and
    // returns a framework decimal instead of a ClickHouseDecimal. The write path is identical for
    // both modes, so this exercises the otherwise-untested false branch of Read.
    [TestCase(4, 9, 2)]   // Decimal32 - ReadInt32 branch
    [TestCase(8, 18, 2)]  // Decimal64 - ReadInt64 branch
    [TestCase(16, 38, 2)] // Decimal128 - BigInteger branch
    [TestCase(32, 76, 2)] // Decimal256 - BigInteger branch
    public void WriteThenRead_WithUseBigDecimalFalse_RoundTripsAsDecimal(int size, int precision, int scale)
    {
        var type = CreateType(size, precision, scale, useBigDecimal: false);

        var bytes = Write(type, new ClickHouseDecimal(new BigInteger(12345), scale));
        Assert.That(bytes.Length, Is.EqualTo(size));

        var roundTripped = Read(type, bytes);

        Assert.That(roundTripped, Is.TypeOf<decimal>());
        Assert.That(roundTripped, Is.EqualTo(123.45m));
    }

    [Test]
    public void Write_WithSameMantissaDifferentScale_ProducesIdenticalEncoding()
    {
        // WriteBigInteger serializes only the scaled mantissa; the type's Scale is not part of the
        // on-wire encoding. Two values with an identical scaled mantissa but different Scale (and
        // therefore different numeric value: 12345.67 vs 1.234567) must encode to the same bytes.
        var mantissa = new BigInteger(1234567);
        var lowScale = new Decimal64Type { Precision = 18, Scale = 2, UseBigDecimal = true };
        var highScale = new Decimal64Type { Precision = 18, Scale = 6, UseBigDecimal = true };

        var lowScaleBytes = Write(lowScale, new ClickHouseDecimal(mantissa, 2));
        var highScaleBytes = Write(highScale, new ClickHouseDecimal(mantissa, 6));

        Assert.That(lowScaleBytes, Is.EqualTo(highScaleBytes));
    }
}
