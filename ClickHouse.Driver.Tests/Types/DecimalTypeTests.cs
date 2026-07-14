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
        // The high bytes must stay zero (relies on the stackalloc buffer being zero-initialized).
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
}
