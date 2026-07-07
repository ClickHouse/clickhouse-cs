using System;
using System.Numerics;
using ClickHouse.Driver.Tcp.Numerics;

namespace ClickHouse.Driver.Tcp.Tests.Numerics;

[TestFixture]
public class Int256Tests
{
    [Test]
    public void UInt256_ReadWrite_RoundTripsLittleEndian()
    {
        var value = UInt256.FromBigInteger((BigInteger.One << 129) + 7);
        Span<byte> buffer = stackalloc byte[UInt256.Size];
        value.WriteLittleEndian(buffer);
        Assert.That(UInt256.ReadLittleEndian(buffer), Is.EqualTo(value));
    }

    [Test]
    public void UInt256_One_IsLittleEndian()
    {
        Span<byte> buffer = stackalloc byte[UInt256.Size];
        UInt256.FromBigInteger(1).WriteLittleEndian(buffer);
        Assert.That(buffer[0], Is.EqualTo(1));
        for (int i = 1; i < UInt256.Size; i++)
        {
            Assert.That(buffer[i], Is.EqualTo(0), $"byte {i}");
        }
    }

    [Test]
    public void UInt256_Max_RoundTrips()
    {
        BigInteger max = (BigInteger.One << 256) - 1;
        var value = UInt256.FromBigInteger(max);
        Assert.That(value.ToBigInteger(), Is.EqualTo(max));
        Assert.That(value.ToString(), Is.EqualTo(max.ToString()));
    }

    [Test]
    public void UInt256_Negative_Throws()
        => Assert.Throws<OverflowException>(() => UInt256.FromBigInteger(-1));

    [Test]
    public void UInt256_Comparison_Works()
    {
        var small = UInt256.FromBigInteger(1);
        var big = UInt256.FromBigInteger(BigInteger.One << 200);
        Assert.That(small, Is.LessThan(big));
        Assert.That(big, Is.GreaterThan(small));
        Assert.That(small == UInt256.FromBigInteger(1), Is.True);
    }

    [TestCaseSource(nameof(SignedValues))]
    public void Int256_RoundTrips(string decimalValue)
    {
        BigInteger n = BigInteger.Parse(decimalValue);
        var value = Int256.FromBigInteger(n);
        Assert.That(value.ToBigInteger(), Is.EqualTo(n));
        Assert.That(value.ToString(), Is.EqualTo(decimalValue));

        Span<byte> buffer = stackalloc byte[Int256.Size];
        value.WriteLittleEndian(buffer);
        Assert.That(Int256.ReadLittleEndian(buffer), Is.EqualTo(value));
    }

    [Test]
    public void Int256_MinusOne_IsAllOnes()
    {
        Span<byte> buffer = stackalloc byte[Int256.Size];
        Int256.FromBigInteger(-1).WriteLittleEndian(buffer);
        foreach (byte b in buffer)
        {
            Assert.That(b, Is.EqualTo(0xFF));
        }
    }

    [Test]
    public void Int256_Sign_And_Comparison()
    {
        var negative = Int256.FromBigInteger(-(BigInteger.One << 100));
        var positive = Int256.FromBigInteger(BigInteger.One << 100);
        Assert.That(negative.IsNegative, Is.True);
        Assert.That(positive.IsNegative, Is.False);
        Assert.That(negative, Is.LessThan(positive));
        Assert.That(negative, Is.LessThan(Int256.Zero));
    }

    [Test]
    public void Int256_OutOfRange_Throws()
    {
        Assert.Throws<OverflowException>(() => Int256.FromBigInteger(BigInteger.One << 255));
        Assert.Throws<OverflowException>(() => Int256.FromBigInteger(-(BigInteger.One << 255) - 1));
    }

    [TestCase(64)]
    [TestCase(128)]
    [TestCase(192)]
    [TestCase(255)]
    public void UInt256_PowerOfTwo_HasBitAtExpectedLittleEndianOffset(int exponent)
    {
        var value = UInt256.FromBigInteger(BigInteger.One << exponent);
        Span<byte> buffer = stackalloc byte[UInt256.Size];
        value.WriteLittleEndian(buffer);

        int byteIndex = exponent / 8;
        byte expectedByte = (byte)(1 << (exponent % 8));
        for (int i = 0; i < UInt256.Size; i++)
        {
            Assert.That(buffer[i], Is.EqualTo(i == byteIndex ? expectedByte : (byte)0), $"byte {i}");
        }
    }

    [Test]
    public void UInt256_EqualsObject_DifferentType_ReturnsFalse()
        => Assert.That(UInt256.FromBigInteger(1).Equals("not a uint256"), Is.False);

    [Test]
    public void UInt256_CompareTo_EqualValues_ReturnsZero()
        => Assert.That(UInt256.FromBigInteger(12345).CompareTo(UInt256.FromBigInteger(12345)), Is.EqualTo(0));

    [Test]
    public void Int256_EqualsObject_DifferentType_ReturnsFalse()
        => Assert.That(Int256.FromBigInteger(-1).Equals(42), Is.False);

    [TestCase("-2", "-1", -1)]
    [TestCase("-1", "-2", 1)]
    [TestCase("-5", "-5", 0)]
    [TestCase("-1", "1", -1)]
    [TestCase("1", "-1", 1)]
    public void Int256_CompareTo_SignedOrdering(string a, string b, int expectedSign)
    {
        int cmp = Int256.FromBigInteger(BigInteger.Parse(a)).CompareTo(Int256.FromBigInteger(BigInteger.Parse(b)));
        Assert.That(Math.Sign(cmp), Is.EqualTo(expectedSign));
    }

    [Test]
    public void Int256_CompareTo_LargeSameSignNegatives_ComparesHighLimb()
    {
        var more = Int256.FromBigInteger(-(BigInteger.One << 193));
        var less = Int256.FromBigInteger(-(BigInteger.One << 192));
        Assert.That(more, Is.LessThan(less)); // -2^193 < -2^192
    }

    private static readonly string[] SignedValues =
    {
        "0",
        "1",
        "-1",
        "1606938044258990275541962092341162602522202993782792835301376", // 2^200
        "-1606938044258990275541962092341162602522202993782792835301376",
        "57896044618658097711785492504343953926634992332820282019728792003956564819967", // 2^255 - 1 (max)
        "-57896044618658097711785492504343953926634992332820282019728792003956564819968", // -2^255 (min)
    };
}
