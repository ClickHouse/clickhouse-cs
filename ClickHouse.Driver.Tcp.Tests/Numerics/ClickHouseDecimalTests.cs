using System;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using ClickHouse.Driver.Tcp.Numerics;

namespace ClickHouse.Driver.Tcp.Tests.Numerics;

[TestFixture]
public class ClickHouseDecimalTests
{
    [TestCase("0", 0, "0")]
    [TestCase("123", 0, "123")]
    [TestCase("12345", 2, "123.45")]
    [TestCase("5", 3, "0.005")]
    [TestCase("-12345", 2, "-123.45")]
    [TestCase("-5", 3, "-0.005")]
    public void ToString_RendersFixedPointInvariant(string mantissa, int scale, string expected)
    {
        var value = new ClickHouseDecimal(BigInteger.Parse(mantissa, CultureInfo.InvariantCulture), scale);
        Assert.That(value.ToString(), Is.EqualTo(expected));
    }

    [Test]
    public void ToString_IsCultureInvariant()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE"); // uses ',' as decimal separator
            var value = new ClickHouseDecimal(new BigInteger(12345), 2);
            Assert.That(value.ToString(), Is.EqualTo("123.45"));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [TestCase("123.45")]
    [TestCase("-123.45")]
    [TestCase("0")]
    [TestCase("0.0001")]
    [TestCase("79228162514264337593543950335")] // decimal.MaxValue
    public void FromDecimal_ThenToDecimal_RoundTrips(string text)
    {
        decimal original = decimal.Parse(text, CultureInfo.InvariantCulture);
        ClickHouseDecimal wide = ClickHouseDecimal.FromDecimal(original);
        Assert.That(wide.ToDecimal(), Is.EqualTo(original));
    }

    [Test]
    public void ToDecimal_ValueBeyondDecimalRange_ThrowsAndTryReturnsFalse()
    {
        // A 20-digit fractional value cannot be a System.Decimal (max scale 28 but this mantissa exceeds 96 bits).
        var wide = new ClickHouseDecimal(BigInteger.Pow(10, 39), scale: 0);
        Assert.Multiple(() =>
        {
            Assert.That(wide.TryToDecimal(out _), Is.False);
            Assert.Throws<OverflowException>(() => wide.ToDecimal());
        });
    }

    [Test]
    public void Equals_IsValueBased_IgnoringScaleDifferences()
    {
        var oneScale1 = new ClickHouseDecimal(new BigInteger(10), 1);   // 1.0
        var oneScale2 = new ClickHouseDecimal(new BigInteger(100), 2);  // 1.00
        var fractionalScale2 = new ClickHouseDecimal(new BigInteger(123), 2);   // 1.23
        var fractionalScale3 = new ClickHouseDecimal(new BigInteger(1230), 3);  // 1.230

        Assert.Multiple(() =>
        {
            Assert.That(oneScale1, Is.EqualTo(oneScale2));
            Assert.That(oneScale1.GetHashCode(), Is.EqualTo(oneScale2.GetHashCode()));
            Assert.That(oneScale1 == oneScale2, Is.True);
            Assert.That(fractionalScale2, Is.EqualTo(fractionalScale3));
            Assert.That(fractionalScale2.GetHashCode(), Is.EqualTo(fractionalScale3.GetHashCode()));
        });
    }

    [Test]
    public void GetHashCode_EqualZeroValuesWithDifferentScales_ReturnsSameHashCode()
    {
        var zero = new ClickHouseDecimal(BigInteger.Zero, 0);
        var zeroScale1 = new ClickHouseDecimal(BigInteger.Zero, 1);
        var zeroScale2 = new ClickHouseDecimal(BigInteger.Zero, 2);
        var set = new HashSet<ClickHouseDecimal> { zero };
        var dictionary = new Dictionary<ClickHouseDecimal, string> { [zero] = "zero" };

        Assert.Multiple(() =>
        {
            Assert.That(zeroScale1, Is.EqualTo(zero));
            Assert.That(zeroScale2.GetHashCode(), Is.EqualTo(zero.GetHashCode()));
            Assert.That(set.Contains(zeroScale2), Is.True);
            Assert.That(dictionary.ContainsKey(zeroScale1), Is.True);
        });
    }

    [Test]
    public void CompareTo_AlignsScales()
    {
        var half = new ClickHouseDecimal(new BigInteger(5), 1);      // 0.5
        var twoThirds = new ClickHouseDecimal(new BigInteger(67), 2); // 0.67

        Assert.Multiple(() =>
        {
            Assert.That(half.CompareTo(twoThirds), Is.LessThan(0));
            Assert.That(twoThirds.CompareTo(half), Is.GreaterThan(0));
            Assert.That(half < twoThirds, Is.True);
        });
    }

    [Test]
    public void CompareTo_SameScale_OrdersByMantissa()
    {
        var negative = new ClickHouseDecimal(new BigInteger(-150), 2); // -1.50
        var small = new ClickHouseDecimal(new BigInteger(125), 2);     //  1.25
        var large = new ClickHouseDecimal(new BigInteger(200), 2);     //  2.00

        Assert.Multiple(() =>
        {
            Assert.That(negative.CompareTo(small), Is.LessThan(0));
            Assert.That(large.CompareTo(small), Is.GreaterThan(0));
            Assert.That(small.CompareTo(small), Is.EqualTo(0));
            Assert.That(negative < small, Is.True);
            Assert.That(large > small, Is.True);
        });
    }

    [Test]
    public void Sign_ReflectsMantissa()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new ClickHouseDecimal(new BigInteger(-1), 0).Sign, Is.EqualTo(-1));
            Assert.That(new ClickHouseDecimal(BigInteger.Zero, 5).Sign, Is.EqualTo(0));
            Assert.That(new ClickHouseDecimal(new BigInteger(1), 0).Sign, Is.EqualTo(1));
        });
    }

    [Test]
    public void Constructor_NegativeScale_Throws()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new ClickHouseDecimal(BigInteger.One, -1));
}
