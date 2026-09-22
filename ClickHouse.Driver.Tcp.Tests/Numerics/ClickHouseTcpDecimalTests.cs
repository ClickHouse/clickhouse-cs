using System;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;

namespace ClickHouse.Driver.Tcp.Tests.Numerics;

[TestFixture]
public class ClickHouseTcpDecimalTests
{
    private const int Values = 5000;

    [TestCase("0", 0, "0")]
    [TestCase("123", 0, "123")]
    [TestCase("12345", 2, "123.45")]
    [TestCase("5", 3, "0.005")]
    [TestCase("-12345", 2, "-123.45")]
    [TestCase("-5", 3, "-0.005")]
    public void ToString_RendersFixedPointInvariant(string mantissa, int scale, string expected)
    {
        var value = new ClickHouseTcpDecimal(BigInteger.Parse(mantissa, CultureInfo.InvariantCulture), scale);
        Assert.That(value.ToString(), Is.EqualTo(expected));
    }

    [Test]
    public void ToString_IsCultureInvariant()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE"); // uses ',' as decimal separator
            var value = new ClickHouseTcpDecimal(new BigInteger(12345), 2);
            Assert.That(value.ToString(), Is.EqualTo("123.45"));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("G")]
    [TestCase("g")]
    public void ToString_GeneralFormat_RendersFixedPointInvariant(string format)
    {
        var value = new ClickHouseTcpDecimal(new BigInteger(12345), 2);
        Assert.That(value.ToString(format, CultureInfo.InvariantCulture), Is.EqualTo("123.45"));
    }

    [TestCase("F3")]
    [TestCase("N2")]
    [TestCase("E")]
    [TestCase("0.00")]
    [TestCase("D")]
    public void ToString_FormatItCannotRender_ThrowsFormatException(string format)
    {
        var value = new ClickHouseTcpDecimal(new BigInteger(12345), 2);
        var ex = Assert.Throws<FormatException>(() => value.ToString(format, CultureInfo.InvariantCulture));
        Assert.That(ex.Message, Does.Contain(format).And.Contain(nameof(ClickHouseTcpDecimal.ToDecimal)));
    }

    [Test]
    public void ToString_InterpolatedWithAFormat_ThrowsRatherThanIgnoringIt()
    {
        // Interpolation reaches IFormattable, so an ignored format would silently render unrequested text.
        var value = new ClickHouseTcpDecimal(new BigInteger(12345), 2);
        Assert.Throws<FormatException>(() => _ = string.Format(CultureInfo.InvariantCulture, "{0:F3}", value));
    }

    [Test]
    public void ToString_CultureWithAnotherSeparator_IsStillInvariant()
    {
        var value = new ClickHouseTcpDecimal(new BigInteger(12345), 2);
        Assert.That(value.ToString(null, CultureInfo.GetCultureInfo("de-DE")), Is.EqualTo("123.45"));
    }

    [TestCase("123.45")]
    [TestCase("-123.45")]
    [TestCase("0")]
    [TestCase("0.0001")]
    [TestCase("79228162514264337593543950335")] // decimal.MaxValue
    public void FromDecimal_ThenToDecimal_RoundTrips(string text)
    {
        decimal original = decimal.Parse(text, CultureInfo.InvariantCulture);
        ClickHouseTcpDecimal wide = ClickHouseTcpDecimal.FromDecimal(original);
        Assert.That(wide.ToDecimal(), Is.EqualTo(original));
    }

    [Test]
    public void ToDecimal_ValueBeyondDecimalRange_ThrowsAndTryReturnsFalse()
    {
        // A 20-digit fractional value cannot be a System.Decimal (max scale 28 but this mantissa exceeds 96 bits).
        var wide = new ClickHouseTcpDecimal(BigInteger.Pow(10, 39), scale: 0);
        Assert.Multiple(() =>
        {
            Assert.That(wide.TryToDecimal(out _), Is.False);
            Assert.Throws<OverflowException>(() => wide.ToDecimal());
        });
    }

    [Test]
    public void Equals_IsValueBased_IgnoringScaleDifferences()
    {
        var oneScale1 = new ClickHouseTcpDecimal(new BigInteger(10), 1);   // 1.0
        var oneScale2 = new ClickHouseTcpDecimal(new BigInteger(100), 2);  // 1.00
        var fractionalScale2 = new ClickHouseTcpDecimal(new BigInteger(123), 2);   // 1.23
        var fractionalScale3 = new ClickHouseTcpDecimal(new BigInteger(1230), 3);  // 1.230

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
        var zero = new ClickHouseTcpDecimal(BigInteger.Zero, 0);
        var zeroScale1 = new ClickHouseTcpDecimal(BigInteger.Zero, 1);
        var zeroScale2 = new ClickHouseTcpDecimal(BigInteger.Zero, 2);
        var set = new HashSet<ClickHouseTcpDecimal> { zero };
        var dictionary = new Dictionary<ClickHouseTcpDecimal, string> { [zero] = "zero" };

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
        var half = new ClickHouseTcpDecimal(new BigInteger(5), 1);      // 0.5
        var twoThirds = new ClickHouseTcpDecimal(new BigInteger(67), 2); // 0.67

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
        var negative = new ClickHouseTcpDecimal(new BigInteger(-150), 2); // -1.50
        var small = new ClickHouseTcpDecimal(new BigInteger(125), 2);     //  1.25
        var large = new ClickHouseTcpDecimal(new BigInteger(200), 2);     //  2.00

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
            Assert.That(new ClickHouseTcpDecimal(new BigInteger(-1), 0).Sign, Is.EqualTo(-1));
            Assert.That(new ClickHouseTcpDecimal(BigInteger.Zero, 5).Sign, Is.EqualTo(0));
            Assert.That(new ClickHouseTcpDecimal(new BigInteger(1), 0).Sign, Is.EqualTo(1));
        });
    }

    [Test]
    public void Constructor_NegativeScale_Throws()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new ClickHouseTcpDecimal(BigInteger.One, -1));

    [Test]
    public void Constructor_Int128Mantissas_AllocatesNothingPerValue()
    {
        _ = new ClickHouseTcpDecimal(Int128.MaxValue, 0);

        var sign = 0;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Values; i++)
        {
            var mantissa = (i & 1) == 0 ? Int128.MinValue : Int128.MaxValue;
            sign += new ClickHouseTcpDecimal(mantissa, 0).Sign;
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Multiple(() =>
        {
            Assert.That(sign, Is.Zero);
            Assert.That(new ClickHouseTcpDecimal(Int128.MinValue, 0).Mantissa.ToBigInteger(), Is.EqualTo((BigInteger)Int128.MinValue));
            Assert.That(new ClickHouseTcpDecimal(Int128.MaxValue, 0).Mantissa.ToBigInteger(), Is.EqualTo((BigInteger)Int128.MaxValue));
            Assert.That(allocated, Is.LessThan(Values), $"constructing from Int128 must not allocate per value; allocated {allocated / (double)Values:F1} B/value");
        });
    }
}
