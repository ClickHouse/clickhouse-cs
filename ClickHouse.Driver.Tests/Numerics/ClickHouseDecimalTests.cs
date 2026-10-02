using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Numerics;

[Parallelizable(ParallelScope.All)]
[Category("ClickHouseDecimal")]
[TestFixture]
public class ClickHouseDecimalTests
{
    static ClickHouseDecimalTests()
    {
    }

    public static readonly decimal[] Decimals =
    [
        -1000000000000m,
        -5478689523m,
        -459m,
        -1.234m,
        -0.7777m,
        -0.00000000001m,
        0,
        0.000000000001m,
        0.000003m,
        0.1m,
        0.19374596m,
        1.0m,
        1.000m,
        2.0m,
        3.14159265359m,
        10,
        1000000,
        1000000000000m,
    ];

    public static readonly decimal[] DecimalsWithoutZero = Decimals.Where(d => d != 0).ToArray();

    public static readonly decimal[] DecimalsWithExtremeValues = Decimals.Union(
        [decimal.MinValue, decimal.MinValue / 100000m, decimal.MaxValue, decimal.MaxValue / 100000m]).ToArray();

    public static readonly string[] LongDecimalStrings =
    [
        new string('1', 100),
        "3.141592653589793238462643383"
    ];

    public static void AssertAreEqualWithDelta(decimal left, decimal right)
    {
        var magic = 0.000000000000000000000000001m;
        var delta = Math.Abs(left - right);
        var noticeableDiff = Math.Max(Math.Abs(left), Math.Abs(right)) * magic;
        noticeableDiff = Math.Max(noticeableDiff, magic);

        if (delta > noticeableDiff)
            Assert.That(right, Is.EqualTo(left));
    }

    public static readonly CultureInfo[] Cultures =
    [
        CultureInfo.InvariantCulture,
        CultureInfo.GetCultureInfo("en-US"),
        CultureInfo.GetCultureInfo("zh-CN"),
        CultureInfo.GetCultureInfo("ru-RU"),
        CultureInfo.GetCultureInfo("ar-SA"),
    ];

    [Test]
    [TestCase(0.001, ExpectedResult = 3)]
    [TestCase(0.01, ExpectedResult = 2)]
    [TestCase(0.1, ExpectedResult = 1)]
    [TestCase(1, ExpectedResult = 0)]
    [TestCase(10, ExpectedResult = 0)]
    public int ShouldNormalizeScale(decimal @decimal) => new ClickHouseDecimal(@decimal).Scale;

    [Test]
    [TestCase(0.001, ExpectedResult = 1)]
    [TestCase(0.01, ExpectedResult = 1)]
    [TestCase(0.1, ExpectedResult = 1)]
    [TestCase(1, ExpectedResult = 1)]
    [TestCase(10, ExpectedResult = 10)]
    public long ShouldNormalizeMantissa(decimal value) => (long)((ClickHouseDecimal)value).Mantissa;

    [Test]
    [TestCase(12.345, 1, ExpectedResult = 12)]
    [TestCase(12.345, 2, ExpectedResult = 12)]
    [TestCase(12.345, 3, ExpectedResult = 12.3)]
    [TestCase(12.345, 4, ExpectedResult = 12.34)]
    [TestCase(12.345, 5, ExpectedResult = 12.345)]
    public decimal ShouldTruncate(decimal value, int precision) => (decimal)new ClickHouseDecimal(value).Truncate(precision);

    [Test]
    public void ShouldValidateBuiltinValues()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClickHouseDecimal.Zero, Is.EqualTo(new ClickHouseDecimal(0m)));
            Assert.That(ClickHouseDecimal.One, Is.EqualTo(new ClickHouseDecimal(1m)));
        });
    }

    [Test]
    public void ShouldRoundtripConversion([ValueSource(typeof(ClickHouseDecimalTests), nameof(DecimalsWithExtremeValues))] decimal value)
    {
        var result = new ClickHouseDecimal(value);
        Assert.That((decimal)result, Is.EqualTo(value));
    }

    [Test]
    [TestCase("1", 31)]
    [TestCase("1234500000000000000000000000000", 60)]
    [TestCase("123456789012345678901234567890", 0)]
    public void ExplicitDecimalConversion_ValueNotRepresentable_ThrowsOverflowException(string mantissa, int scale)
    {
        var value = new ClickHouseDecimal(BigInteger.Parse(mantissa, CultureInfo.InvariantCulture), scale);
        Assert.Throws<OverflowException>(() => _ = (decimal)value);
    }

    [Test, Combinatorial]
    public void ShouldAdd(
        [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
        [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal right)
    {
        decimal expected = left + right;
        var actual = (ClickHouseDecimal)left + (ClickHouseDecimal)right;
        Assert.That((decimal)actual, Is.EqualTo(expected));
    }

    [Test, Combinatorial]
    public void ShouldFormat([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal value,
                                [ValueSource(typeof(ClickHouseDecimalTests), nameof(Cultures))] CultureInfo culture)
    {
        var expected = value.ToString(culture);
        var actual = ((ClickHouseDecimal)value).ToString(culture);
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test, Combinatorial]
    public void ShouldParse([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal expected,
                                [ValueSource(typeof(ClickHouseDecimalTests), nameof(Cultures))] CultureInfo culture)
    {
        var actual = (decimal)ClickHouseDecimal.Parse(expected.ToString(culture), culture);
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    [TestCaseSource(typeof(ClickHouseDecimalTests), nameof(LongDecimalStrings))]
    public void ShouldParseLarge(string input)
    {
        var actual = ClickHouseDecimal.Parse(input);
        Assert.That(actual.ToString(CultureInfo.InvariantCulture), Is.EqualTo(input));
    }

    [Test, Combinatorial]
    public void ShouldSubtract([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
                                [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal right)
    {
        decimal expected = left - right;
        var actual = (ClickHouseDecimal)left - (ClickHouseDecimal)right;
        Assert.That((decimal)actual, Is.EqualTo(expected));
    }

    [Test, Combinatorial]
    public void ShouldMultiply([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
                                [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal right)
    {
        decimal expected = left * right;
        var actual = (ClickHouseDecimal)left * (ClickHouseDecimal)right;
        Assert.That((decimal)actual, Is.EqualTo(expected));
    }

    [Test, Combinatorial]
    public void ShouldDivide([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
                                [ValueSource(typeof(ClickHouseDecimalTests), nameof(DecimalsWithoutZero))] decimal right)
    {

        decimal expected = left / right;
        var scale = GetScale(expected);

        var actual = (ClickHouseDecimal)left / (ClickHouseDecimal)right;
        actual = new ClickHouseDecimal(ClickHouseDecimal.ScaleMantissa(actual, GetScale(expected)), scale);

        AssertAreEqualWithDelta(expected, (decimal)actual);
    }

    [Test, Combinatorial]
    public void ShouldDivideWithRemainder([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
                                [ValueSource(typeof(ClickHouseDecimalTests), nameof(DecimalsWithoutZero))] decimal right)
    {
        decimal expected = left % right;
        var actual = (ClickHouseDecimal)left % (ClickHouseDecimal)right;
        Assert.That((decimal)actual, Is.EqualTo(expected));
    }

    [Test, Combinatorial]
    public void ShouldCompare([ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal left,
                                [ValueSource(typeof(ClickHouseDecimalTests), nameof(Decimals))] decimal right)
    {
        int expected = left.CompareTo(right);
        int actual = ((ClickHouseDecimal)left).CompareTo((ClickHouseDecimal)right);
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    [TestCase(-5e20)]
    [TestCase(-1000.0)]
    [TestCase(-Math.E)]
    [TestCase(-1.0)]
    [TestCase(-0.0)]
    [TestCase(0.0)]
    [TestCase(Math.PI)]
    [TestCase(1000)]
    [TestCase(5e20)]
    public void ShouldRoundtripIntoDouble(double @double)
    {
        ClickHouseDecimal @decimal = @double;
        Assert.That(@decimal.ToDouble(CultureInfo.InvariantCulture), Is.EqualTo(@double));
    }

    [Test]
    [TestCase(typeof(bool))]
    [TestCase(typeof(byte))]
    [TestCase(typeof(sbyte))]
    [TestCase(typeof(short))]
    [TestCase(typeof(ushort))]
    [TestCase(typeof(int))]
    [TestCase(typeof(uint))]
    [TestCase(typeof(long))]
    [TestCase(typeof(ulong))]
    [TestCase(typeof(float))]
    [TestCase(typeof(double))]
    [TestCase(typeof(decimal))]
    [TestCase(typeof(string))]
    public void ShouldConvertToType(Type type)
    {
        var expected = Convert.ChangeType(5.00m, type);
        var actual = Convert.ChangeType(new ClickHouseDecimal(5.00m), type);
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public void ShouldConvertToBigInteger()
    {
        var expected = new BigInteger(123);
        var actual = new ClickHouseDecimal(123.45m).ToType(typeof(BigInteger), CultureInfo.InvariantCulture);
        Assert.That(actual, Is.EqualTo(expected));
    }

    // The fractional part is truncated toward zero, then the integer part must fit the target type,
    // as the server does: toUInt8(toDecimal32(255.9, 1)) is 255, toUInt8(toDecimal32(256.5, 1)) overflows.
    public static IEnumerable<TestCaseData> NarrowIntegralInRangeCases()
    {
        yield return new TestCaseData(new ClickHouseDecimal(-128m), sbyte.MinValue);
        yield return new TestCaseData(new ClickHouseDecimal(127m), sbyte.MaxValue);
        yield return new TestCaseData(new ClickHouseDecimal(-128.9m), sbyte.MinValue);
        yield return new TestCaseData(new ClickHouseDecimal(0m), byte.MinValue);
        yield return new TestCaseData(new ClickHouseDecimal(255m), byte.MaxValue);
        yield return new TestCaseData(new ClickHouseDecimal(255.9m), byte.MaxValue);
        yield return new TestCaseData(new ClickHouseDecimal(-0.5m), byte.MinValue);
        yield return new TestCaseData(new ClickHouseDecimal(5 * BigInteger.Pow(10, 70), 70), (byte)5);
        yield return new TestCaseData(new ClickHouseDecimal(-32768m), short.MinValue);
        yield return new TestCaseData(new ClickHouseDecimal(32767m), short.MaxValue);
        yield return new TestCaseData(new ClickHouseDecimal(65535m), ushort.MaxValue);
        yield return new TestCaseData(new ClickHouseDecimal(65535m), char.MaxValue);
        yield return new TestCaseData(new ClickHouseDecimal(-2147483648m), int.MinValue);
        yield return new TestCaseData(new ClickHouseDecimal(2147483647m), int.MaxValue);
        yield return new TestCaseData(new ClickHouseDecimal(100000.00m), 100000);
    }

    [Test]
    [TestCaseSource(nameof(NarrowIntegralInRangeCases))]
    public void ChangeType_NarrowIntegralTypeInRange_ReturnsValue(ClickHouseDecimal value, object expected)
    {
        var actual = Convert.ChangeType(value, expected.GetType(), CultureInfo.InvariantCulture);
        Assert.That(actual, Is.EqualTo(expected).And.TypeOf(expected.GetType()));
    }

    public static IEnumerable<TestCaseData> NarrowIntegralOutOfRangeCases()
    {
        yield return new TestCaseData(new ClickHouseDecimal(-129m), typeof(sbyte));
        yield return new TestCaseData(new ClickHouseDecimal(128m), typeof(sbyte));
        yield return new TestCaseData(new ClickHouseDecimal(-1m), typeof(byte));
        yield return new TestCaseData(new ClickHouseDecimal(256m), typeof(byte));
        yield return new TestCaseData(new ClickHouseDecimal(256.5m), typeof(byte));
        yield return new TestCaseData(new ClickHouseDecimal(-32769m), typeof(short));
        yield return new TestCaseData(new ClickHouseDecimal(32768m), typeof(short));
        yield return new TestCaseData(new ClickHouseDecimal(-1m), typeof(ushort));
        yield return new TestCaseData(new ClickHouseDecimal(-1.5m), typeof(ushort));
        yield return new TestCaseData(new ClickHouseDecimal(65536m), typeof(ushort));
        yield return new TestCaseData(new ClickHouseDecimal(-1m), typeof(char));
        yield return new TestCaseData(new ClickHouseDecimal(65536m), typeof(char));
        yield return new TestCaseData(new ClickHouseDecimal(-2147483649m), typeof(int));
        yield return new TestCaseData(new ClickHouseDecimal(2147483648m), typeof(int));
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.Pow(10, 70), 0), typeof(int));
    }

    [Test]
    [TestCaseSource(nameof(NarrowIntegralOutOfRangeCases))]
    public void ChangeType_NarrowIntegralTypeOutOfRange_ThrowsOverflowException(ClickHouseDecimal value, Type type)
    {
        Assert.Throws<OverflowException>(() => Convert.ChangeType(value, type, CultureInfo.InvariantCulture));
    }

    [Test]
    [TestCase(typeof(decimal?))]
    [TestCase(typeof(int?))]
    [TestCase(typeof(Guid))]
    [TestCase(typeof(DateTimeOffset))]
    [TestCase(typeof(TimeSpan))]
    [TestCase(typeof(DayOfWeek))]
    [TestCase(typeof(IntPtr))]
    public void ChangeType_TypeWithoutConversion_ThrowsInvalidCastException(Type type)
    {
        var @decimal = new ClickHouseDecimal(5m);
        Assert.Throws<InvalidCastException>(() => Convert.ChangeType(@decimal, type, CultureInfo.InvariantCulture));
    }

    [Test]
    public void ToType_NullType_ThrowsArgumentNullException()
    {
        var @decimal = new ClickHouseDecimal(5m);
        Assert.Throws<ArgumentNullException>(() => @decimal.ToType(null, CultureInfo.InvariantCulture));
    }

    [Test]
    [TestCase(typeof(ClickHouseDecimal))]
    [TestCase(typeof(object))]
    public void ToType_OwnTypeOrObject_ReturnsSameValue(Type type)
    {
        var @decimal = new ClickHouseDecimal(123.45m);
        Assert.That(@decimal.ToType(type, CultureInfo.InvariantCulture), Is.EqualTo(@decimal));
    }

    [Test]
    [TestCase(typeof(bool))]
    [TestCase(typeof(char))]
    [TestCase(typeof(byte))]
    [TestCase(typeof(sbyte))]
    [TestCase(typeof(short))]
    [TestCase(typeof(ushort))]
    [TestCase(typeof(int))]
    [TestCase(typeof(uint))]
    [TestCase(typeof(long))]
    [TestCase(typeof(ulong))]
    [TestCase(typeof(float))]
    [TestCase(typeof(double))]
    [TestCase(typeof(decimal))]
    [TestCase(typeof(string))]
    public void ToType_ConvertibleType_MatchesChangeType(Type type)
    {
        var @decimal = new ClickHouseDecimal(5.00m);
        var expected = Convert.ChangeType(@decimal, type, CultureInfo.InvariantCulture);
        var actual = @decimal.ToType(type, CultureInfo.InvariantCulture);
        Assert.That(actual, Is.EqualTo(expected).And.TypeOf(type));
    }

    [Test]
    [RequiredFeature(Feature.WideTypes)]
    public async Task ValuesFromClickHouseShouldMatch([ValueSource(typeof(ClickHouseDecimalTests), nameof(DecimalsWithExtremeValues))] decimal value)
    {
        var scale = GetScale(value);

        using var connection = TestUtilities.GetTestClickHouseConnection();
        var result = (ClickHouseDecimal)await connection.ExecuteScalarAsync($"SELECT toDecimal256('{value.ToString(CultureInfo.InvariantCulture)}', {scale})");
        Assert.That((decimal)result, Is.EqualTo(value));
    }

    private static int GetScale(decimal value)
    {
        var parts = decimal.GetBits(value);
        return (parts[3] >> 16) & 0x7F;
    }
}
