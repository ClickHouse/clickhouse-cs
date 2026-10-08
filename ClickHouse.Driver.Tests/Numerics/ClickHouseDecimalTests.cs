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

    // Mantissas at the edges of the digit count: around powers of ten, at the precision limits of Decimal32/64/128/256,
    // and at the mantissa limits of double (2^53) and System.Decimal (2^96). At most 76 digits, the Decimal256 limit.
    private static readonly BigInteger[] CornerCaseMantissas =
        new BigInteger[] { 1, 2, 5, 9, 10, 11, 99, 100, 101, 12345, BigInteger.Parse("100000000000000012345", CultureInfo.InvariantCulture) }
            .Concat(new[] { 9, 15, 16, 17, 18, 19, 20, 28, 29, 38, 39, 50, 51, 75, 76 }
                .SelectMany(k => new[] { BigInteger.Pow(10, k) - 1, BigInteger.Pow(10, k), BigInteger.Pow(10, k) + 1 }))
            .Concat([BigInteger.Pow(2, 53) - 1, BigInteger.Pow(2, 53) + 1, BigInteger.Pow(2, 96) - 1, BigInteger.Pow(2, 96)])
            .Where(m => m.ToString(CultureInfo.InvariantCulture).Length <= 76)
            .Distinct()
            .ToArray();

    // Each corner-case mantissa with both signs, at scales 0, 1 and 76, and at the scales where the value has one integer
    // digit, no integer digit, or a leading zero after the decimal separator
    public static IEnumerable<TestCaseData> CornerCaseValues()
    {
        foreach (var scale in new[] { 0, 1, 76 })
            yield return new TestCaseData(ToLiteral(BigInteger.Zero, scale), scale);
        foreach (var mantissa in CornerCaseMantissas)
        {
            var digits = mantissa.ToString(CultureInfo.InvariantCulture).Length;
            foreach (var scale in new[] { 0, 1, digits - 1, digits, digits + 1, 76 }.Where(s => s is >= 0 and <= 76).Distinct())
            {
                yield return new TestCaseData(ToLiteral(mantissa, scale), scale);
                yield return new TestCaseData(ToLiteral(-mantissa, scale), scale);
            }
        }
    }

    // Both signs, values below one with leading zeros, powers of ten and their neighbours, the Decimal32/64/128 limits,
    // trailing zeros, scales up to 76 and values up to 76 digits. Many pairs have a divisor scale above the dividend
    // scale, a quotient wider than MaxDivisionPrecision, or both.
    public static readonly string[] DivisionOperands =
        new[]
        {
            "1", "-1", "2", "3", "-3", "7", "9", "10", "100", "1.000", "0.100", "0.1", "0.3", "-0.3", "0.5", "1.5",
            "-123.45", "0.7777", "3.14159265359", "0.9999999999", "123456789", "79228162514264337593543950336",
        }
        .Concat(new[] { 6, 28, 56, 60, 76 }.Select(scale => ToLiteral(BigInteger.One, scale)))
        .Append(ToLiteral(BigInteger.MinusOne, 60))
        .Concat(new[] { 18, 38, 45, 48, 49, 50, 51, 60, 75 }.Select(exponent => ToLiteral(BigInteger.Pow(10, exponent), 0)))
        .Append(ToLiteral(-BigInteger.Pow(10, 60), 0))
        .Concat(new[] { 0, 75, 76 }.Select(scale => ToLiteral(BigInteger.Pow(10, 76) - 1, scale)))
        .Concat(new[]
            {
                BigInteger.Pow(10, 9) - 1, BigInteger.Pow(10, 16) + 1, BigInteger.Pow(10, 18) - 1,
                BigInteger.Parse("100000000000000012345", CultureInfo.InvariantCulture), BigInteger.Pow(10, 38) - 1, BigInteger.Pow(10, 38) + 1,
            }
            .SelectMany(mantissa => new[] { 0, mantissa.ToString(CultureInfo.InvariantCulture).Length }.Select(scale => ToLiteral(mantissa, scale))))
        .Distinct()
        .ToArray();

    public static IEnumerable<TestCaseData> DivisionOperandPairs() =>
        from dividend in DivisionOperands.Prepend("0.000").Prepend("0")
        from divisor in DivisionOperands
        select new TestCaseData(dividend, divisor);

    public static IEnumerable<TestCaseData> WideQuotientCases()
    {
        var oneE60 = "1" + new string('0', 60);
        var oneEMinus60 = ToLiteral(BigInteger.One, 60);
        yield return new TestCaseData("1", oneEMinus60).Returns(oneE60);
        yield return new TestCaseData("0", oneEMinus60).Returns("0");
        yield return new TestCaseData(oneE60, "0.3").Returns(new string('3', 61));
        yield return new TestCaseData("-" + oneE60, "0.3").Returns("-" + new string('3', 61));
        yield return new TestCaseData(oneE60, "-0.3").Returns("-" + new string('3', 61));
        yield return new TestCaseData("1" + new string('0', 45), "0.000001").Returns("1" + new string('0', 51));
        yield return new TestCaseData("5", ToLiteral(BigInteger.One, 56)).Returns("5" + new string('0', 56));
        yield return new TestCaseData("1.5", oneEMinus60).Returns("15" + new string('0', 59));
        yield return new TestCaseData("-123.45", oneEMinus60).Returns("-12345" + new string('0', 58));
        yield return new TestCaseData("1" + new string('0', 75), "0.1").Returns("1" + new string('0', 76));
        yield return new TestCaseData("1" + new string('0', 50), "0.3").Returns(new string('3', 51));
        yield return new TestCaseData("1" + new string('0', 49), "0.3").Returns(new string('3', 50));
        yield return new TestCaseData("1" + new string('0', 48), "0.3").Returns(new string('3', 49) + ".3");
        yield return new TestCaseData("1", "0.3").Returns("3." + new string('3', 49));
        yield return new TestCaseData(oneE60, "3").Returns(new string('3', 60));
    }

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
    [TestCase("1000", 3, 1, ExpectedResult = "1")]
    [TestCase("-100", 2, 2, ExpectedResult = "-1.0")]
    [TestCase("100000000000000012345", 20, 18, ExpectedResult = "1.00000000000000012")]
    public string Truncate_MantissaAtOrJustAbovePowerOfTen_KeepsGivenNumberOfDigits(string mantissa, int scale, int precision)
    {
        var value = new ClickHouseDecimal(BigInteger.Parse(mantissa, CultureInfo.InvariantCulture), scale);
        return value.Truncate(precision).ToString(CultureInfo.InvariantCulture);
    }

    [Test]
    [TestCase("1", 1, ExpectedResult = "0")]
    [TestCase("100", 3, ExpectedResult = "0")]
    [TestCase("100", 2, ExpectedResult = "1")]
    [TestCase("100000000000000012345", 20, ExpectedResult = "1")]
    public string Floor_MantissaAtOrJustAbovePowerOfTen_ReturnsIntegerPart(string mantissa, int scale)
    {
        var value = new ClickHouseDecimal(BigInteger.Parse(mantissa, CultureInfo.InvariantCulture), scale);
        return value.Floor().ToString(CultureInfo.InvariantCulture);
    }

    [Test]
    [RequiredFeature(Feature.WideTypes)]
    [TestCaseSource(nameof(CornerCaseValues))]
    public async Task TruncateFloorAndConvertToBigInteger_CornerCaseValueFromClickHouse_MatchesServer(string literal, int scale)
    {
        var digits = BigInteger.Abs(BigInteger.Parse(literal.Replace(".", string.Empty), CultureInfo.InvariantCulture))
            .ToString(CultureInfo.InvariantCulture).Length;
        var integerDigits = digits - scale;
        var precisions = new[] { 1, integerDigits, integerDigits + 1, digits - 1, digits, digits + 1 }
            .Where(p => p > 0).Distinct().ToArray();
        // Truncate(precision) keeps `precision` significant digits, but never removes integer digits
        var keptScales = precisions.Select(p => Math.Clamp(p - integerDigits, 0, scale)).ToArray();

        using var connection = TestUtilities.GetTestClickHouseConnection();
        var truncations = string.Concat(keptScales.Select(s => $", trunc(x, {s})"));
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT x, trunc(x), toInt256(x), floor(x){truncations} FROM (SELECT toDecimal256('{literal}', {scale}) AS x)");
        Assert.That(reader.Read(), Is.True);

        var value = (ClickHouseDecimal)reader.GetValue(0);
        Assert.Multiple(() =>
        {
            Assert.That(value.Truncate(), Is.EqualTo((ClickHouseDecimal)reader.GetValue(1)), "Truncate()");
            Assert.That(
                Convert.ChangeType(value, typeof(BigInteger), CultureInfo.InvariantCulture),
                Is.EqualTo((BigInteger)reader.GetValue(2)),
                "BigInteger conversion");
            // Floor() of a negative non-integer rounds toward zero; that is a separate bug
            if (value.Sign >= 0 || (value.Mantissa % BigInteger.Pow(10, scale)).IsZero)
                Assert.That(value.Floor(), Is.EqualTo((ClickHouseDecimal)reader.GetValue(3)), "Floor()");
            for (var i = 0; i < precisions.Length; i++)
            {
                var truncated = value.Truncate(precisions[i]);
                Assert.That(truncated, Is.EqualTo((ClickHouseDecimal)reader.GetValue(4 + i)), $"Truncate({precisions[i]})");
                Assert.That(truncated.Scale, Is.EqualTo(keptScales[i]), $"Truncate({precisions[i]}).Scale");
            }
        });
    }

    [Test]
    public void NumberOfDigits_ValuesAroundPowersOfTen_ReturnsDecimalDigitCount()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClickHouseDecimal.NumberOfDigits(BigInteger.Zero), Is.EqualTo(0));
            foreach (var exponent in Enumerable.Range(0, 401).Append(1000).Append(4000))
            {
                var power = BigInteger.Pow(10, exponent);
                var values = new[]
                {
                    power - 1, power, power + 1, power + 12345, (2 * power) - 1, 5 * power, 9 * power,
                    power - BigInteger.Pow(10, exponent / 2), power + BigInteger.Pow(10, Math.Max(exponent - 16, 0)),
                    power - BigInteger.Pow(10, Math.Max(exponent - 17, 0)), BigInteger.Pow(2, exponent), BigInteger.Pow(2, exponent) - 1,
                };
                foreach (var value in values)
                {
                    if (value.IsZero)
                        continue;
                    var expected = value.ToString(CultureInfo.InvariantCulture).Length;
                    Assert.That(ClickHouseDecimal.NumberOfDigits(value), Is.EqualTo(expected), $"{value}");
                    Assert.That(ClickHouseDecimal.NumberOfDigits(-value), Is.EqualTo(expected), $"-{value}");
                }
            }
        });
    }

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

    [Test]
    [TestCase("1", "3", ExpectedResult = "0.33333333333333333333333333333333333333333333333333")]
    [TestCase("10", "3", ExpectedResult = "3.3333333333333333333333333333333333333333333333333")]
    [TestCase("100", "7", ExpectedResult = "14.285714285714285714285714285714285714285714285714")]
    [TestCase("123456789012345678901234567890123456789012345678901", "10", ExpectedResult = "12345678901234567890123456789012345678901234567890.1")]
    public string Divide_PowerOfTenDividendOrDivisor_SizesResultFromExactDigitCount(string dividend, string divisor)
    {
        var result = ClickHouseDecimal.Parse(dividend, CultureInfo.InvariantCulture) / ClickHouseDecimal.Parse(divisor, CultureInfo.InvariantCulture);
        return result.ToString(CultureInfo.InvariantCulture);
    }

    [Test]
    [TestCaseSource(nameof(WideQuotientCases))]
    public string Divide_DivisorScaleAboveDividendScale_KeepsIntegerPartOfQuotient(string dividend, string divisor)
    {
        var result = ClickHouseDecimal.Parse(dividend, CultureInfo.InvariantCulture) / ClickHouseDecimal.Parse(divisor, CultureInfo.InvariantCulture);
        return result.ToString(CultureInfo.InvariantCulture);
    }

    [Test]
    [TestCaseSource(nameof(DivisionOperandPairs))]
    public void Divide_CornerCaseOperands_ReturnsQuotientTruncatedTowardZero(string dividend, string divisor)
    {
        var left = ParseInvariant(dividend);
        var right = ParseInvariant(divisor);
        AssertTruncatedQuotient(left, right, left / right, ClickHouseDecimal.MaxDivisionPrecision);
    }

    // The precision is passed as an argument, because MaxDivisionPrecision is process-wide and the tests run in parallel
    [Test]
    public void Divide_SmallPrecision_ReturnsQuotientTruncatedTowardZero([Values(-1, 0, 1, 5)] int precision)
    {
        Assert.Multiple(() =>
        {
            foreach (var dividend in DivisionOperands.Prepend("0").Select(ParseInvariant))
            {
                foreach (var divisor in DivisionOperands.Select(ParseInvariant))
                    AssertTruncatedQuotient(dividend, divisor, ClickHouseDecimal.Divide(dividend, divisor, precision), precision);
            }
        });
    }

    [Test]
    [TestCase(5, "123456789", "0.5", ExpectedResult = "246913578")]
    [TestCase(5, "1", "3", ExpectedResult = "0.33333")]
    [TestCase(1, "1", "0.3", ExpectedResult = "3")]
    [TestCase(0, "10", "0.5", ExpectedResult = "20")]
    [TestCase(0, "1", "0.3", ExpectedResult = "3")]
    [TestCase(0, "10", "3", ExpectedResult = "3")]
    [TestCase(0, "1", "3", ExpectedResult = "0")]
    public string Divide_SmallPrecision_ReturnsExpectedQuotient(int precision, string dividend, string divisor)
        => ClickHouseDecimal.Divide(ParseInvariant(dividend), ParseInvariant(divisor), precision).ToString(CultureInfo.InvariantCulture);

    [Test]
    [TestCase("1", "0")]
    [TestCase("1", "0.000")]
    [TestCase("0", "0")]
    [TestCase("-0.5", "0.0")]
    public void Divide_ZeroDivisor_ThrowsDivideByZeroException(string dividend, string divisor)
    {
        Assert.Throws<DivideByZeroException>(() => _ = ParseInvariant(dividend) / ParseInvariant(divisor));
    }

    [Test]
    [RequiredFeature(Feature.WideTypes)]
    [TestCase("toDecimal256('1', 0)", "toDecimal256('0.000000000000000000000000000000000000000000000000000000000001', 60)")]
    [TestCase("toDecimal256('1000000000000000000000000000000000000000000000000000000000000', 0)", "toDecimal32('0.3', 1)")]
    [TestCase("toDecimal256('-1000000000000000000000000000000000000000000000000000000000000', 0)", "toDecimal32('0.3', 1)")]
    [TestCase("toDecimal256('1000000000000000000000000000000000000000000000', 0)", "toDecimal64('0.000001', 6)")]
    [TestCase("toDecimal64('1.5', 1)", "toDecimal256('0.000000000000000000000000000000000000000000000000000000000001', 60)")]
    [TestCase("toDecimal256('1000000000000000000000000000000000000000000000000000000000000', 0)", "toDecimal32('3', 0)")]
    [TestCase("toDecimal128('-123.45', 2)", "toDecimal64('0.7', 1)")]
    [TestCase("toDecimal32('1', 0)", "toDecimal32('3', 0)")]
    public async Task Divide_ValuesFromClickHouse_MatchesServerDivideDecimal(string dividendExpression, string divisorExpression)
    {
        using var connection = TestUtilities.GetTestClickHouseConnection();
        ClickHouseDecimal dividend, divisor;
        using (var reader = await connection.ExecuteReaderAsync($"SELECT {dividendExpression}, {divisorExpression}"))
        {
            Assert.That(reader.Read(), Is.True);
            dividend = (ClickHouseDecimal)reader.GetValue(0);
            divisor = (ClickHouseDecimal)reader.GetValue(1);
        }

        var quotient = dividend / divisor;
        var expected = await connection.ExecuteScalarAsync(
            $"SELECT divideDecimal({dividendExpression}, {divisorExpression}, {quotient.Scale})");
        Assert.Multiple(() =>
        {
            Assert.That(quotient, Is.EqualTo((ClickHouseDecimal)expected));
            AssertTruncatedQuotient(dividend, divisor, quotient, ClickHouseDecimal.MaxDivisionPrecision);
        });
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

    private static string ToLiteral(BigInteger mantissa, int scale) =>
        new ClickHouseDecimal(mantissa, scale).ToString(CultureInfo.InvariantCulture);

    private static ClickHouseDecimal ParseInvariant(string value) => ClickHouseDecimal.Parse(value, CultureInfo.InvariantCulture);

    // Asserts that the quotient is the exact quotient truncated toward zero, with all its integer digits and at least
    // `precision` significant digits (more are allowed)
    private static void AssertTruncatedQuotient(ClickHouseDecimal dividend, ClickHouseDecimal divisor, ClickHouseDecimal quotient, int precision)
    {
        // exact quotient = numerator / denominator
        var numerator = dividend.Mantissa * BigInteger.Pow(10, divisor.Scale);
        var denominator = divisor.Mantissa * BigInteger.Pow(10, dividend.Scale);
        var scale = quotient.Scale;
        if (!numerator.IsZero)
        {
            // the scale at which the exact quotient has `precision` significant digits
            var leadingDigitExponent = LeadingDigitExponent(BigInteger.Abs(numerator), BigInteger.Abs(denominator));
            scale = Math.Max(scale, precision - 1 - leadingDigitExponent);
        }

        var expected = new ClickHouseDecimal(numerator * BigInteger.Pow(10, scale) / denominator, scale);
        Assert.That(
            quotient,
            Is.EqualTo(expected),
            $"{dividend.ToString(CultureInfo.InvariantCulture)} / {divisor.ToString(CultureInfo.InvariantCulture)} with precision {precision}");
    }

    // Returns e such that 10^e <= numerator / denominator < 10^(e + 1), for positive arguments
    private static int LeadingDigitExponent(BigInteger numerator, BigInteger denominator)
    {
        var exponent = numerator.ToString(CultureInfo.InvariantCulture).Length - denominator.ToString(CultureInfo.InvariantCulture).Length;
        var belowPowerOfTen = exponent >= 0
            ? numerator < denominator * BigInteger.Pow(10, exponent)
            : numerator * BigInteger.Pow(10, -exponent) < denominator;
        return belowPowerOfTen ? exponent - 1 : exponent;
    }
}
