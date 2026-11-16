using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Numerics;

public class ClickHouseOldDecimalSqlTests
{
    private readonly ClickHouseConnection connection;

    public ClickHouseOldDecimalSqlTests()
    {
        connection = TestUtilities.GetTestClickHouseConnection(customDecimals: false);
    }

    public static IEnumerable<string> DecimalTypes
    {
        get
        {
            yield return "Decimal32(3)";
            yield return "Decimal64(3)";
            yield return "Decimal128(3)";
            if (TestUtilities.SupportedFeatures.HasFlag(Feature.WideTypes))
            {
                yield return "Decimal256(3)";
            }
        }
    }

    public static IEnumerable<TestCaseData> DecimalTestCases
    {
        get
        {
            var values = Enumerable.Range(0, 10).Select(i => $"1{new string('0', i)}").Select(Convert.ToDecimal).ToList();

            return from typeName in DecimalTypes
                   from v in values
                   let type = (DecimalType)TypeConverter.ParseClickHouseType(typeName, TypeSettings.Default)
                   where v < type.MaxValue && v > type.MinValue
                   select new TestCaseData(v, $"SELECT CAST('{v}', '{type}')");
        }
    }

    [Test]
    [TestCaseSource(typeof(ClickHouseOldDecimalSqlTests), nameof(DecimalTestCases))]
    public async Task Select(decimal expected, string sql)
    {
        using var reader = await connection.ExecuteReaderAsync(sql);
        reader.AssertHasFieldCount(1);
        var result = reader.GetEnsureSingleRow().Single();
        ClassicAssert.IsInstanceOf<decimal>(result);
        Assert.That(result, Is.EqualTo(expected));
    }

    // Values System.Decimal holds exactly, stored with a scale above 28 or a mantissa wider than 96 bits.
    [TestCase("SELECT toDecimal128('0.5', 38)", "0.5")]
    [TestCase("SELECT toDecimal128('-79228162514264337593543950335', 9)", "-79228162514264337593543950335")]
    public async Task Select_WideDecimalRepresentableAsSystemDecimal_ReturnsExactValue(string sql, string expected)
    {
        using var reader = await connection.ExecuteReaderAsync(sql);
        var result = reader.GetEnsureSingleRow().Single();
        Assert.That(result, Is.TypeOf<decimal>().And.EqualTo(decimal.Parse(expected, CultureInfo.InvariantCulture)));
    }

    // More than 28 significant fractional digits: System.Decimal cannot hold the value without
    // rounding, so the read must fail rather than lose digits.
    [TestCase("SELECT toDecimal128('0.1234567890123456789012345678901', 31)")]
    [TestCase("SELECT toDecimal128('0.0000000000000000000000000000001', 31)")]
    public void Select_DecimalNotRepresentableAsSystemDecimal_ThrowsOverflowException(string sql)
    {
        Assert.ThrowsAsync<OverflowException>(async () =>
        {
            using var reader = await connection.ExecuteReaderAsync(sql);
            reader.GetEnsureSingleRow();
        });
    }

    [OneTimeTearDown]
    public void Dispose()
    {
        connection?.Dispose();
    }
}
