using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// A <see cref="decimal"/> keeps a scale of its own, so 7 and 7.0000 are equal but print and
/// round-trip differently. Reading a ClickHouse Decimal as <see cref="decimal"/> must keep the
/// column's scale, the same as reading it as <c>ClickHouseDecimal</c> does.
/// </summary>
[Category("Cloud")]
public class DecimalReadScaleTests
{
    private const string Zeros28 = "0000000000000000000000000000";

    // Args: (select expression, expected invariant string of the System.Decimal read back).
    private static TestCaseData[] ScaleCases() =>
    [
        new("toDecimal32(7, 4)", "7.0000"),
        new("toDecimal32(7, 0)", "7"),
        new("toDecimal32(0, 2)", "0.00"),
        new("toDecimal32('-7.5', 4)", "-7.5000"),
        new("toDecimal32('1.2345', 4)", "1.2345"),
        new("toDecimal64(1, 3)", "1.000"),
        new("toDecimal64(7, 10)", "7.0000000000"),
        new("toDecimal64('0.5', 18)", "0.500000000000000000"),
        new("toDecimal64(-7, 10)", "-7.0000000000"),
        new("toDecimal64('-999999999999999999', 0)", "-999999999999999999"),
        new("toDecimal128(7, 10)", "7.0000000000"),
        new("toDecimal128(-7, 10)", "-7.0000000000"),
        new("toDecimal128(7, 28)", "7." + Zeros28),
        new("toDecimal256(7, 4)", "7.0000"),
        new("toDecimal256('1.5', 28)", "1.5" + Zeros28[1..]),

        // System.Decimal holds at most 28 fractional digits and a 96-bit mantissa. A value that does
        // not fit at the column's scale drops trailing zeros until it does, instead of failing to convert.
        new("toDecimal128(7, 30)", "7." + Zeros28),
        new("toDecimal256(7, 40)", "7." + Zeros28),
        new("toDecimal128('1000000000', 20)", "1000000000." + Zeros28[..19]), // mantissa 10^29, scale 19

    ];

    [TestCaseSource(nameof(ScaleCases))]
    public async Task Read_DecimalWithoutCustomDecimals_KeepsColumnScale(string expression, string expected)
    {
        using var connection = TestUtilities.GetTestClickHouseConnection(customDecimals: false);
        using var reader = await connection.ExecuteReaderAsync($"SELECT {expression}");
        ClassicAssert.IsTrue(reader.Read());

        Assert.That(reader.GetValue(0), Is.TypeOf<decimal>());
        Assert.That(((decimal)reader.GetValue(0)).ToString(CultureInfo.InvariantCulture), Is.EqualTo(expected));
        Assert.That(reader.GetDecimal(0).ToString(CultureInfo.InvariantCulture), Is.EqualTo(expected));
        Assert.That(reader.GetFieldValue<decimal>(0).ToString(CultureInfo.InvariantCulture), Is.EqualTo(expected));
    }

    // The POCO fast path reads a decimal property as decimal whatever UseCustomDecimals is set to,
    // so it is affected with the default settings too.
    [TestCaseSource(nameof(ScaleCases))]
    public async Task QueryAsync_DecimalPropertyWithDefaultSettings_KeepsColumnScale(string expression, string expected)
    {
        using var client = TestUtilities.GetTestClickHouseClient();
        client.RegisterPocoType<DecimalHolder>();

        var rows = new List<DecimalHolder>();
        await foreach (var row in client.QueryAsync<DecimalHolder>($"SELECT {expression} AS Value"))
            rows.Add(row);

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Value.ToString(CultureInfo.InvariantCulture), Is.EqualTo(expected));
    }

    public class DecimalHolder
    {
        public decimal Value { get; set; }
    }
}
