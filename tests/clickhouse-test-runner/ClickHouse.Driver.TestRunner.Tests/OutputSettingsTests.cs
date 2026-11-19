using NUnit.Framework;

namespace ClickHouse.Driver.TestRunner.Tests;

// The settings that change the server's text output, and the HTTP session time zone. The expected text is what
// ClickHouse 26.10 writes for FORMAT TSV with the same settings.
public class OutputSettingsTests
{
    private static readonly TimeZoneInfo Utc = TimeZoneInfo.Utc;

    [TestCase("SELECT 1 SETTINGS a = 1", "a = 1")]
    [TestCase("SELECT 1 SETTINGS a = 1, b = 'x' FORMAT TSV;", "a = 1, b = 'x'")]
    [TestCase("SELECT 1 FORMAT TSV SETTINGS a = 1;", "a = 1")]
    [TestCase("SELECT * FROM (SELECT 1 SETTINGS a = 1)", null)]
    [TestCase("SELECT 'SETTINGS a = 1' -- SETTINGS b = 1", null)]
    [TestCase("SELECT t.settings FROM t", null)]
    [TestCase("SELECT 1", null)]
    public void TrailingSettingsClause_Query_FindsTheClauseOfTheQuery(string sql, string? expected) =>
        Assert.That(Statements.TrailingSettingsClause(sql), Is.EqualTo(expected));

    [Test]
    public void From_Values_ReadsEachSetting()
    {
        var s = OutputSettings.From(new Dictionary<string, string>
        {
            ["date_time_output_format"] = "unix_timestamp",
            ["format_tsv_null_representation"] = "a\\tb",
            ["output_format_decimal_trailing_zeros"] = "1",
            ["date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands"] = "true",
        });
        Assert.That(s, Is.EqualTo(new OutputSettings(DateTimeOutputFormat.UnixTimestamp, "a\tb", true, true)));
        Assert.That(OutputSettings.From(new Dictionary<string, string> { ["date_time_output_format"] = "ISO" }).DateTimeFormat, Is.EqualTo(DateTimeOutputFormat.Iso));
        Assert.That(OutputSettings.From(new Dictionary<string, string>()), Is.EqualTo(OutputSettings.Default));
        Assert.Throws<NotSupportedException>(() => OutputSettings.From(new Dictionary<string, string> { ["date_time_output_format"] = "x" }));
    }

    private static readonly object[] DateTimeCases =
    [
        // TCP raw counts.
        new object[] { "DateTime('Europe/Moscow')", 1602709200u, "iso", "2020-10-14T21:00:00Z" },
        new object[] { "DateTime('Europe/Moscow')", 1602709200u, "unix_timestamp", "1602709200" },
        new object[] { "DateTime64(2, 'Europe/Moscow')", 160270920012L, "iso", "2020-10-14T21:00:00.12Z" },
        new object[] { "DateTime64(2, 'Europe/Moscow')", 160270920012L, "unix_timestamp", "1602709200.12" },
        new object[] { "DateTime64(1, 'UTC')", -3156191995L, "unix_timestamp", "-315619199.5" },
        new object[] { "DateTime64(1, 'UTC')", -3156191995L, "iso", "1960-01-01T00:00:00.5Z" },
        // HTTP calendar values.
        new object[] { "DateTime('Europe/Moscow')", new DateTime(2020, 10, 15, 0, 0, 0, DateTimeKind.Unspecified), "iso", "2020-10-14T21:00:00Z" },
        new object[] { "DateTime64(2, 'Europe/Moscow')", new DateTime(2020, 10, 15, 0, 0, 0, DateTimeKind.Unspecified).AddMilliseconds(120), "unix_timestamp", "1602709200.12" },
        new object[] { "DateTime('UTC')", new DateTime(2020, 10, 15, 0, 0, 0, DateTimeKind.Utc), "iso", "2020-10-15T00:00:00Z" },
        new object[] { "DateTime64(1)", new DateTime(1960, 1, 1, 0, 0, 0, DateTimeKind.Unspecified).AddMilliseconds(500), "unix_timestamp", "-315619199.5" },
    ];

    [TestCaseSource(nameof(DateTimeCases))]
    public void Render_DateTimeOutputFormat_WritesTheServerText(string type, object value, string format, string expected)
    {
        var renderer = value is DateTime ? new ValueRenderer() : new ValueRenderer(() => Utc);
        renderer.Output = OutputSettings.From(new Dictionary<string, string> { ["date_time_output_format"] = format });
        Assert.That(Render(renderer, type, value, false), Is.EqualTo(expected));
        Assert.That(Render(renderer, "Date", new DateOnly(2020, 1, 1), false), Is.EqualTo("2020-01-01"));
    }

    [TestCase(1704067200100000L, 6, "2024-01-01 00:00:00.100")]
    [TestCase(1704067200123400L, 6, "2024-01-01 00:00:00.123400")]
    [TestCase(1704067200000000L, 6, "2024-01-01 00:00:00")]
    [TestCase(17040672001L, 1, "2024-01-01 00:00:00.100")]
    [TestCase(170406720012340L, 5, "2024-01-01 00:00:00.123400")]
    [TestCase(1704067200123456700L, 9, "2024-01-01 00:00:00.123456700")]
    public void Render_CutDateTime64TrailingZeros_AlignsToGroupsOfThree(long count, int precision, string expected)
    {
        var renderer = new ValueRenderer(() => Utc) { Output = OutputSettings.Default with { CutDateTime64TrailingZeros = true } };
        Assert.That(Render(renderer, $"DateTime64({precision}, 'UTC')", count, false), Is.EqualTo(expected));
        Assert.That(Render(renderer, "DateTime('UTC')", 1704067200u, false), Is.EqualTo("2024-01-01 00:00:00"));
    }

    [Test]
    public void Render_NullRepresentation_IsTheTopLevelNullOnly()
    {
        var renderer = new ValueRenderer { Output = OutputSettings.Default with { NullText = "XYZ" } };
        Assert.That(Render(renderer, "Nullable(String)", null, false), Is.EqualTo("XYZ"));
        Assert.That(Render(renderer, "Array(Nullable(String))", new string?[] { null }, false), Is.EqualTo("[NULL]"));
        Assert.That(Render(new ValueRenderer(), "Nullable(String)", null, false), Is.EqualTo("\\N"));
    }

    [TestCase("Decimal(9, 4)", "1.5", "1.5000")]
    [TestCase("Decimal32(2)", "0", "0.00")]
    [TestCase("Decimal(9, 2)", "-1.1", "-1.10")]
    [TestCase("Decimal(18, 3)", "100", "100.000")]
    [TestCase("Decimal(9, 0)", "7", "7")]
    public void Render_DecimalTrailingZeros_WritesTheScale(string type, string value, string expected)
    {
        var renderer = new ValueRenderer { Output = OutputSettings.Default with { DecimalTrailingZeros = true } };
        var d = decimal.Parse(value, System.Globalization.CultureInfo.InvariantCulture);
        Assert.That(Render(renderer, type, d, false), Is.EqualTo(expected));
        Assert.That(Render(new ValueRenderer(), type, d, false), Is.EqualTo(value));
    }

    [Test]
    public void Render_HttpDateTimeWithoutZone_TakesTheServerTimezone()
    {
        // The HTTP client gives the UTC wall clock of a DateTime without a zone in its type.
        var renderer = new ValueRenderer { HttpServerTimezone = "Asia/Tokyo" };
        var utcWallClock = new DateTime(2019, 12, 31, 15, 0, 0, DateTimeKind.Unspecified);
        Assert.That(Render(renderer, "DateTime", utcWallClock, false), Is.EqualTo("2020-01-01 00:00:00"));
        Assert.That(Render(renderer, "DateTime64(1)", utcWallClock.AddMilliseconds(500), false), Is.EqualTo("2020-01-01 00:00:00.5"));
        // America/New_York before 1883: local mean time, -4:56:02.
        renderer.HttpServerTimezone = "America/New_York";
        Assert.That(Render(renderer, "DateTime64(0)", new DateTime(1789, 7, 14, 4, 56, 2, DateTimeKind.Unspecified), false), Is.EqualTo("1789-07-14 00:00:00"));
        // A zone in the type: the client already gives its wall clock.
        Assert.That(Render(renderer, "DateTime('Asia/Tokyo')", new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Unspecified), false), Is.EqualTo("2020-01-01 00:00:00"));
        renderer.HttpServerTimezone = "UTC";
        Assert.That(Render(renderer, "DateTime", utcWallClock, false), Is.EqualTo("2019-12-31 15:00:00"));
    }

    private static string Render(ValueRenderer renderer, string type, object? value, bool nested)
    {
        var b = new Utf8Buffer();
        renderer.Render(b, ChType.Parse(type), value, nested);
        return b.ToString();
    }
}
