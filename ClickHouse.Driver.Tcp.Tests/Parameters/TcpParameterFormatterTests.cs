using System;
using System.Collections.Generic;
using System.Net;
using ClickHouse.Driver.Tcp.Parameters;

namespace ClickHouse.Driver.Tcp.Tests.Parameters;

// The SQL-text half of the formatter: the representation the server parses against the declared type. This is
// also the text the HTTP transport sends verbatim, so these expectations are the parity contract between the
// two formatters. The native protocol's extra escape and quote is covered separately below.
[TestFixture]
public class TcpParameterFormatterTests
{
    private static readonly DateTime Unspecified = new(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);

    private static IEnumerable<TestCaseData> SqlTextCases()
    {
        yield return new TestCaseData(42, "Int32").Returns("42").SetName("Int32");
        yield return new TestCaseData((byte)7, "UInt8").Returns("7").SetName("UInt8");
        yield return new TestCaseData(long.MinValue, "Int64").Returns("-9223372036854775808").SetName("Int64 min");
        yield return new TestCaseData(ulong.MaxValue, "UInt64").Returns("18446744073709551615").SetName("UInt64 max");
        yield return new TestCaseData(1.5d, "Float64").Returns("1.5").SetName("Float64");
        yield return new TestCaseData(true, "Bool").Returns("true").SetName("Bool");
        yield return new TestCaseData(1.2345m, "Decimal64(4)").Returns("1.2345").SetName("Decimal from decimal");
        yield return new TestCaseData("1.2345", "Decimal64(4)").Returns("1.2345").SetName("Decimal from string");

        // A top-level string is escaped but not quoted; the quoting is the composite's job.
        yield return new TestCaseData("plain", "String").Returns("plain").SetName("String");
        yield return new TestCaseData("O'Brien", "String").Returns(@"O\'Brien").SetName("String with a quote");
        yield return new TestCaseData(@"a\b", "String").Returns(@"a\\b").SetName("String with a backslash");
        yield return new TestCaseData("a\nb", "String").Returns(@"a\nb").SetName("String with a newline");
        yield return new TestCaseData("a\tb", "String").Returns(@"a\tb").SetName("String with a tab");
        yield return new TestCaseData(string.Empty, "String").Returns(string.Empty).SetName("Empty string");
        yield return new TestCaseData("x", "LowCardinality(String)").Returns("x").SetName("LowCardinality unwraps");
        yield return new TestCaseData("a", "Enum8('a' = 1, 'b' = 2)").Returns("a").SetName("Enum label");

        yield return new TestCaseData(Guid.Parse("61f0c404-5cb3-11e7-907b-a6006ad3dba0"), "UUID")
            .Returns("61f0c404-5cb3-11e7-907b-a6006ad3dba0").SetName("UUID");
        yield return new TestCaseData(IPAddress.Parse("192.168.1.1"), "IPv4").Returns("192.168.1.1").SetName("IPv4");
        yield return new TestCaseData(IPAddress.Parse("::1"), "IPv6").Returns("::1").SetName("IPv6");

        // Interval<Unit> carries its underlying Int64 count, the same form the codecs surface it as.
        yield return new TestCaseData(5L, "IntervalSecond").Returns("5").SetName("IntervalSecond");
        yield return new TestCaseData(-3, "IntervalDay").Returns("-3").SetName("IntervalDay negative");
        yield return new TestCaseData(new[] { 1L, 2L }, "Array(IntervalMonth)").Returns("[1,2]").SetName("Array of IntervalMonth");

        yield return new TestCaseData(new DateOnly(2024, 1, 2), "Date").Returns("2024-01-02").SetName("Date");
        yield return new TestCaseData(Unspecified, "DateTime").Returns("2024-01-02T03:04:05").SetName("DateTime unspecified");
        yield return new TestCaseData(Unspecified, "DateTime64(3)").Returns("2024-01-02 03:04:05.0000000").SetName("DateTime64 unspecified");

        // A kinded value names an instant, so it is moved into the type's timezone to keep that instant.
        yield return new TestCaseData(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), "DateTime('Europe/Amsterdam')")
            .Returns("2024-01-02T04:04:05").SetName("DateTime shifted into the type timezone");
        yield return new TestCaseData(new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero), "DateTime('UTC')")
            .Returns("2024-01-02T03:04:05").SetName("DateTimeOffset in a UTC type");

        // Composites quote their string-like elements; that is the only place quoting appears.
        yield return new TestCaseData(new[] { 1, 2, 3 }, "Array(Int32)").Returns("[1,2,3]").SetName("Array of Int32");
        yield return new TestCaseData(new[] { "a", "b" }, "Array(String)").Returns("['a','b']").SetName("Array of String");
        yield return new TestCaseData(new[] { "O'B" }, "Array(String)").Returns(@"['O\'B']").SetName("Array element with a quote");
        yield return new TestCaseData(Array.Empty<int>(), "Array(Int32)").Returns("[]").SetName("Empty array");
        yield return new TestCaseData(new int?[] { 1, null, 3 }, "Array(Nullable(Int32))").Returns("[1,null,3]").SetName("Array with a null element");
        yield return new TestCaseData(new[] { new[] { 1, 2 }, new[] { 3 } }, "Array(Array(Int32))").Returns("[[1,2],[3]]").SetName("Jagged array");
        yield return new TestCaseData(new int[,] { { 1, 2 }, { 3, 4 } }, "Array(Array(Int32))").Returns("[[1,2],[3,4]]").SetName("Rank-2 array");
        yield return new TestCaseData(("a", 1), "Tuple(String, Int32)").Returns("('a',1)").SetName("Tuple");
        yield return new TestCaseData(("a", 1), "Tuple(x String, y Int32)").Returns("('a',1)").SetName("Named tuple");
    }

    [TestCaseSource(nameof(SqlTextCases))]
    public string FormatSqlText_ValueOfDeclaredType_ProducesTheServersTextForm(object value, string typeName)
        => TcpParameterFormatter.FormatSqlText(value, typeName, "p");

    [Test]
    public void FormatSqlText_MapValue_SpacesTheKeyFromTheValue()
    {
        var value = new Dictionary<string, int> { ["a"] = 1 };

        Assert.That(TcpParameterFormatter.FormatSqlText(value, "Map(String, Int32)", "p"), Is.EqualTo("{'a' : 1}"));
    }

    [TestCase(null)]
    public void FormatSqlText_NullValue_ProducesTheNullMarker(object value)
        => Assert.That(TcpParameterFormatter.FormatSqlText(value, "Nullable(String)", "p"), Is.EqualTo(@"\N"));

    [Test]
    public void FormatSqlText_DBNullValue_ProducesTheNullMarker()
        => Assert.That(TcpParameterFormatter.FormatSqlText(DBNull.Value, "Nullable(String)", "p"), Is.EqualTo(@"\N"));

    [Test]
    public void FormatSqlText_NullInsideAComposite_ProducesTheLiteralNull()
    {
        // Nested in a composite the marker would be read as text, so the literal null is used instead.
        Assert.That(TcpParameterFormatter.FormatSqlText(new string[] { null }, "Array(Nullable(String))", "p"), Is.EqualTo("[null]"));
    }

    [Test]
    public void FormatSqlText_IdentifierType_LeavesTheValueUnescaped()
    {
        // The server substitutes an Identifier as a bare SQL name and quotes it itself, so escaping here would
        // corrupt it. This is the one type whose text is not escaped.
        Assert.That(TcpParameterFormatter.FormatSqlText(@"we'ird\name", "Identifier", "p"), Is.EqualTo(@"we'ird\name"));
    }

    [Test]
    public void FormatSqlText_WideDecimalString_KeepsEveryDigit()
    {
        // Wider than decimal can hold, so the text path must not go through decimal.
        const string wide = "12345678901234567890123456789012345.6789";

        Assert.That(TcpParameterFormatter.FormatSqlText(wide, "Decimal256(4)", "p"), Is.EqualTo(wide));
    }

    [Test]
    public void FormatSqlText_ClickHouseDecimal_UsesItsOwnScale()
    {
        var value = new ClickHouseTcpDecimal(12345, 4);

        Assert.That(TcpParameterFormatter.FormatSqlText(value, "Decimal64(4)", "p"), Is.EqualTo("1.2345"));
    }

    [Test]
    public void FormatSqlText_VariantValue_PicksTheAlternativeMatchingTheValue()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TcpParameterFormatter.FormatSqlText("x", "Variant(Int64, String)", "p"), Is.EqualTo("x"));
            Assert.That(TcpParameterFormatter.FormatSqlText(7L, "Variant(Int64, String)", "p"), Is.EqualTo("7"));
        });
    }

    [Test]
    public void FormatSqlText_ValueOfAnIncompatibleType_ThrowsNamingTheParameter()
    {
        var exception = Assert.Throws<ArgumentException>(
            () => TcpParameterFormatter.FormatSqlText(new object(), "Array(Int32)", "ids"));

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Does.Contain("ids"));
            Assert.That(exception.Message, Does.Contain("Array(Int32)"));
        });
    }

    [Test]
    public void FormatSqlText_MalformedTypeName_ThrowsFormatException()
        => Assert.Throws<FormatException>(() => TcpParameterFormatter.FormatSqlText(1, "Array(", "p"));

    // The native protocol's own layer. A parameter value crosses two server-side unescape stages, so the SQL
    // text above is escaped a second time and quoted as a whole. Every expectation here was confirmed against a
    // live server by reading the value back with hex().
    private static IEnumerable<TestCaseData> WireValueCases()
    {
        yield return new TestCaseData(42, "Int32").Returns("'42'").SetName("Int32 is quoted too");
        yield return new TestCaseData("plain", "String").Returns("'plain'").SetName("Plain string");
        yield return new TestCaseData("O'Brien", "String").Returns(@"'O\\\'Brien'").SetName("Quote is escaped twice");
        yield return new TestCaseData(@"a\b", "String").Returns(@"'a\\\\b'").SetName("Backslash is escaped twice");
        yield return new TestCaseData("a\nb", "String").Returns(@"'a\\nb'").SetName("Newline survives as an escape");
        yield return new TestCaseData("a\tb", "String").Returns(@"'a\\tb'").SetName("Tab survives as an escape");
        yield return new TestCaseData("' OR 1=1 --", "String").Returns(@"'\\\' OR 1=1 --'").SetName("Injection attempt stays data");
        yield return new TestCaseData(new[] { "a" }, "Array(String)").Returns(@"'[\'a\']'").SetName("Array element quotes are escaped");
        yield return new TestCaseData(null, "Nullable(String)").Returns(@"'\\N'").SetName("Null marker is escaped");
    }

    [TestCaseSource(nameof(WireValueCases))]
    public string Format_AnyValue_EscapesAndQuotesForTheFieldStage(object value, string typeName)
        => TcpParameterFormatter.Format(value, typeName, "p");
}
