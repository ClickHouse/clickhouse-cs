using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Parameters;

namespace ClickHouse.Driver.Tcp.Tests.Parameters;

// Covers the inner SQL representation shared with HTTP, and the wire value the native protocol wraps it in.
[TestFixture]
public class TcpParameterFormatterTests
{
    private static readonly DateTime Unspecified = new(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);

    // Only the forms a round-trip cannot show: text the server rewrites when it reads the value back, and the
    // escaping it undoes. Every value form itself has a live case in ClickHouseTcpParameterIntegrationTests.
    private static IEnumerable<TestCaseData> SqlTextCases()
    {
        // A top-level string is escaped but not quoted; the quoting is the composite's job.
        yield return new TestCaseData("O'Brien", "String").Returns(@"O\'Brien").SetName("String with a quote");
        yield return new TestCaseData(@"a\b", "String").Returns(@"a\\b").SetName("String with a backslash");
        yield return new TestCaseData("a\nb", "String").Returns(@"a\nb").SetName("String with a newline");
        yield return new TestCaseData("a\tb", "String").Returns(@"a\tb").SetName("String with a tab");

        // An Unspecified DateTime is a wall clock with no instant attached, which is what a type declaring no
        // timezone carries, so it stays legal. The ISO separator and the fixed fraction are the client's form.
        yield return new TestCaseData(Unspecified, "DateTime").Returns("2024-01-02T03:04:05").SetName("DateTime unspecified");
        yield return new TestCaseData(Unspecified, "DateTime64(3)").Returns("2024-01-02 03:04:05.0000000").SetName("DateTime64 unspecified");

        // A kinded value names an instant, so it is moved into the type's timezone to keep that instant.
        yield return new TestCaseData(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), "DateTime('Europe/Amsterdam')")
            .Returns("2024-01-02T04:04:05").SetName("DateTime shifted into the type timezone");
        yield return new TestCaseData(new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero), "DateTime('UTC')")
            .Returns("2024-01-02T03:04:05").SetName("DateTimeOffset in a UTC type");

        // A composite writes null as the literal the server reads inside one, and prints it back as NULL.
        yield return new TestCaseData(new int?[] { 1, null, 3 }, "Array(Nullable(Int32))").Returns("[1,null,3]").SetName("Array with a null element");
    }

    [TestCaseSource(nameof(SqlTextCases))]
    public string FormatSqlText_ValueOfDeclaredType_ProducesTheInnerSqlText(object value, string typeName)
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
        // The server quotes Identifier values, so client-side escaping would change the name.
        Assert.That(TcpParameterFormatter.FormatSqlText(@"we'ird\name", "Identifier", "p"), Is.EqualTo(@"we'ird\name"));
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

    // Native parameter values add a second escape pass and an outer quote.
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
