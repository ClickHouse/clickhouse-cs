using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Formats;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Formats;

public class HttpParameterFormatterTests
{
    // Server-side {name:Identifier} parameters are sent verbatim; the server substitutes the value
    // as a bare SQL identifier and owns all backtick quoting/escaping. The formatter must therefore
    // emit the raw value with no quoting and no escaping.
    [TestCase("test_db", ExpectedResult = "test_db", TestName = "Format_IdentifierType_PlainName_ReturnsRawValue")]
    [TestCase("O'Brien", ExpectedResult = "O'Brien", TestName = "Format_IdentifierType_SingleQuote_NotEscaped")]
    [TestCase("weird`col", ExpectedResult = "weird`col", TestName = "Format_IdentifierType_Backtick_NotEscaped")]
    [TestCase(@"a\b", ExpectedResult = @"a\b", TestName = "Format_IdentifierType_Backslash_NotEscaped")]
    public string Format_IdentifierType_ReturnsRawUnquotedUnescapedValue(string value)
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "id", Value = value, ClickHouseType = "Identifier" };
        return HttpParameterFormatter.Format(parameter, "Identifier", TypeSettings.Default);
    }

    [Test]
    public void Format_StringType_SingleQuote_EscapesValue()
    {
        // Contrast with the sibling String type, whose behavior is unchanged: String escapes special
        // characters (here ' -> \'), confirming the new Identifier arm is a deliberate no-escape path
        // and not a blanket change to string-valued formatting.
        var parameter = new ClickHouseDbParameter { ParameterName = "s", Value = "O'Brien", ClickHouseType = "String" };
        var formatted = HttpParameterFormatter.Format(parameter, "String", TypeSettings.Default);
        Assert.That(formatted, Is.EqualTo(@"O\'Brien"));
    }
}
