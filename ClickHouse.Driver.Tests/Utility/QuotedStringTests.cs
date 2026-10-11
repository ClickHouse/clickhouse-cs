using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.Utility;

[TestFixture]
public class QuotedStringTests
{
    [Test]
    public void EncloseColumnName_EscapesInteriorBackticks()
    {
        var result = "weird`name".EncloseColumnName();

        Assert.That(result, Is.EqualTo("`weird\\`name`"));
    }

    [Test]
    public void EncloseColumnName_AlreadyEnclosedButNeedsEscape_EscapesInterior()
    {
        var result = "`col`name`".EncloseColumnName();

        Assert.That(result, Is.EqualTo("`\\`col\\`name\\``"));
    }

    [Test]
    public void EncloseColumnName_HandlesNullOrEmpty()
    {
        Assert.That(StringExtensions.EncloseColumnName(null), Is.Null);
        Assert.That(StringExtensions.EncloseColumnName(string.Empty), Is.EqualTo(string.Empty));
    }

    [Test]
    public void QuoteSingle_AddsOuterSingleQuotes()
    {
        var input = "O\\'Reilly";
        var result = input.QuoteSingle();

        Assert.That(result, Is.EqualTo("'O\\'Reilly'"));
    }

    [Test]
    public void QuoteSingle_EmptyString_ReturnsQuotedEmptyString()
    {
        Assert.That(string.Empty.QuoteSingle(), Is.EqualTo("''"));
    }

    [Test]
    public void QuoteDouble_AddsOuterDoubleQuotes()
    {
        var input = "Product \\\"Items\\\"";
        var result = input.QuoteDouble();

        Assert.That(result, Is.EqualTo("\"Product \\\"Items\\\"\""));
    }

    [Test]
    public void QuoteDouble_EmptyString_ReturnsQuotedEmptyString()
    {
        Assert.That(string.Empty.QuoteDouble(), Is.EqualTo("\"\""));
    }

    [TestCase("O\\'Reilly", "'O\\'Reilly'")]
    [TestCase("Product \\\"Items\\\"", "\"Product \\\"Items\\\"\"")]
    public void Quote_AddsOuterQuotes(string input, string expected)
    {
        var result = input.Contains('\'')
            ? input.QuoteSingle()
            : input.QuoteDouble();

        Assert.That(result, Is.EqualTo(expected));
    }
}
