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
    public void QuoteSingle_EscapesQuotesAndAddsOuterQuotes()
    {
        var result = "'O'Reilly'".QuoteSingle();

        Assert.That(result, Is.EqualTo("'\\'O\\'Reilly\\''"));
    }

    [Test]
    public void QuoteSingle_EmptyString_ReturnsQuotedEmptyString()
    {
        Assert.That(string.Empty.QuoteSingle(), Is.EqualTo("''"));
    }

    [Test]
    public void QuoteDouble_EscapesQuotesAndAddsOuterQuotes()
    {
        var input = "\"Product \"Items\"\"";
        var result = input.QuoteDouble();

        Assert.That(result, Is.EqualTo("\"\\\"Product \\\"Items\\\"\\\"\""));
    }

    [Test]
    public void QuoteDouble_EmptyString_ReturnsQuotedEmptyString()
    {
        Assert.That(string.Empty.QuoteDouble(), Is.EqualTo("\"\""));
    }
}
