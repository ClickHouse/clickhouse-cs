using NUnit.Framework;

namespace ClickHouse.Driver.TestRunner.Tests;

// Ported from hint_test.go of the clickhouse-go harness.
public class TestHintTests
{
    private static readonly object?[] HintCases =
    [
        new object?[] { "SELECT 1; -- { serverError UNKNOWN_TABLE }", new[] { 60 }, Array.Empty<int>() },
        new object?[] { "SELECT 1; -- { serverError 60 }", new[] { 60 }, Array.Empty<int>() },
        new object?[] { "SELECT 1; -- { serverError 153, ILLEGAL_DIVISION }", new[] { 153, 153 }, Array.Empty<int>() },
        new object?[] { "SELECT; -- { clientError SYNTAX_ERROR }", Array.Empty<int>(), new[] { 62 } },
        new object?[] { "SELECT 1; -- { error 62 }", new[] { 62 }, new[] { 62 } },
        new object?[] { "SELECT 1 /* { serverError 60 } */;", new[] { 60 }, Array.Empty<int>() },
        new object?[] { "SELECT 1; -- { serverError 60 } because there is no table", new[] { 60 }, Array.Empty<int>() },
        new object?[] { "-- { serverError 60 }\nSELECT 1;", Array.Empty<int>(), Array.Empty<int>() },
        new object?[] { "SELECT '-- { serverError 60 }';", Array.Empty<int>(), Array.Empty<int>() },
        new object?[] { "SELECT 1; -- a comment", Array.Empty<int>(), Array.Empty<int>() },
    ];

    [TestCaseSource(nameof(HintCases))]
    public void Parse_Hint_GivesTheExpectedCodes(string input, int[] server, int[] client)
    {
        var h = TestHint.Parse(input);
        Assert.That(h.ParseError, Is.Null);
        Assert.That(h.ServerErrors, Is.EqualTo(server));
        Assert.That(h.ClientErrors, Is.EqualTo(client));
    }

    [Test]
    public void Parse_UnknownNameOrRetry_IsAParseError()
    {
        Assert.That(TestHint.Parse("SELECT 1; -- { serverError NOT_A_REAL_ERROR_NAME }").ParseError, Is.Not.Null);
        Assert.That(TestHint.Parse("SELECT 1; -- { retry 3 until serverError 60 }").ParseError, Is.Not.Null);
    }

    [Test]
    public void Matches_ClientErrorHint_AcceptsAServerError()
    {
        var h = TestHint.Parse("SELECT; -- { clientError SYNTAX_ERROR }");
        // Through a driver, the server finds syntax errors.
        Assert.That(h.Matches(62), Is.True);
        Assert.That(h.Matches(60), Is.False);
        Assert.That(h.Label, Is.EqualTo("clientError SYNTAX_ERROR"));
    }

    [Test]
    public void Parse_EchoHints_SetEcho()
    {
        Assert.That(TestHint.Parse("-- { echoOn }\nSELECT 1;").Echo, Is.True);
        Assert.That(TestHint.Parse("SELECT 1; -- { echoOff }").Echo, Is.False);
    }
}
