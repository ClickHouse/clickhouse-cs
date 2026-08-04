using System;
using System.Collections.Generic;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;
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

    // Issue #483: a byte[] bound to String used to fall through to value.ToString() and format as the
    // literal text "System.Byte[]". Byte payloads are now escaped byte-for-byte (printable ASCII
    // verbatim, ' and \ backslash-escaped, anything else \xHH), so data that is not valid UTF-8
    // survives instead of collapsing to U+FFFD as UTF-8 decoding would produce.
    [TestCaseSource(nameof(BytePayloadCases))]
    public string Format_BytesBoundToStringLikeType_EscapesRawBytes(object value, string clickHouseType)
        => HttpParameterFormatter.Format(
            new ClickHouseDbParameter { ParameterName = "b", Value = value }, clickHouseType, TypeSettings.Default);

    private static IEnumerable<TestCaseData> BytePayloadCases()
    {
        static TestCaseData Case(string name, object value, string clickHouseType, string expected)
            => new TestCaseData(value, clickHouseType).Returns(expected).SetName($"Format_Bytes_{name}");

        yield return Case("ByteArrayToString", new byte[] { 0x41, 0x42, 0x43 }, "String", "ABC");
        yield return Case("ByteArrayToFixedString", new byte[] { 0x41, 0x42, 0x43 }, "FixedString(3)", "ABC");
        yield return Case("ReadOnlyMemoryToString", (ReadOnlyMemory<byte>)new byte[] { 0x41, 0x42, 0x43 }, "String", "ABC");
        yield return Case("ReadOnlyMemoryNonUtf8", (ReadOnlyMemory<byte>)new byte[] { 0xFF, 0x00, 0x41 }, "String", @"\xFF\x00A");
        yield return Case("Empty", Array.Empty<byte>(), "String", "");
        yield return Case("SingleInvalidUtf8Byte", new byte[] { 0xFF }, "String", @"\xFF");
        yield return Case("InvalidUtf8Sequence", new byte[] { 0xFF, 0xFE }, "String", @"\xFF\xFE");
        yield return Case("PrintableAroundInvalidByte", new byte[] { 0x41, 0xFF, 0x42 }, "String", @"A\xFFB");
        yield return Case("ValidUtf8Multibyte", new byte[] { 0xC3, 0xA9 }, "String", @"\xC3\xA9"); // "é"
        yield return Case("ControlAndDelBytes", new byte[] { 0x00, 0x0A, 0x09, 0x7F }, "String", @"\x00\x0A\x09\x7F");
        yield return Case("QuoteAndBackslash", new byte[] { 0x27, 0x5C }, "String", @"\'\\");
    }

    [Test]
    public void Format_ByteArrayBoundToStringInQuotedContext_IsSingleQuoted()
    {
        // Composite contexts (e.g. Array(String)) format elements with quote=true.
        var formatted = HttpParameterFormatter.Format(new StringType(), new byte[] { 0x41, 0x42, 0x43 }, true);
        Assert.That(formatted, Is.EqualTo("'ABC'"));
    }

    // Issue #483: TimeOnly does not implement IConvertible, so it used to throw InvalidCastException
    // (Time) or hit the default throw (Time64), even with an explicit {name:Time} hint.
    [TestCase("Time", 14, 30, 0, 0, ExpectedResult = "14:30:00", TestName = "Format_TimeOnly_Time")]
    [TestCase("Time64(3)", 14, 30, 0, 500, ExpectedResult = "14:30:00.500", TestName = "Format_TimeOnly_Time64WithFraction")]
    public string Format_TimeOnly_ReturnsFormattedTime(string clickHouseType, int hour, int minute, int second, int millisecond)
        => HttpParameterFormatter.Format(
            new ClickHouseDbParameter { ParameterName = "t", Value = new TimeOnly(hour, minute, second, millisecond) },
            clickHouseType,
            TypeSettings.Default);
}
