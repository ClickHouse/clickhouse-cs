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

    // --- Issue #483: byte[] / ReadOnlyMemory<byte> bound to String / FixedString ---------------
    // The binary write path (Types/StringType.cs) accepts string, byte[] and ReadOnlyMemory<byte>.
    // The HTTP parameter path used to fall through to value.ToString() for String, silently
    // inserting the literal text "System.Byte[]" instead of the payload.

    [Test]
    public void Format_ByteArrayBoundToString_ReturnsDecodedText()
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "b", Value = new byte[] { 0x41, 0x42, 0x43 } };
        Assert.That(HttpParameterFormatter.Format(parameter, "String", TypeSettings.Default), Is.EqualTo("ABC"));
    }

    [Test]
    public void Format_ByteArrayBoundToFixedString_ReturnsDecodedText()
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "b", Value = new byte[] { 0x41, 0x42, 0x43 } };
        Assert.That(HttpParameterFormatter.Format(parameter, "FixedString(3)", TypeSettings.Default), Is.EqualTo("ABC"));
    }

    [Test]
    public void Format_ReadOnlyMemoryOfByteBoundToString_ReturnsDecodedText()
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "b", Value = (ReadOnlyMemory<byte>)new byte[] { 0x41, 0x42, 0x43 } };
        Assert.That(HttpParameterFormatter.Format(parameter, "String", TypeSettings.Default), Is.EqualTo("ABC"));
    }

    [Test]
    public void Format_ByteArrayBoundToStringInQuotedContext_IsSingleQuoted()
    {
        // Composite contexts (e.g. Array(String)) format elements with quote=true.
        var formatted = HttpParameterFormatter.Format(new StringType(), new byte[] { 0x41, 0x42, 0x43 }, true);
        Assert.That(formatted, Is.EqualTo("'ABC'"));
    }

    // A byte[] is escaped byte-for-byte into ClickHouse escaped-string text, so a payload that is
    // NOT valid UTF-8 round-trips losslessly (via \xHH) instead of collapsing to the U+FFFD
    // replacement char Encoding.UTF8.GetString would produce. '\' and '\'' are backslash-escaped;
    // printable ASCII passes through; every other byte becomes \xHH.
    [TestCaseSource(nameof(ByteArrayEscapingCases))]
    public string Format_ByteArrayBoundToString_EscapesRawBytesFaithfully(byte[] payload)
        => HttpParameterFormatter.Format(
            new ClickHouseDbParameter { ParameterName = "b", Value = payload }, "String", TypeSettings.Default);

    private static IEnumerable<TestCaseData> ByteArrayEscapingCases()
    {
        yield return new TestCaseData((object)new byte[] { 0x41, 0x42, 0x43 }).Returns("ABC").SetName("Format_ByteArray_PrintableAscii");
        yield return new TestCaseData((object)new byte[] { 0xFF }).Returns(@"\xFF").SetName("Format_ByteArray_SingleInvalidUtf8Byte");
        yield return new TestCaseData((object)new byte[] { 0xFF, 0xFE }).Returns(@"\xFF\xFE").SetName("Format_ByteArray_InvalidUtf8Sequence");
        yield return new TestCaseData((object)new byte[] { 0x41, 0xFF, 0x42 }).Returns(@"A\xFFB").SetName("Format_ByteArray_PrintableAroundInvalidByte");
        yield return new TestCaseData((object)new byte[] { 0xC3, 0xA9 }).Returns(@"\xC3\xA9").SetName("Format_ByteArray_ValidUtf8Multibyte"); // "é"
        yield return new TestCaseData((object)new byte[] { 0x00, 0x0A, 0x09, 0x7F }).Returns(@"\x00\x0A\x09\x7F").SetName("Format_ByteArray_ControlAndDelBytes");
        yield return new TestCaseData((object)new byte[] { 0x27, 0x5C }).Returns(@"\'\\").SetName("Format_ByteArray_QuoteAndBackslash");
    }

    [Test]
    public void Format_ReadOnlyMemoryOfByte_NonUtf8_EscapesRawBytesFaithfully()
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "b", Value = (ReadOnlyMemory<byte>)new byte[] { 0xFF, 0x00, 0x41 } };
        Assert.That(HttpParameterFormatter.Format(parameter, "String", TypeSettings.Default), Is.EqualTo(@"\xFF\x00A"));
    }

    // --- Issue #483: TimeOnly bound to Time / Time64 ------------------------------------------
    // TimeOnly does not implement IConvertible, so it used to throw InvalidCastException (Time) or
    // hit the default throw (Time64), even when an explicit {name:Time} hint was supplied.

    [Test]
    public void Format_TimeOnlyBoundToTime_ReturnsFormattedTime()
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "t", Value = new TimeOnly(14, 30, 0) };
        Assert.That(HttpParameterFormatter.Format(parameter, "Time", TypeSettings.Default), Is.EqualTo("14:30:00"));
    }

    [Test]
    public void Format_TimeOnlyBoundToTime64_ReturnsFormattedTimeWithFraction()
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "t", Value = new TimeOnly(14, 30, 0, 500) };
        Assert.That(HttpParameterFormatter.Format(parameter, "Time64(3)", TypeSettings.Default), Is.EqualTo("14:30:00.500"));
    }

    // Contrast: TimeSpan binding (the previously-supported type) is unchanged.
    [Test]
    public void Format_TimeSpanBoundToTime_ReturnsFormattedTime()
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "t", Value = new TimeSpan(14, 30, 0) };
        Assert.That(HttpParameterFormatter.Format(parameter, "Time", TypeSettings.Default), Is.EqualTo("14:30:00"));
    }
}
