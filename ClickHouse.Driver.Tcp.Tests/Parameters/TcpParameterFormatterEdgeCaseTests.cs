using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Parameters;

namespace ClickHouse.Driver.Tcp.Tests.Parameters;

// The arms a per-type round-trip does not reach: types the server never sends back as themselves, the error
// paths, and the CLR shapes that map onto one type in more than one way.
[TestFixture]
public class TcpParameterFormatterEdgeCaseTests
{
    private static string Format(object value, string typeName)
        => TcpParameterFormatter.FormatSqlText(value, typeName, "p");

    [Test]
    public void FormatSqlText_NothingType_ProducesTheNullMarker()
    {
        // Nothing holds no value, so whatever arrives formats as null.
        Assert.That(Format("ignored", "Nothing"), Is.EqualTo(@"\N"));
    }

    [Test]
    public void FormatSqlText_FixedStringFromBytes_DecodesAsUtf8()
    {
        Assert.That(Format(Encoding.UTF8.GetBytes("héllo"), "FixedString(8)"), Is.EqualTo("héllo"));
    }

    [Test]
    public void FormatSqlText_FixedStringFromBytesInsideAnArray_IsQuoted()
    {
        Assert.That(Format(new[] { Encoding.UTF8.GetBytes("a'b") }, "Array(FixedString(4))"), Is.EqualTo(@"['a\'b']"));
    }

    [Test]
    public void FormatSqlText_DateFromADateTime_DropsTheTimeOfDay()
    {
        Assert.That(Format(new DateTime(2024, 1, 2, 3, 4, 5), "Date"), Is.EqualTo("2024-01-02"));
    }

    [Test]
    public void FormatSqlText_DateFromADateTimeOffset_DropsTheTimeOfDay()
    {
        Assert.That(Format(new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero), "Date32"), Is.EqualTo("2024-01-02"));
    }

    [TestCase(3723, ExpectedResult = "1:02:03", TestName = "Time from whole seconds")]
    [TestCase(-3723, ExpectedResult = "-1:02:03", TestName = "Time negative")]
    [TestCase(0, ExpectedResult = "0:00:00", TestName = "Time zero")]
    public string FormatSqlText_TimeFromSeconds_UsesTheHourMinuteSecondForm(int seconds)
        => Format(seconds, "Time");

    [Test]
    public void FormatSqlText_TimeFromATimeSpan_RoundsToWholeSeconds()
    {
        Assert.That(Format(new TimeSpan(0, 1, 2, 3, 600), "Time"), Is.EqualTo("1:02:04"));
    }

    [Test]
    public void FormatSqlText_Time64_KeepsTheDeclaredScale()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Format(new TimeSpan(0, 1, 2, 3, 123), "Time64(3)"), Is.EqualTo("1:02:03.123"));
            Assert.That(Format(new TimeSpan(0, 1, 2, 3, 123), "Time64(1)"), Is.EqualTo("1:02:03.1"));
            Assert.That(Format(-new TimeSpan(0, 1, 2, 3, 123), "Time64(3)"), Is.EqualTo("-1:02:03.123"));
        });
    }

    [Test]
    public void FormatSqlText_NestedRows_WrapsTupleRowsInAnArray()
    {
        object[] rows = [(1, "x"), (2, "y")];

        Assert.That(Format(rows, "Nested(a UInt8, b String)"), Is.EqualTo("[(1,'x'),(2,'y')]"));
    }

    [Test]
    public void FormatSqlText_NestedSingleRow_FormatsAsOneTuple()
    {
        Assert.That(Format((1, "x"), "Nested(a UInt8, b String)"), Is.EqualTo("(1,'x')"));
    }

    [Test]
    public void FormatSqlText_TupleFromAList_ReadsElementsByPosition()
    {
        Assert.That(Format(new List<object> { "a", 1 }, "Tuple(String, Int32)"), Is.EqualTo("('a',1)"));
    }

    [Test]
    public void FormatSqlText_JsonFromAString_PassesTheTextThrough()
    {
        Assert.That(Format(@"{""a"":1}", "JSON"), Is.EqualTo(@"{""a"":1}"));
    }

    [Test]
    public void FormatSqlText_JsonFromAnObject_SerializesIt()
    {
        Assert.That(Format(new Dictionary<string, int> { ["a"] = 1 }, "JSON"), Is.EqualTo(@"{""a"":1}"));
    }

    [Test]
    public void FormatSqlText_NullInsideAMap_ProducesTheLiteralNull()
    {
        var value = new Dictionary<string, string> { ["k"] = null };

        Assert.That(Format(value, "Map(String, Nullable(String))"), Is.EqualTo("{'k' : null}"));
    }

    [Test]
    public void FormatSqlText_LowCardinalityInsideAnArray_QuotesTheInnerValue()
    {
        Assert.That(Format(new[] { "a" }, "Array(LowCardinality(String))"), Is.EqualTo("['a']"));
    }

    [Test]
    public void FormatSqlText_NullableDateTimeInsideAnArray_IsQuoted()
    {
        object[] value = [new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified), null];

        Assert.That(Format(value, "Array(Nullable(DateTime))"), Is.EqualTo("['2024-01-02T03:04:05',null]"));
    }

    [Test]
    public void FormatSqlText_VariantWithNoMatchingAlternative_Throws()
    {
        var exception = Assert.Throws<ArgumentException>(() => Format(IPAddress.Loopback, "Variant(Int64, String)"));

        Assert.That(exception.Message, Does.Contain("Variant"));
    }

    [Test]
    public void FormatSqlText_VariantHoldingANullableAlternative_MatchesThroughTheWrapper()
    {
        Assert.That(Format(7L, "Variant(Nullable(Int64), String)"), Is.EqualTo("7"));
    }

    [Test]
    public void FormatSqlText_DecimalFromAnUnparsableString_Throws()
    {
        Assert.Throws<ArgumentException>(() => Format("not-a-decimal", "Decimal64(4)"));
    }

    [Test]
    public void FormatSqlText_DateTimeFromANonDateValue_Throws()
    {
        Assert.Throws<ArgumentException>(() => Format(new object(), "DateTime"));
    }

    [Test]
    public void FormatSqlText_DateTime64WithATimezoneAndAPrecision_ReadsTheTimezoneNotThePrecision()
    {
        // The timezone is the last argument, so the precision digit must not be mistaken for one.
        var instant = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(Format(instant, "DateTime64(3, 'Europe/Amsterdam')"), Is.EqualTo("2024-01-02 04:04:05.0000000"));
            Assert.That(Format(instant, "DateTime64(3)"), Is.EqualTo("2024-01-02 03:04:05.0000000"));
        });
    }

    [Test]
    public void FormatSqlText_BFloat16AndWideIntegers_UseInvariantText()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Format(1.5f, "BFloat16"), Is.EqualTo("1.5"));
            Assert.That(Format(UInt128.MaxValue, "UInt128"), Is.EqualTo("340282366920938463463374607431768211455"));
            Assert.That(Format(new Int256(1, 0, 0, 0), "Int256"), Is.Not.Empty);
        });
    }

    [TestCase("Array(Array(Array(Int32)))", "deeper", TestName = "Declared deeper than the CLR rank")]
    [TestCase("Array(Int32)", "shallower", TestName = "Declared shallower than the CLR rank")]
    public void FormatSqlText_MultidimensionalArrayOfTheWrongDepth_SaysWhichWayToChangeIt(string typeName, string suggestion)
    {
        // Without this check the rank-3 case would emit three bracket levels for a two-level type and only the
        // server would notice.
        var exception = Assert.Throws<ArgumentException>(() => Format(new int[,] { { 1, 2 }, { 3, 4 } }, typeName));

        Assert.That(exception.Message, Does.Contain(suggestion));
    }

    [Test]
    public void FormatSqlText_UnsupportedTypeName_ThrowsNamingTheType()
    {
        var exception = Assert.Throws<ArgumentException>(() => Format(1, "Point"));

        Assert.That(exception.Message, Does.Contain("Point"));
    }
}

// The last rung of type resolution: what a value formats as when the query names no type for it.
[TestFixture]
public class ParameterTypeInferenceTests
{
    private static IEnumerable<TestCaseData> InferenceCases()
    {
        yield return new TestCaseData(null).Returns("Nullable(Nothing)").SetName("null");
        yield return new TestCaseData(true).Returns("Bool").SetName("bool");
        yield return new TestCaseData((byte)1).Returns("UInt8").SetName("byte");
        yield return new TestCaseData((sbyte)1).Returns("Int8").SetName("sbyte");
        yield return new TestCaseData((ushort)1).Returns("UInt16").SetName("ushort");
        yield return new TestCaseData((short)1).Returns("Int16").SetName("short");
        yield return new TestCaseData(1u).Returns("UInt32").SetName("uint");
        yield return new TestCaseData(1).Returns("Int32").SetName("int");
        yield return new TestCaseData(1ul).Returns("UInt64").SetName("ulong");
        yield return new TestCaseData(1L).Returns("Int64").SetName("long");
        yield return new TestCaseData((UInt128)1).Returns("UInt128").SetName("UInt128");
        yield return new TestCaseData((Int128)1).Returns("Int128").SetName("Int128");
        yield return new TestCaseData(1.5f).Returns("Float32").SetName("float");
        yield return new TestCaseData(1.5d).Returns("Float64").SetName("double");
        yield return new TestCaseData(1.2345m).Returns("Decimal128(4)").SetName("decimal keeps its scale");
        yield return new TestCaseData("x").Returns("String").SetName("string");
        yield return new TestCaseData('c').Returns("String").SetName("char");
        yield return new TestCaseData(new byte[] { 1 }).Returns("String").SetName("byte array");
        yield return new TestCaseData(Guid.Empty).Returns("UUID").SetName("Guid");
        yield return new TestCaseData(new DateOnly(2024, 1, 2)).Returns("Date").SetName("DateOnly");
        yield return new TestCaseData(TimeSpan.Zero).Returns("Time64(9)").SetName("TimeSpan");
        yield return new TestCaseData(new DateTime(2024, 1, 2)).Returns("DateTime64(7, 'UTC')").SetName("DateTime");
        yield return new TestCaseData(IPAddress.Parse("1.2.3.4")).Returns("IPv4").SetName("IPv4 address");
        yield return new TestCaseData(IPAddress.Parse("::1")).Returns("IPv6").SetName("IPv6 address");
        yield return new TestCaseData(new[] { 1, 2 }).Returns("Array(Int32)").SetName("array");
        yield return new TestCaseData(Array.Empty<int>()).Returns("Array(Nullable(String))").SetName("empty array");
        yield return new TestCaseData(new int?[] { null, 5 }).Returns("Array(Int32)").SetName("array skips leading nulls");
        yield return new TestCaseData(("a", 1)).Returns("Tuple(String, Int32)").SetName("tuple");
    }

    [TestCaseSource(nameof(InferenceCases))]
    public string Infer_ValueWithNoDeclaredType_MapsToItsClickHouseType(object value)
        => ParameterTypeInference.Infer(value, "p");

    [Test]
    public void Infer_Dictionary_MapsToAMapOfTheFirstPairsTypes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ParameterTypeInference.Infer(new Dictionary<string, int> { ["a"] = 1 }, "p"), Is.EqualTo("Map(String, Int32)"));
            Assert.That(ParameterTypeInference.Infer(new Dictionary<string, int>(), "p"), Is.EqualTo("Map(String, String)"));
        });
    }

    [Test]
    public void Infer_ClickHouseDecimal_KeepsItsOwnScale()
    {
        Assert.That(ParameterTypeInference.Infer(new ClickHouseDecimal(12345, 4), "p"), Is.EqualTo("Decimal128(4)"));
    }

    [Test]
    public void Infer_UnmappableValue_ThrowsNamingTheParameterAndTheRemedies()
    {
        var exception = Assert.Throws<ArgumentException>(() => ParameterTypeInference.Infer(new object(), "widget"));

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Does.Contain("widget"));
            Assert.That(exception.Message, Does.Contain("ClickHouseType"));
        });
    }
}
