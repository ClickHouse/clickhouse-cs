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
    public void FormatSqlText_StringFromBytes_DecodesAsUtf8()
    {
        // Only FixedString read the bytes, so a String parameter printed the CLR type name instead.
        Assert.That(Format(Encoding.UTF8.GetBytes("héllo"), "String"), Is.EqualTo("héllo"));
    }

    [Test]
    public void FormatSqlText_StringFromBytesInsideAnArray_IsEscapedAndQuoted()
    {
        Assert.That(Format(new[] { Encoding.UTF8.GetBytes(@"a'b\c") }, "Array(String)"), Is.EqualTo(@"['a\'b\\c']"));
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
    public void FormatSqlText_VariantHoldingJson_PicksTheJsonAlternative()
    {
        var value = new Dictionary<string, int> { ["a"] = 1 };

        Assert.That(Format(value, "Variant(JSON, UInt64)"), Is.EqualTo(@"{""a"":1}"));
    }

    [Test]
    public void FormatSqlText_VariantHoldingAMap_PrefersItToJson()
    {
        var value = new Dictionary<string, int> { ["a"] = 1 };

        Assert.That(
            Format(value, "Variant(JSON, Map(String, Int32))"),
            Is.EqualTo("{'a' : 1}"));
    }

    [Test]
    public void FormatSqlText_VariantHoldingAMapThatDoesNotFit_FallsBackToJson()
    {
        var value = new Dictionary<string, string> { ["a"] = "x" };

        Assert.That(
            Format(value, "Variant(Map(String, Int32), JSON)"),
            Is.EqualTo(@"{""a"":""x""}"));
    }

    [Test]
    public void FormatSqlText_VariantWithAnUnrelatedArrayAlternative_PreservesTheJsonTupleShape()
    {
        object value = Tuple.Create(1, "x");

        Assert.That(
            Format(value, "Variant(JSON, Array(Int32))"),
            Is.EqualTo(@"{""Item1"":1,""Item2"":""x""}"));
    }

    [Test]
    public void FormatSqlText_VariantWithARejectedMapAlternative_PreservesTheNestedJsonTupleShape()
    {
        var value = new Dictionary<string, Tuple<int, string>> { ["a"] = Tuple.Create(1, "x") };

        Assert.That(
            Format(value, "Variant(Map(String, Int32), JSON)"),
            Is.EqualTo(@"{""a"":{""Item1"":1,""Item2"":""x""}}"));
    }

    [Test]
    public void FormatSqlText_VariantWithARejectedArrayAlternative_PreservesTheNestedJsonTupleShape()
    {
        Tuple<int, string>[] value = [Tuple.Create(1, "x")];

        Assert.That(
            Format(value, "Variant(Array(Int32), JSON)"),
            Is.EqualTo(@"[{""Item1"":1,""Item2"":""x""}]"));
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

    // Non-zero lower bounds require GetLowerBound; zero-based indexing would throw.
    [Test]
    public void FormatSqlText_ArrayWithANonZeroLowerBound_ReadsFromItsOwnBounds()
    {
        var value = Array.CreateInstance(typeof(int), [2, 3], [5, 10]);
        for (int i = 0; i < 2; i++)
        {
            for (int j = 0; j < 3; j++)
            {
                value.SetValue((i * 3) + j, 5 + i, 10 + j);
            }
        }

        Assert.That(Format(value, "Array(Array(Int32))"), Is.EqualTo("[[0,1,2],[3,4,5]]"));
    }

    [TestCase(0, 5, ExpectedResult = "[]", TestName = "Rank-2 array with no rows")]
    [TestCase(3, 0, ExpectedResult = "[[],[],[]]", TestName = "Rank-2 array of empty rows")]
    public string FormatSqlText_ArrayWithAZeroLengthAxis_KeepsTheOtherAxis(int rows, int columns)
        => Format(new int[rows, columns], "Array(Array(Int32))");

    [Test]
    public void FormatSqlText_MapWithKeysDifferingOnlyByCase_KeepsBoth()
    {
        // A ClickHouse Map is an Array(Tuple(..)), so it does not collapse keys. The formatter must not either.
        var value = new Dictionary<string, int> { ["A"] = 1, ["a"] = 2 };

        Assert.That(Format(value, "Map(String, Int32)"), Is.EqualTo("{'A' : 1,'a' : 2}"));
    }

    // Refuse instants without a declared timezone rather than guessing the session timezone.
    private static IEnumerable<TestCaseData> InstantWithoutATimezoneCases()
    {
        yield return new TestCaseData(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), "DateTime")
            .SetName("DateTime of Kind Utc");
        yield return new TestCaseData(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Local), "DateTime")
            .SetName("DateTime of Kind Local");
        yield return new TestCaseData(new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromHours(9)), "DateTime")
            .SetName("DateTimeOffset");
        yield return new TestCaseData(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), "DateTime64(3)")
            .SetName("DateTime64, whose digit is a precision and not a timezone");
        yield return new TestCaseData(new[] { new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc) }, "Array(DateTime)")
            .SetName("Inside a composite");
        yield return new TestCaseData(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), "Nullable(DateTime)")
            .SetName("Through a Nullable");
    }

    [TestCaseSource(nameof(InstantWithoutATimezoneCases))]
    public void FormatSqlText_InstantForATypeWithNoTimezone_ThrowsAndSaysHowToFixIt(object value, string typeName)
    {
        var exception = Assert.Throws<ArgumentException>(() => Format(value, typeName));

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Does.Contain("names an instant"), "says what about the value is the problem");
            Assert.That(exception.Message, Does.Contain("declares no timezone"), "says what about the type is the problem");
            Assert.That(exception.Message, Does.Contain("session timezone"), "says what the server would do instead");
            Assert.That(exception.Message, Does.Contain("'UTC'"), "shows the type to write");
            Assert.That(exception.Message, Does.Contain("Kind=Unspecified"), "offers the other way out");
            Assert.That(exception.Message, Does.Contain("Parameter 'p'"), "names the parameter");
        });
    }

    [Test]
    public void FormatSqlText_InstantForADateTime64WithNoTimezone_SuggestsAScaleAndATimezone()
    {
        // The suggestion has to stay valid for the type it is about: DateTime64('UTC') is not a type.
        var exception = Assert.Throws<ArgumentException>(
            () => Format(new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc), "DateTime64(3)"));

        Assert.That(exception.Message, Does.Contain("DateTime64(3, 'UTC')"));
    }

    [TestCase("DateTime('UTC')", TestName = "A named timezone")]
    [TestCase("DateTime('Europe/Amsterdam')", TestName = "A timezone that is not UTC")]
    [TestCase("DateTime64(3, 'UTC')", TestName = "A scale and a timezone")]
    public void FormatSqlText_InstantForATypeThatDeclaresATimezone_IsAccepted(string typeName)
    {
        Assert.DoesNotThrow(() => Format(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), typeName));
    }

    [TestCase("DateTime", ExpectedResult = "2024-01-02T03:04:05", TestName = "DateTime keeps the wall clock")]
    [TestCase("DateTime64(3)", ExpectedResult = "2024-01-02 03:04:05.0000000", TestName = "DateTime64 keeps the wall clock")]
    public string FormatSqlText_UnspecifiedKindForATypeWithNoTimezone_StillPasses(string typeName)
    {
        // Unspecified means a wall-clock time with no instant attached, which is exactly what a type with no
        // timezone carries. Nothing is lost, so this stays legal.
        return Format(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified), typeName);
    }

    [Test]
    public void FormatSqlText_VariantWithNoMatchingAlternative_Throws()
    {
        var exception = Assert.Throws<ArgumentException>(() => Format(IPAddress.Loopback, "Variant(Int64, String)"));

        Assert.That(exception.Message, Does.Contain("Variant"));
    }

    [Test]
    public void FormatSqlText_VariantHoldingAByteArray_PrefersTheArrayAlternative()
    {
        // Prefer Array for byte[] so String does not format the CLR type name.
        Assert.That(Format(new byte[] { 1, 2 }, "Variant(Array(UInt8), String)"), Is.EqualTo("[1,2]"));
    }

    [Test]
    public void FormatSqlText_VariantHoldingAnUnmappableValue_NamesTheVariantNotAnInventedParameter()
    {
        // Report the declared Variant, not an internal placeholder used during matching.
        var exception = Assert.Throws<ArgumentException>(() => Format(new object(), "Variant(Int64, String)"));

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Does.Contain("no alternative"));
            Assert.That(exception.Message, Does.Not.Contain("{variant:"));
        });
    }

    [TestCase("1E2", ExpectedResult = "100", TestName = "Decimal in exponent form")]
    [TestCase("1,234.50", ExpectedResult = "1234.50", TestName = "Decimal with thousands separators")]
    [TestCase("(5)", ExpectedResult = "-5", TestName = "Decimal in accounting parentheses")]
    [TestCase("1.2345", ExpectedResult = "1.2345", TestName = "Decimal in plain form")]
    public string FormatSqlText_DecimalStringForm_AcceptsWhatTheHttpFormatterAccepts(string text)
        => Format(text, "Decimal64(4)");

    [Test]
    public void FormatSqlText_Time64AtAMidpoint_RoundsToEven()
    {
        // Explicit pre-rounding is required because decimal formatting rounds midpoints away from zero.
        Assert.Multiple(() =>
        {
            Assert.That(Format(TimeSpan.FromSeconds(0.5), "Time64(0)"), Is.EqualTo("0:00:00"));
            Assert.That(Format(TimeSpan.FromSeconds(1.5), "Time64(0)"), Is.EqualTo("0:00:02"));
        });
    }

    // Match Array alternatives by element type, not only by the outer name.
    [TestCase("Variant(Array(Int32), Array(String))", ExpectedResult = "['a','b']", TestName = "Array picks by element type")]
    [TestCase("Variant(Array(String), Array(Int32))", ExpectedResult = "['a','b']", TestName = "Array picks by element type, declared the other way round")]
    public string FormatSqlText_VariantOfTwoArrays_PicksTheOneWhoseElementsFit(string typeName)
        => Format(new[] { "a", "b" }, typeName);

    [Test]
    public void FormatSqlText_VariantOfTwoArraysHoldingIntegers_PicksTheIntegerArray()
    {
        Assert.That(Format(new[] { 1, 2 }, "Variant(Array(String), Array(Int32))"), Is.EqualTo("[1,2]"));
    }

    [Test]
    public void FormatSqlText_VariantOfTwoMaps_PicksTheOneWhoseValuesFit()
    {
        var value = new Dictionary<string, string> { ["k"] = "v" };

        Assert.That(Format(value, "Variant(Map(String, Int32), Map(String, String))"), Is.EqualTo("{'k' : 'v'}"));
    }

    [Test]
    public void FormatSqlText_VariantOfTwoTuples_PicksTheOneWhoseElementsFit()
    {
        Assert.That(Format(("a", 1), "Variant(Tuple(Int32, Int32), Tuple(String, Int32))"), Is.EqualTo("('a',1)"));
    }

    [Test]
    public void FormatSqlText_VariantOfTuplesOfDifferentArity_PicksTheMatchingArity()
    {
        Assert.That(Format((1, 2), "Variant(Tuple(Int32, Int32, Int32), Tuple(Int32, Int32))"), Is.EqualTo("(1,2)"));
    }

    [Test]
    public void FormatSqlText_VariantWhereNoArrayElementTypeFits_Throws()
    {
        var exception = Assert.Throws<ArgumentException>(
            () => Format(new[] { "a" }, "Variant(Array(Int32), Array(Date))"));

        Assert.That(exception.Message, Does.Contain("no alternative"));
    }

    [Test]
    public void FormatSqlText_VariantWithAnEmptyArray_TakesTheFirstArrayAlternative()
    {
        // An empty sequence fits any element type, so select the first Array alternative.
        Assert.That(Format(Array.Empty<string>(), "Variant(Array(Int32), Array(String))"), Is.EqualTo("[]"));
    }

    [Test]
    public void FormatSqlText_VariantHoldingKeyValuePairs_PicksTheMapAlternative()
    {
        // A pair sequence is also an IEnumerable, so without its own arm the Array alternative would claim it.
        KeyValuePair<string, int>[] pairs = [new("a", 1)];

        Assert.That(Format(pairs, "Variant(Array(String), Map(String, Int32))"), Is.EqualTo("{'a' : 1}"));
    }

    [Test]
    public void FormatSqlText_VariantHoldingKeyValuePairsWhoseValuesDoNotFit_Throws()
    {
        KeyValuePair<string, string>[] pairs = [new("a", "b")];

        var exception = Assert.Throws<ArgumentException>(() => Format(pairs, "Variant(Map(String, Int32), Int64)"));

        Assert.That(exception.Message, Does.Contain("no alternative"));
    }

    [TestCase("Variant(Int64, String)", TestName = "A dictionary where no alternative is a Map")]
    public void FormatSqlText_VariantWhereACompositeMatchesNoAlternative_Throws(string typeName)
    {
        var value = new Dictionary<string, int> { ["a"] = 1 };

        var exception = Assert.Throws<ArgumentException>(() => Format(value, typeName));

        Assert.That(exception.Message, Does.Contain("no alternative"));
    }

    [Test]
    public void FormatSqlText_VariantHoldingATupleWhereNoAlternativeIsATuple_Throws()
    {
        var exception = Assert.Throws<ArgumentException>(() => Format(("a", 1), "Variant(Int64, String)"));

        Assert.That(exception.Message, Does.Contain("no alternative"));
    }

    [Test]
    public void FormatSqlText_VariantHoldingASequenceWhereNoAlternativeIsAnArray_Throws()
    {
        // The sequence never reaches the recursive Array arm, so the fallback name comparison is what has to
        // reject it.
        var exception = Assert.Throws<ArgumentException>(() => Format(new[] { 1, 2 }, "Variant(Int64, String)"));

        Assert.That(exception.Message, Does.Contain("no alternative"));
    }

    [Test]
    public void FormatSqlText_MapTypeGivenAValueThatIsNeitherDictionaryNorPairs_Throws()
    {
        var exception = Assert.Throws<ArgumentException>(() => Format(42, "Map(String, Int32)"));

        Assert.That(exception.Message, Does.Contain("Map(String, Int32)"));
    }

    [Test]
    public void FormatSqlText_MapGivenAsKeyValuePairs_FormatsInSequenceOrder()
    {
        // Preserve the ordered, duplicate-key Map shape returned by the read path.
        KeyValuePair<string, int>[] pairs = [new("b", 2), new("a", 1), new("b", 3)];

        Assert.That(Format(pairs, "Map(String, Int32)"), Is.EqualTo("{'b' : 2,'a' : 1,'b' : 3}"));
    }

    [Test]
    public void FormatSqlText_EmptyKeyValuePairsForAMap_ProducesTheEmptyLiteral()
    {
        Assert.That(Format(Array.Empty<KeyValuePair<string, int>>(), "Map(String, Int32)"), Is.EqualTo("{}"));
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
        // The final argument is a timezone only when it is not the precision.
        var instant = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(Format(instant, "DateTime64(3, 'Europe/Amsterdam')"), Is.EqualTo("2024-01-02 04:04:05.0000000"));
            Assert.Throws<ArgumentException>(() => Format(instant, "DateTime64(3)"));
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

// Retained for future client-side placeholders and used today for Variant matching.
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
