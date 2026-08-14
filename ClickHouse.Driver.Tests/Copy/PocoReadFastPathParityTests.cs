using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Numerics;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Poco;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Tests.Copy;

/// <summary>
/// Server-free tests for the box-free POCO read fast path (#509), covering the
/// (column type x target CLR type) dispatch matrix of <see cref="PocoReadExpressionFactory"/>.
///
/// <para>Line coverage cannot see this matrix: <c>TryBuildTypedRead</c> is a single
/// <c>Expression.Call</c> that one scalar test marks as covered, while most of the
/// <see cref="ITypedReader{T}"/> implementations it can dispatch to are never reached.</para>
///
/// <para>Split by what is not already covered elsewhere:</para>
/// <list type="bullet">
/// <item><b>Dispatch</b> — that a pair resolves to a typed read at all. Novel: no other test asserts it.
/// For most types the typed reader and the boxed <c>Read</c> are the same method
/// (e.g. <c>Int8Type.Read =&gt; ReadValue</c>), so their <i>decoding</i> is already round-tripped by
/// <c>SerialisationTests</c> and read off a real server by <c>SqlSimpleSelectTests</c> via
/// <c>TestCases.GetDataTypeSamples()</c>. Only the dispatch is asserted here.</item>
/// <item><b>Decode</b> — full value + alignment, restricted to representations the boxed path cannot
/// produce (byte[], DateOnly/DateTimeOffset, native Int128/UInt128, ClickHouseDecimal, Map-as-pairs,
/// the Nullable lift). Nothing else decodes these, so they are checked end to end.</item>
/// <item><b>Negative</b> — pairs that must decline and fall back to the boxed reader.</item>
/// </list>
///
/// <para>Decode cases read a payload produced by the type's own
/// <see cref="ClickHouseType.Write(ExtendedBinaryWriter, object)"/> and then read a trailing sentinel from
/// the same reader, so a typed reader that consumes the wrong number of bytes fails here rather than
/// silently corrupting the next column of a real row.</para>
/// </summary>
[TestFixture]
public class PocoReadFastPathParityTests
{
    private static readonly ParameterExpression ReaderParam =
        Expression.Parameter(typeof(ExtendedBinaryReader), "reader");

    // Follows the value under test on the wire; a misaligned typed read decodes garbage here.
    private const long Sentinel = 0x1234_5678_09AB_CDEFL;

    private const string Enum8Def = "Enum8('a' = -5, 'b' = 7)";
    private const string Enum16Def = "Enum16('a' = -300, 'b' = 4000)";

    private static ClickHouseType Parse(string typeName, TypeSettings? settings = null)
        => TypeConverter.ParseClickHouseType(typeName, settings ?? TypeSettings.Default);

    private static Expression TryBuild(ClickHouseType type, Type target)
        => PocoReadExpressionFactory.TryBuildReadBody(type, ReaderParam, target);

    // ---- Dispatch matrix: the pair resolves to a typed read of exactly the target type ----
    //
    // Guards against a type silently losing an ITypedReader<T> and falling back to the boxed path: the
    // values would still be correct (same decoder), so only an assertion on dispatch can catch it.

    [TestCase("Int8", typeof(sbyte))]
    [TestCase("Int16", typeof(short))]
    [TestCase("Int32", typeof(int))]
    [TestCase("Int64", typeof(long))]
    [TestCase("UInt8", typeof(byte))]
    [TestCase("UInt16", typeof(ushort))]
    [TestCase("UInt32", typeof(uint))]
    [TestCase("UInt64", typeof(ulong))]
    [TestCase("Float32", typeof(float))]
    [TestCase("Float64", typeof(double))]
    [TestCase("BFloat16", typeof(float))]
    [TestCase("Bool", typeof(bool))]
    [TestCase("String", typeof(string))]
    [TestCase("String", typeof(byte[]))]
    [TestCase("FixedString(5)", typeof(string))]
    [TestCase("FixedString(5)", typeof(byte[]))]
    [TestCase("UUID", typeof(Guid))]
    [TestCase("IPv4", typeof(System.Net.IPAddress))]
    [TestCase("IPv6", typeof(System.Net.IPAddress))]
    [TestCase("Time", typeof(TimeSpan))]
    [TestCase("Time64(3)", typeof(TimeSpan))]
    [TestCase("Decimal(10, 2)", typeof(decimal))]
    [TestCase("Decimal(10, 2)", typeof(ClickHouseDecimal))]
    [TestCase("Int128", typeof(BigInteger))]
    [TestCase("UInt128", typeof(BigInteger))]
    [TestCase("Int256", typeof(BigInteger))]
    [TestCase("UInt256", typeof(BigInteger))]
    [TestCase(Enum8Def, typeof(string))]
    [TestCase(Enum8Def, typeof(int))]
    [TestCase(Enum16Def, typeof(string))]
    [TestCase(Enum16Def, typeof(int))]
    [TestCase("LowCardinality(String)", typeof(string))]
#if NET8_0_OR_GREATER
    [TestCase("Int128", typeof(Int128))]
    [TestCase("UInt128", typeof(UInt128))]
#endif
    public void TryBuildReadBody_SupportedPair_ReturnsExpressionOfTargetType(string typeName, Type target)
    {
        var body = TryBuild(Parse(typeName), target);
        Assert.That(body, Is.Not.Null, $"expected a fast path for {typeName} -> {target}");
        Assert.That(body.Type, Is.EqualTo(target));
    }

    // Every date/time column offers all three representations.
    [TestCase("Date")]
    [TestCase("Date32")]
    [TestCase("DateTime('UTC')")]
    [TestCase("DateTime64(3, 'UTC')")]
    public void TryBuildReadBody_DateTimeColumn_OffersAllThreeRepresentations(string typeName)
    {
        var type = Parse(typeName);
        Assert.Multiple(() =>
        {
            Assert.That(TryBuild(type, typeof(DateTime))?.Type, Is.EqualTo(typeof(DateTime)));
            Assert.That(TryBuild(type, typeof(DateTimeOffset))?.Type, Is.EqualTo(typeof(DateTimeOffset)));
            Assert.That(TryBuild(type, typeof(DateOnly))?.Type, Is.EqualTo(typeof(DateOnly)));
        });
    }

    // ---- Decode: representations the boxed path cannot produce, so nothing else covers them ----

    private static byte[] WriteBoxed(ClickHouseType type, object value)
    {
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        type.Write(writer, value);
        new Int64Type().Write(writer, Sentinel);
        writer.Flush();
        return stream.ToArray();
    }

    private static T ReadTyped<T>(ClickHouseType type, byte[] payload)
    {
        var body = TryBuild(type, typeof(T));
        Assert.That(body, Is.Not.Null, $"expected a fast path for {type} -> {typeof(T)}");
        Assert.That(body.Type, Is.EqualTo(typeof(T)), "fast path expression must have exactly the target type");

        var read = Expression.Lambda<Func<ExtendedBinaryReader, T>>(body, ReaderParam).Compile();

        using var stream = new MemoryStream(payload);
        using var reader = new ExtendedBinaryReader(stream);
        var value = read(reader);
        Assert.That(reader.ReadInt64(), Is.EqualTo(Sentinel),
            $"typed read of {type} -> {typeof(T)} consumed the wrong number of bytes");
        return value;
    }

    private static T RoundTrip<T>(string typeName, object writeValue, TypeSettings? settings = null)
    {
        var type = Parse(typeName, settings);
        return ReadTyped<T>(type, WriteBoxed(type, writeValue));
    }

    private static void AssertTypedRead<T>(string typeName, object writeValue, T expected, TypeSettings? settings = null)
        => Assert.That(RoundTrip<T>(typeName, writeValue, settings), Is.EqualTo(expected));

    private static TestCaseData Case<T>(string name, string typeName, object writeValue, T expected)
        => new TestCaseData((Action)(() => AssertTypedRead(typeName, writeValue, expected)))
            .SetName($"FastPathRead_{name}_DecodesValueAndStaysAligned");

    // String/FixedString offer both representations regardless of the client's ReadStringsAsByteArrays
    // setting, which steers only the boxed Read. Asserting under both settings pins that independence.
    private static IEnumerable<TestCaseData> StringCases()
    {
        var utf8 = new byte[] { 0xC3, 0xA9, 0x21 }; // "é!"
        var fixedBytes = new byte[] { 1, 2, 3, 4, 5 };

        foreach (var asBytes in new[] { false, true })
        {
            var settings = TypeSettings.Default with { readStringsAsByteArrays = asBytes };
            var suffix = asBytes ? "ReadAsByteArray" : "ReadAsString";

            yield return StringCase($"String_ToString_{suffix}", "String", "é!", "é!", settings);
            yield return StringCase($"String_ToByteArray_{suffix}", "String", "é!", utf8, settings);
            yield return StringCase($"FixedString_ToString_{suffix}", "FixedString(5)", "abcde", "abcde", settings);
            yield return StringCase($"FixedString_ToByteArray_{suffix}", "FixedString(5)", fixedBytes, fixedBytes, settings);
        }

        static TestCaseData StringCase<T>(string name, string typeName, object writeValue, T expected, TypeSettings settings)
            => new TestCaseData((Action)(() => AssertTypedRead(typeName, writeValue, expected, settings)))
                .SetName($"FastPathRead_{name}_DecodesValueAndStaysAligned");
    }

    [TestCaseSource(nameof(StringCases))]
    public void FastPathRead_StringFamily_DecodesValueAndStaysAligned(Action assertion) => assertion();

#if NET8_0_OR_GREATER
    // The native Int128/UInt128 readers are the only typed readers with a decoder wholly independent of the
    // boxed path (BinaryPrimitives vs. the BigInteger path), so these are the differential test between the
    // two. The all-0xFF extremes are byte-order palindromes, hence the asymmetric 0x0102...0F10 values.
    private static IEnumerable<TestCaseData> NativeWideIntegerCases()
    {
        var asymmetric = BigInteger.Parse("1339673755198158349044581307228491536");

        yield return Case("Int128_ToNativeInt128_Max", "Int128",
            BigInteger.Parse("170141183460469231731687303715884105727"), Int128.MaxValue);
        yield return Case("Int128_ToNativeInt128_Asymmetric", "Int128", asymmetric,
            new Int128(0x0102030405060708UL, 0x090A0B0C0D0E0F10UL));
        yield return Case("UInt128_ToNativeUInt128_Asymmetric", "UInt128", asymmetric,
            new UInt128(0x0102030405060708UL, 0x090A0B0C0D0E0F10UL));
    }

    [TestCaseSource(nameof(NativeWideIntegerCases))]
    public void FastPathRead_NativeWideInteger_DecodesValueAndStaysAligned(Action assertion) => assertion();
#endif

    // ClickHouseDecimal carries the column's scale, which is the whole reason to prefer it over decimal.
    // ClickHouseDecimal.Equals rescales before comparing, so the value alone would not pin it.
    [TestCase("Decimal(10, 2)", "123.45", 2)]
    [TestCase("Decimal(38, 8)", "12345.67890000", 8)] // size 16 -> the BigInteger mantissa branch
    public void FastPathRead_DecimalToClickHouseDecimal_PreservesColumnScale(
        string typeName, string literal, int expectedScale)
    {
        var written = decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture);

        var value = RoundTrip<ClickHouseDecimal>(typeName, written);

        Assert.That((decimal)value, Is.EqualTo(written));
        Assert.That(value.Scale, Is.EqualTo(expectedScale));
    }

    // ---- Date/time: the representations and the DateTimeKind contract ----

    // Kind is load-bearing for consumers calling ToUniversalTime(), and DateTime.Equals ignores it, so it
    // needs its own assertion. A UTC (or offset-0) column yields Utc; a column with no timezone, or one
    // whose offset is non-zero, yields Unspecified to preserve wall-clock time.
    [TestCase("Date", DateTimeKind.Utc)]
    [TestCase("Date32", DateTimeKind.Utc)]
    [TestCase("DateTime('UTC')", DateTimeKind.Utc)]
    [TestCase("DateTime64(3, 'UTC')", DateTimeKind.Utc)]
    [TestCase("DateTime", DateTimeKind.Unspecified)]
    [TestCase("DateTime64(3)", DateTimeKind.Unspecified)]
    [TestCase("DateTime('America/New_York')", DateTimeKind.Unspecified)]
    public void FastPathRead_DateTimeColumn_ReadsExpectedDateTimeKind(string typeName, DateTimeKind expectedKind)
    {
        var written = new DateTime(2021, 6, 15, 12, 30, 0, DateTimeKind.Utc);

        Assert.That(RoundTrip<DateTime>(typeName, written).Kind, Is.EqualTo(expectedKind));
    }

    // DateOnly and DateTimeOffset are fast-path-only. For a zoned column the offset comes from the source
    // instant (DateTimeType/DateTime64Type override); the date-only columns use the offset-0 base default.
    [TestCase("Date", 0)]
    [TestCase("Date32", 0)]
    [TestCase("DateTime('UTC')", 0)]
    [TestCase("DateTime64(3, 'UTC')", 0)]
    [TestCase("DateTime('America/New_York')", -4)] // EDT on this date
    public void FastPathRead_DateTimeColumn_ReadsAsDateTimeOffsetWithColumnOffset(string typeName, int expectedOffsetHours)
    {
        // Midnight UTC so the value is unambiguous for the date-only columns too.
        var written = new DateTime(2021, 6, 15, 0, 0, 0, DateTimeKind.Utc);

        var value = RoundTrip<DateTimeOffset>(typeName, written);

        Assert.That(value.UtcDateTime, Is.EqualTo(written));
        Assert.That(value.Offset, Is.EqualTo(TimeSpan.FromHours(expectedOffsetHours)));
    }

    [TestCase("Date")]
    [TestCase("Date32")]
    [TestCase("DateTime('UTC')")]
    [TestCase("DateTime64(3, 'UTC')")]
    public void FastPathRead_DateTimeColumn_ReadsAsDateOnly(string typeName)
    {
        var written = new DateTime(2021, 6, 15, 12, 30, 0, DateTimeKind.Utc);

        Assert.That(RoundTrip<DateOnly>(typeName, written), Is.EqualTo(new DateOnly(2021, 6, 15)));
    }

    // ---- Wrappers: the Nullable lift and its null branch ----

    private static IEnumerable<TestCaseData> WrapperCases()
    {
        // Reference target: the inner expression is used as-is, with no Nullable<> lift.
        yield return Case("NullableString_Value", "Nullable(String)", "abc", "abc");
        yield return Case("NullableString_Null", "Nullable(String)", null, (string)null);
        yield return Case("NullableByteArray_Null", "Nullable(String)", null, (byte[])null);
        yield return Case("LowCardinalityNullableString_Value", "LowCardinality(Nullable(String))", "abc", "abc");
        yield return Case("LowCardinalityNullableString_Null", "LowCardinality(Nullable(String))", null, (string)null);
        // Value target: Expression.Default(Nullable<U>) on the null branch, Convert on the value branch.
        yield return Case("NullableUuid_Null", "Nullable(UUID)", null, (Guid?)null);
        yield return Case("NullableDateTime64_ToDateTimeOffset_Null", "Nullable(DateTime64(3, 'UTC'))",
            null, (DateTimeOffset?)null);
        // Non-nullable column, Nullable<U> property: read the underlying and wrap.
        yield return Case("NonNullableDate_ToNullableDateOnly", "Date",
            new DateTime(2023, 3, 14, 0, 0, 0, DateTimeKind.Utc), (DateOnly?)new DateOnly(2023, 3, 14));
    }

    [TestCaseSource(nameof(WrapperCases))]
    public void FastPathRead_Wrapper_DecodesValueAndStaysAligned(Action assertion) => assertion();

    // ---- Map -> ordered KeyValuePair sequence (order/round-trip is covered by PocoMapTupleTests) ----

    private static IEnumerable<TestCaseData> MapCases()
    {
        yield return Case("EmptyMap_ToList", "Map(String, Int32)",
            new Dictionary<string, int>(), new List<KeyValuePair<string, int>>());
        yield return Case("EmptyMap_ToArray", "Map(String, Int32)",
            new Dictionary<string, int>(), Array.Empty<KeyValuePair<string, int>>());
        // A null map value is surfaced as default(TValue), matching the boxed Read's ClearDBNull.
        yield return Case("MapWithNullValue_ToList", "Map(String, Nullable(Int32))",
            new Dictionary<string, int?> { ["a"] = null, ["b"] = 2 },
            new List<KeyValuePair<string, int?>>
            {
                new("a", null),
                new("b", 2),
            });
    }

    [TestCaseSource(nameof(MapCases))]
    public void FastPathRead_Map_DecodesValueAndStaysAligned(Action assertion) => assertion();

    // ---- Negative matrix: pairs that must decline the fast path and fall back to the boxed reader ----

    [Test]
    public void TryBuildReadBody_NullableColumnWithNonNullableTarget_ReturnsNull()
        => AssertNoFastPath("Nullable(Int64)", typeof(long));

    // Nullable(Int64) -> int?: the underlying has no ITypedReader<int>, so the nullable wrapper declines too.
    [Test]
    public void TryBuildReadBody_NullableColumnWithMismatchedUnderlyingTarget_ReturnsNull()
        => AssertNoFastPath("Nullable(Int64)", typeof(int?));

    [Test]
    public void TryBuildReadBody_NonNullableColumnWithMismatchedNullableTarget_ReturnsNull()
        => AssertNoFastPath("Int64", typeof(int?));

    // Map declines anything that is not exactly List<KVP<K,V>>/KVP<K,V>[] with matching K and V, so the
    // boxed Dictionary path stays in charge rather than silently coercing elements.
    [TestCase("Map(String, Int32)", typeof(Dictionary<string, int>), TestName = "TryBuildReadBody_MapToDictionary_ReturnsNull")]
    [TestCase("Map(String, Int32)", typeof(IList<KeyValuePair<string, int>>), TestName = "TryBuildReadBody_MapToPairInterface_ReturnsNull")]
    [TestCase("Map(String, Int32)", typeof(List<string>), TestName = "TryBuildReadBody_MapToNonPairList_ReturnsNull")]
    [TestCase("Map(String, Int32)", typeof(string[]), TestName = "TryBuildReadBody_MapToNonPairArray_ReturnsNull")]
    [TestCase("Map(String, Int32)", typeof(List<KeyValuePair<string, long>>), TestName = "TryBuildReadBody_MapToMismatchedValueType_ReturnsNull")]
    [TestCase("Map(String, Int32)", typeof(KeyValuePair<int, int>[]), TestName = "TryBuildReadBody_MapToMismatchedKeyType_ReturnsNull")]
    [TestCase("Map(String, Nullable(Int32))", typeof(List<KeyValuePair<string, int>>), TestName = "TryBuildReadBody_NullableMapValueToNonNullablePair_ReturnsNull")]
    public void TryBuildReadBody_UnsupportedMapTarget_ReturnsNull(string typeName, Type target)
        => AssertNoFastPath(typeName, target);

    // Dispatch is invariant in T: no widening, and no representation the type did not opt into.
    [TestCase("Int32", typeof(long), TestName = "TryBuildReadBody_IntegerWideningTarget_ReturnsNull")]
    [TestCase("Float32", typeof(double), TestName = "TryBuildReadBody_FloatWideningTarget_ReturnsNull")]
    [TestCase("UUID", typeof(string), TestName = "TryBuildReadBody_UuidToStringTarget_ReturnsNull")]
    [TestCase("Enum8('a' = -5)", typeof(sbyte), TestName = "TryBuildReadBody_EnumToUnofferedSByteTarget_ReturnsNull")]
    [TestCase("Int128", typeof(long), TestName = "TryBuildReadBody_Int128ToNarrowerTarget_ReturnsNull")]
    [TestCase("Time", typeof(TimeOnly), TestName = "TryBuildReadBody_TimeToTimeOnlyTarget_ReturnsNull")]
    public void TryBuildReadBody_MismatchedTargetType_ReturnsNull(string typeName, Type target)
        => AssertNoFastPath(typeName, target);

    // Composite and polymorphic columns have no typed reader at all and always fall back.
    [TestCase("Array(Int64)", typeof(long[]), TestName = "TryBuildReadBody_ArrayColumn_ReturnsNull")]
    [TestCase("Tuple(Int64, String)", typeof(object[]), TestName = "TryBuildReadBody_TupleColumn_ReturnsNull")]
    [TestCase("Variant(Int64, String)", typeof(object), TestName = "TryBuildReadBody_VariantColumn_ReturnsNull")]
    [TestCase("Nullable(Array(Int64))", typeof(long[]), TestName = "TryBuildReadBody_NullableArrayColumn_ReturnsNull")]
    public void TryBuildReadBody_CompositeColumn_ReturnsNull(string typeName, Type target)
        => AssertNoFastPath(typeName, target);

    // SimpleAggregateFunction is wire-transparent like LowCardinality, but the factory does not unwrap it,
    // so it currently falls back to the boxed path. Pins today's behaviour; see the note in the PR.
    [Test]
    public void TryBuildReadBody_SimpleAggregateFunctionColumn_ReturnsNull()
        => AssertNoFastPath("SimpleAggregateFunction(sum, UInt64)", typeof(ulong));

    private static void AssertNoFastPath(string typeName, Type targetClrType)
    {
        var type = Parse(typeName);
        Assert.That(TryBuild(type, targetClrType), Is.Null,
            $"expected no fast path for {type} -> {targetClrType}");
    }
}
