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
/// Server-free tests for the POCO insert box-free write fast path (issue #505). The fast path must
/// produce byte-identical output to the boxed <see cref="ClickHouseType.Write(ExtendedBinaryWriter, object)"/>
/// path, and must only engage on an exact CLR-type match (otherwise it falls back to the boxed path,
/// preserving the coercions that path performs).
/// </summary>
[TestFixture]
public class PocoWriteFastPathTests
{
    private sealed class Box<T>
        where T : class
    {
        // Boxed for reference-type T; the interesting value-type cases use BoxV<T> below.
        public T Value { get; set; }
    }

    private sealed class BoxV<T>
        where T : struct
    {
        public T Value { get; set; }
    }

    // Unconstrained holder used by the Stage 2 parity cases, so it can also carry Nullable<T> properties
    // (which the `where T : struct` constraint on BoxV would reject).
    private sealed class Holder<T>
    {
        public T Value { get; set; }
    }

    private static byte[] WriteBoxed(ClickHouseType type, object value)
    {
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        type.Write(writer, value);
        writer.Flush();
        return stream.ToArray();
    }

    private static byte[] WriteViaDelegate<TRow>(ClickHouseType type, Type propertyType, TRow row)
        where TRow : class
    {
        var prop = typeof(TRow).GetProperty("Value");
        var propInfo = new PocoPropertyInfo
        {
            Property = prop,
            ColumnName = "Value",
            PropertyName = "Value",
            PropertyType = propertyType,
        };
        Func<TRow, object> getter = r => prop.GetValue(r);

        var registry = new PocoTypeRegistry();
        var writers = registry.GetOrBuildWriters<TRow>(new[] { propInfo }, new[] { getter }, new[] { type });

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        writers[0](row, writer);
        writer.Flush();
        return stream.ToArray();
    }

    // Struct value-type parity: property type is exactly the value's CLR type, so the fast path fires.
    private static void AssertValueParity<T>(ClickHouseType type, T value)
        where T : struct
    {
        var expected = WriteBoxed(type, value);
        var actual = WriteViaDelegate(type, typeof(T), new BoxV<T> { Value = value });
        Assert.That(actual, Is.EqualTo(expected));
    }

    private static IEnumerable<TestCaseData> FastPathValueCases()
    {
        yield return Case("Int8", new Int8Type(), (sbyte)-42);
        yield return Case("Int16", new Int16Type(), (short)-12345);
        yield return Case("Int32", new Int32Type(), -1_234_567_890);
        yield return Case("Int64", new Int64Type(), -1_234_567_890_123L);
        yield return Case("UInt8", new UInt8Type(), (byte)200);
        yield return Case("UInt16", new UInt16Type(), (ushort)54321);
        yield return Case("UInt32", new UInt32Type(), 4_000_000_000u);
        yield return Case("UInt64", new UInt64Type(), 18_000_000_000_000_000_000ul);
        yield return Case("Float32", new Float32Type(), 3.14159f);
        yield return Case("Float64", new Float64Type(), 2.718281828459045);
        yield return Case("BoolTrue", new BooleanType(), true);
        yield return Case("BoolFalse", new BooleanType(), false);

        static TestCaseData Case<T>(string name, ClickHouseType type, T value)
            where T : struct
            => new TestCaseData((Action)(() => AssertValueParity(type, value)))
                .SetName($"FastPath_{name}_ProducesSameBytesAsBoxed");
    }

    [TestCaseSource(nameof(FastPathValueCases))]
    public void FastPath_ScalarValue_ProducesSameBytesAsBoxed(Action assertion) => assertion();

    // Stage 2: value types with bespoke serialization (Guid, DateTime family, decimal/ClickHouseDecimal,
    // BigInteger, TimeSpan) and Nullable<T> wrappers. Each asserts the box-free delegate is byte-identical
    // to the boxed Write. Because both paths run the same WriteValue core, equality also proves the
    // interface dispatch / null-marker plumbing is wired correctly.
    private static void AssertParity<T>(ClickHouseType type, T value)
    {
        var expected = WriteBoxed(type, value);
        var actual = WriteViaDelegate(type, typeof(T), new Holder<T> { Value = value });
        Assert.That(actual, Is.EqualTo(expected));
    }

    private static IEnumerable<TestCaseData> Stage2ParityCases()
    {
        var utc = new DateTime(2024, 3, 14, 15, 9, 26, DateTimeKind.Utc);
        var unspecified = new DateTime(2024, 3, 14, 15, 9, 26, DateTimeKind.Unspecified);
        var guid = Guid.Parse("11223344-5566-7788-99aa-bbccddeeff00");

        // Guid / UUID
        yield return Case("Uuid", new UuidType(), guid);

        // DateTime family
        yield return Case("DateTime_Utc", new DateTimeType(), utc);
        yield return Case("DateTime_Unspecified", new DateTimeType(), unspecified);
        yield return Case("DateTime32", new DateTime32Type(), utc);
        yield return Case("DateTime64_Scale3", new DateTime64Type { Scale = 3 }, utc);
        yield return Case("DateTime64_Scale9", new DateTime64Type { Scale = 9 }, utc);
        yield return Case("Date", new DateType(), utc);
        yield return Case("Date32", new Date32Type(), utc);
        yield return Case("Date32_PreEpoch", new Date32Type(), new DateTime(1950, 6, 1, 0, 0, 0, DateTimeKind.Utc));

        // DateTime family via alternate CLR representations (DateTimeOffset, DateOnly)
        var dto = new DateTimeOffset(2024, 3, 14, 15, 9, 26, TimeSpan.FromHours(5));
        yield return Case("DateTime_FromDateTimeOffset", new DateTimeType(), dto);
        yield return Case("DateTime64_FromDateTimeOffset", new DateTime64Type { Scale = 6 }, dto);
        yield return Case("Date_FromDateTimeOffset", new DateType(), dto);
#if NET6_0_OR_GREATER
        var dateOnly = new DateOnly(2024, 3, 14);
        yield return Case("DateTime_FromDateOnly", new DateTimeType(), dateOnly);
        yield return Case("Date_FromDateOnly", new DateType(), dateOnly);
        yield return Case("Date32_FromDateOnly", new Date32Type(), dateOnly);
        yield return Case("Nullable_DateOnly_Value", Nullable(new DateType()), (DateOnly?)dateOnly);
#endif
        yield return Case("Nullable_DateTimeOffset_Value", Nullable(new DateTimeType()), (DateTimeOffset?)dto);

        // Decimal (decimal) and ClickHouseDecimal
        yield return Case("Decimal64", new Decimal64Type { Precision = 18, Scale = 4 }, 123456.7891m);
        yield return Case("Decimal128_Negative", new Decimal128Type { Precision = 38, Scale = 10 }, -98765.4321m);
        yield return Case("Decimal_ClickHouseDecimal", new Decimal128Type { Precision = 38, Scale = 6 }, (ClickHouseDecimal)42.5m);

        // Big integers
        yield return Case("Int128", new Int128Type(), new BigInteger(123456789012345L));
        yield return Case("Int256_Negative", new Int256Type(), new BigInteger(-987654321098765L));
        yield return Case("UInt128", new UInt128Type(), new BigInteger(ulong.MaxValue));
        yield return Case("UInt256", new UInt256Type(), new BigInteger(ulong.MaxValue));

        // Time / Time64 (TimeSpan)
        yield return Case("Time", new TimeType(), new TimeSpan(1, 30, 45));
        yield return Case("Time64_Scale3", new Time64Type { Scale = 3 }, new TimeSpan(0, 2, 15, 30, 500));

        // Nullable<T> — non-null (recurses into the underlying fast path)
        yield return Case("Nullable_Int64_Value", Nullable(new Int64Type()), (long?)-1234567890123L);
        yield return Case("Nullable_Int32_Value", Nullable(new Int32Type()), (int?)42);
        yield return Case("Nullable_Float64_Value", Nullable(new Float64Type()), (double?)3.14159);
        yield return Case("Nullable_Bool_Value", Nullable(new BooleanType()), (bool?)true);
        yield return Case("Nullable_Uuid_Value", Nullable(new UuidType()), (Guid?)guid);
        yield return Case("Nullable_DateTime_Value", Nullable(new DateTimeType()), (DateTime?)utc);
        yield return Case("Nullable_Decimal_Value", Nullable(new Decimal64Type { Precision = 18, Scale = 4 }), (decimal?)12.34m);

        // Nullable<T> — null (writes the 1-byte null marker only)
        yield return Case("Nullable_Int64_Null", Nullable(new Int64Type()), (long?)null);
        yield return Case("Nullable_Uuid_Null", Nullable(new UuidType()), (Guid?)null);
        yield return Case("Nullable_DateTime_Null", Nullable(new DateTimeType()), (DateTime?)null);

        static NullableType Nullable(ClickHouseType underlying) => new() { UnderlyingType = underlying };

        static TestCaseData Case<T>(string name, ClickHouseType type, T value)
            => new TestCaseData((Action)(() => AssertParity(type, value)))
                .SetName($"Stage2_{name}_ProducesSameBytesAsBoxed");
    }

    [TestCaseSource(nameof(Stage2ParityCases))]
    public void Stage2_ValueType_ProducesSameBytesAsBoxed(Action assertion) => assertion();

    private enum ByteEnum : byte { A = 1, B = 200 }

    private enum IntEnum { X = 5, Y = -7 }

    // Generous numeric coercion: a value-type property that isn't the column's exact framework type
    // (widening/narrowing/differing signedness/enums) must still be byte-identical to the boxed
    // Convert.ToXxx path.
    private static IEnumerable<TestCaseData> NumericCoercionCases()
    {
        yield return Case("Int_To_Int64", new Int64Type(), 1_234_567_890);
        yield return Case("Long_To_Int32", new Int32Type(), 123_456L);
        yield return Case("Byte_To_Int32", new Int32Type(), (byte)200);
        yield return Case("Short_To_UInt32", new UInt32Type(), (short)1000);
        yield return Case("UInt_To_Int64", new Int64Type(), 4_000_000_000u);
        yield return Case("Double_To_Float32", new Float32Type(), 3.14159265358979);
        yield return Case("Float_To_Float64", new Float64Type(), 3.14159f);
        yield return Case("Int_To_Float64", new Float64Type(), 42);
        yield return Case("Decimal_To_Int64", new Int64Type(), 42m);
        yield return Case("Double_To_Int32_Rounds", new Int32Type(), 2.5);
        yield return Case("Bool_To_Int32", new Int32Type(), true);
        yield return Case("Int_To_Decimal", new Decimal64Type { Precision = 18, Scale = 4 }, 12345);
        yield return Case("Double_To_Decimal", new Decimal128Type { Precision = 38, Scale = 6 }, 3.5);
        yield return Case("IntEnum_To_Int32", new Int32Type(), IntEnum.Y);
        yield return Case("ByteEnum_To_Int16", new Int16Type(), ByteEnum.B);
        yield return Case("ByteEnum_To_UInt8", new UInt8Type(), ByteEnum.B);

        static TestCaseData Case<T>(string name, ClickHouseType type, T value)
            => new TestCaseData((Action)(() => AssertParity(type, value)))
                .SetName($"NumericCoercion_{name}_ProducesSameBytesAsBoxed");
    }

    [TestCaseSource(nameof(NumericCoercionCases))]
    public void NumericCoercion_ValueType_ProducesSameBytesAsBoxed(Action assertion) => assertion();

    [Test]
    public void NumericCoercion_ThrowingConversion_ThrowsSameAsBoxed()
    {
        // DateTime → Int64: Convert.ToInt64(DateTime) exists but throws. The fast path emits that call, so it
        // must throw the same InvalidCastException the boxed path throws — parity holds even here.
        var type = new Int64Type();
        var dt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var boxedEx = Assert.Throws<InvalidCastException>(() => WriteBoxed(type, dt));
        var fastEx = Assert.Throws<InvalidCastException>(
            () => WriteViaDelegate(type, typeof(DateTime), new Holder<DateTime> { Value = dt }));

        Assert.That(fastEx.Message, Is.EqualTo(boxedEx.Message));
    }

    [Test]
    public void TryBuildWriteBody_NonConvertibleValueTypeOnIntColumn_ReturnsNull()
    {
        // A Guid property on an Int64 column: Guid is not IConvertible and has no Convert.ToInt64 overload,
        // so the fast path must decline (→ boxed fallback, which throws InvalidCastException exactly as
        // before). (Contrast DateTime, which *does* have a throwing Convert.ToInt64 overload, so it is
        // "fast-pathed" into an identical throw — still byte/exception-parity-correct.)
        var writer = Expression.Parameter(typeof(ExtendedBinaryWriter), "w");
        var value = Expression.Parameter(typeof(Guid), "v");

        var body = PocoWriteExpressionFactory.TryBuildWriteBody(new Int64Type(), value, writer);

        Assert.That(body, Is.Null);
    }

    [Test]
    public void TryBuildWriteBody_NonConvertibleValueTypeOnDecimalColumn_ReturnsNull()
    {
        // A Guid property on a Decimal column: no Convert.ToDecimal(Guid) overload, so the fast path declines
        // and the boxed path handles it (throwing exactly as before).
        var writer = Expression.Parameter(typeof(ExtendedBinaryWriter), "w");
        var value = Expression.Parameter(typeof(Guid), "v");

        var body = PocoWriteExpressionFactory.TryBuildWriteBody(
            new Decimal64Type { Precision = 18, Scale = 4 }, value, writer);

        Assert.That(body, Is.Null);
    }

    [Test]
    public void TryBuildWriteBody_ReferenceTypeOnIntColumn_ReturnsNull()
    {
        // A string property on an Int64 column: reference type (no box to remove) and single-arg
        // Convert.ToInt64(string) uses the current culture, so it must stay on the boxed path.
        var writer = Expression.Parameter(typeof(ExtendedBinaryWriter), "w");
        var value = Expression.Parameter(typeof(string), "v");

        var body = PocoWriteExpressionFactory.TryBuildWriteBody(new Int64Type(), value, writer);

        Assert.That(body, Is.Null);
    }

    [Test]
    public void FastPath_String_ProducesSameBytesAsBoxed()
    {
        var type = new StringType();
        const string value = "hello ünïcode 🚀";

        var expected = WriteBoxed(type, value);
        var actual = WriteViaDelegate(type, typeof(string), new Box<string> { Value = value });

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public void FastPath_NullString_ThrowsSameAsBoxed()
    {
        var type = new StringType();

        var boxedEx = Assert.Throws<ArgumentException>(() => WriteBoxed(type, null));
        var fastEx = Assert.Throws<ArgumentException>(
            () => WriteViaDelegate(type, typeof(string), new Box<string> { Value = null }));

        Assert.That(fastEx.Message, Is.EqualTo(boxedEx.Message));
    }

    [Test]
    public void FastPath_TypeMismatch_FallsBackAndMatchesBoxedCoercion()
    {
        // A short property mapped to an Int64 column: no fast path, so the delegate must fall back to
        // the boxed Write (which widens via Convert.ToInt64) and stay byte-identical.
        var type = new Int64Type();

        var expected = WriteBoxed(type, (short)12345);
        var actual = WriteViaDelegate(type, typeof(short), new BoxV<short> { Value = 12345 });

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public void TryBuildWriteBody_ExactMatch_ReturnsExpression()
    {
        var writer = Expression.Parameter(typeof(ExtendedBinaryWriter), "w");
        var value = Expression.Parameter(typeof(long), "v");

        var body = PocoWriteExpressionFactory.TryBuildWriteBody(new Int64Type(), value, writer);

        Assert.That(body, Is.Not.Null);
    }

    [TestCase(typeof(short))]   // narrower than Int64
    [TestCase(typeof(int))]     // narrower than Int64
    [TestCase(typeof(ulong))]   // different signedness
    public void TryBuildWriteBody_NumericMismatch_FastPathsViaCoercion(Type clrType)
    {
        // A numeric value type that isn't the exact framework type is now de-boxed via box-free Convert.ToXxx
        // (byte-parity for these is asserted by NumericCoercionCases); it no longer falls back to boxed.
        var writer = Expression.Parameter(typeof(ExtendedBinaryWriter), "w");
        var value = Expression.Parameter(clrType, "v");

        var body = PocoWriteExpressionFactory.TryBuildWriteBody(new Int64Type(), value, writer);

        Assert.That(body, Is.Not.Null);
    }

    [Test]
    public void TryBuildWriteBody_NullableOfFastPathUnderlying_ReturnsExpression()
    {
        // Stage 2: Nullable(T) with a Nullable<U> property whose underlying U has a fast path is itself
        // de-boxed (the underlying recursion succeeds).
        var nullableInt64 = TypeConverter.ParseClickHouseType("Nullable(Int64)", TypeSettings.Default);
        var writer = Expression.Parameter(typeof(ExtendedBinaryWriter), "w");
        var value = Expression.Parameter(typeof(long?), "v");

        var body = PocoWriteExpressionFactory.TryBuildWriteBody(nullableInt64, value, writer);

        Assert.That(body, Is.Not.Null);
    }

    [Test]
    public void TryBuildWriteBody_NullableUnderlyingMismatch_FastPathsViaCoercion()
    {
        // Nullable(Int64) with an int? property: the underlying recursion now succeeds via numeric coercion
        // (int → Int64), so the whole Nullable is de-boxed. (Byte-parity is covered by Stage2ParityCases /
        // NumericCoercionCases running through the same cores.)
        var nullableInt64 = TypeConverter.ParseClickHouseType("Nullable(Int64)", TypeSettings.Default);
        var writer = Expression.Parameter(typeof(ExtendedBinaryWriter), "w");
        var value = Expression.Parameter(typeof(int?), "v");

        var body = PocoWriteExpressionFactory.TryBuildWriteBody(nullableInt64, value, writer);

        Assert.That(body, Is.Not.Null);
    }

    [Test]
    public void TryBuildWriteBody_NullableUnderlyingNoFastPath_ReturnsNull()
    {
        // Nullable(UUID) with an int? property: the underlying recursion (int vs UUID/Guid) has no fast path
        // — UuidType is neither ITypedWriter<int> nor a numeric-coercion target — so the whole Nullable
        // falls back to the boxed path.
        var nullableUuid = TypeConverter.ParseClickHouseType("Nullable(UUID)", TypeSettings.Default);
        var writer = Expression.Parameter(typeof(ExtendedBinaryWriter), "w");
        var value = Expression.Parameter(typeof(int?), "v");

        var body = PocoWriteExpressionFactory.TryBuildWriteBody(nullableUuid, value, writer);

        Assert.That(body, Is.Null);
    }

    [Test]
    public void TryBuildWriteBody_ArrayColumn_ReturnsNull()
    {
        // Composite types (here Array) hold reference-type values that the getter never boxes, so there
        // is nothing to de-box; they must fall back to the boxed path.
        var arrayInt32 = TypeConverter.ParseClickHouseType("Array(Int32)", TypeSettings.Default);
        var writer = Expression.Parameter(typeof(ExtendedBinaryWriter), "w");
        var value = Expression.Parameter(typeof(int[]), "v");

        var body = PocoWriteExpressionFactory.TryBuildWriteBody(arrayInt32, value, writer);

        Assert.That(body, Is.Null);
    }

    [Test]
    public void TryBuildWriteBody_EnumColumn_ReturnsNull()
    {
        // Enum's FrameworkType is string (a reference type, never boxed by the getter) and it has no
        // ITypedWriter, so it falls back to the boxed path.
        var writer = Expression.Parameter(typeof(ExtendedBinaryWriter), "w");
        var value = Expression.Parameter(typeof(string), "v");

        var body = PocoWriteExpressionFactory.TryBuildWriteBody(new Enum8Type(), value, writer);

        Assert.That(body, Is.Null);
    }
}
