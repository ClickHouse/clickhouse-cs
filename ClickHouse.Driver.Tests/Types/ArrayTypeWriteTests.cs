using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Reflection;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// Pins <c>ArrayType.Write</c>'s box-free element path against the boxed one it replaces. The two must emit
/// identical bytes, so every case writes the same values twice — once as the <c>T[]</c> that takes the typed
/// path, once as a <c>List&lt;T&gt;</c>, which is not an <see cref="Array"/> and so keeps the boxed
/// <see cref="IList"/> loop.
/// </summary>
[TestFixture]
public class ArrayTypeWriteTests
{
    public static IEnumerable<TestCaseData> ParityCases()
    {
        // Element CLR type == the column's own type: the typed path writes these.
        yield return Case("Array(Int8)", new sbyte[] { -128, 0, 127 });
        yield return Case("Array(Int16)", new short[] { -32768, 0, 32767 });
        yield return Case("Array(Int32)", new[] { int.MinValue, 0, int.MaxValue });
        yield return Case("Array(Int64)", new[] { long.MinValue, 0L, long.MaxValue });
        yield return Case("Array(UInt8)", new byte[] { 0, 128, 255 });
        yield return Case("Array(UInt16)", new ushort[] { 0, 1, 65535 });
        yield return Case("Array(UInt32)", new[] { 0u, 1u, uint.MaxValue });
        yield return Case("Array(UInt64)", new[] { 0ul, 1ul, ulong.MaxValue });
        yield return Case("Array(Float32)", new[] { float.MinValue, 0f, float.MaxValue });
        yield return Case("Array(Float64)", new[] { double.MinValue, 0d, double.MaxValue });
        yield return Case("Array(Bool)", new[] { true, false, true });
        yield return Case("Array(UUID)", new[] { Guid.Empty, new Guid("2ee6b16f-1b03-4b1e-a1a5-99f7ae6a1c2c") });
        yield return Case("Array(Decimal(9, 2))", new[] { -1.23m, 0m, 4.50m });
        yield return Case("Array(Decimal(9, 2))", new[] { new ClickHouseDecimal(-1.23m), new ClickHouseDecimal(4.50m) });
        yield return Case("Array(Int128)", new[] { BigInteger.MinusOne, BigInteger.Zero, new BigInteger(ulong.MaxValue) });

        // The only path through the unsigned branch of the big-integer writer, where a mantissa that fills
        // the width is padded differently than a signed one.
        yield return Case("Array(UInt128)", new[] { BigInteger.Zero, new BigInteger(ulong.MaxValue) });
        yield return Case("Array(Time)", new[] { TimeSpan.Zero, TimeSpan.FromSeconds(3661) });
        yield return Case("Array(Time64(3))", new[] { TimeSpan.Zero, TimeSpan.FromMilliseconds(3661123) });
        yield return Case("Array(BFloat16)", new[] { 1.5f, -2.5f });

        // The DateTime family: the coercion to the column's timezone depends on the value's Kind, so each
        // Kind is written through both paths rather than only the one the fast path happens to take.
        yield return Case("Array(DateTime('Europe/Amsterdam'))", new[]
        {
            new DateTime(2024, 3, 31, 1, 30, 0, DateTimeKind.Unspecified),
            new DateTime(2024, 3, 31, 1, 30, 0, DateTimeKind.Utc),
            new DateTime(2024, 6, 1, 12, 0, 0, DateTimeKind.Local),
            new DateTime(1970, 1, 1, 12, 0, 0, DateTimeKind.Unspecified),
        });
        yield return Case("Array(DateTime64(3, 'Europe/Amsterdam'))", new[]
        {
            new DateTime(2024, 3, 31, 1, 30, 0, 123, DateTimeKind.Unspecified),
            new DateTime(2024, 3, 31, 1, 30, 0, 123, DateTimeKind.Utc),
        });
        yield return Case("Array(DateTime('Europe/Amsterdam'))", new[]
        {
            new DateTimeOffset(2024, 3, 31, 1, 30, 0, TimeSpan.FromHours(2)),
            new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero),
        });
        yield return Case("Array(Date)", new[] { new DateOnly(1970, 1, 1), new DateOnly(2024, 2, 29) });

        // Transparent wrappers decode and encode as the type they wrap, so the fast path has to look through
        // them — and produce what the wrapped type's boxed Write produced.
        yield return Case("Array(LowCardinality(Int32))", new[] { 1, 2, 3 });
        yield return Case("Array(SimpleAggregateFunction(any, Int64))", new[] { 1L, 2L, 3L });

        // Element CLR type != the column's own type: the typed path must decline, leaving the boxed path's
        // Convert coercion in place.
        yield return Case("Array(Int64)", new[] { 1, 2, 3 });
        yield return Case("Array(Int32)", new[] { 1L, 2L, 3L });
        yield return Case("Array(Float64)", new[] { 1, 2, 3 });
        yield return Case("Array(Decimal(9, 2))", new[] { 1, 2, 3 });
        yield return Case("Array(Enum8('a' = 1, 'b' = 2))", new sbyte[] { 1, 2 });

        // Nullable is not wire-transparent — it prefixes a marker byte — so it must not be looked through
        // the way the transparent wrappers above are, even when the values themselves are never null.
        yield return Case("Array(Nullable(Int32))", new[] { 1, 2, 3 });
        yield return Case("Array(Nullable(DateTime('UTC')))", new[] { new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
        yield return Case("Array(LowCardinality(Nullable(Int32)))", new[] { 1, 2, 3 });

        // Elements the typed path has no entry for at all.
        yield return Case("Array(Nullable(Int32))", new int?[] { 1, null, 3 });
        yield return Case("Array(String)", new[] { "a", "b" });
        yield return Case("Array(Array(Int32))", new[] { new[] { 1, 2 }, new[] { 3 } });

        // Empty and single-element arrays, where an off-by-one in the length prefix would hide.
        yield return Case("Array(Int32)", Array.Empty<int>());
        yield return Case("Array(Int32)", new[] { 42 });
    }

    private static TestCaseData Case<T>(string clickHouseType, T[] values)
        => new TestCaseData(clickHouseType, values, values.ToList())
        { TestName = $"ArrayWrite({clickHouseType} from {typeof(T).Name}[{values.Length}])" };

    [Test]
    [TestCaseSource(nameof(ParityCases))]
    public void Write_TypedArray_EmitsTheSameBytesAsTheBoxedList(string clickHouseType, object array, object list)
    {
        var type = TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);

        Assert.That(Serialize(type, array), Is.EqualTo(Serialize(type, list)));
    }

    /// <summary>
    /// The coercion the boxed path performs is not only a formality: a value that does not fit the column
    /// must still fail, rather than being written through a typed path that never looked at the range.
    /// </summary>
    [Test]
    public void Write_WideningElementArray_StillOverflowsLikeTheBoxedPath()
    {
        var type = TypeConverter.ParseClickHouseType("Array(Int32)", TypeSettings.Default);
        var tooWide = new[] { (long)int.MaxValue + 1 };

        Assert.That(() => Serialize(type, tooWide), Throws.InstanceOf<OverflowException>());
        Assert.That(() => Serialize(type, tooWide.ToList()), Throws.InstanceOf<OverflowException>());
    }

    /// <summary>
    /// A rank-1 array with a non-zero lower bound reports the same element type as a <c>T[]</c> but is not
    /// one. Writing it does not work on the boxed path either — the <see cref="IList"/> indexer counts from
    /// that lower bound — but it must keep failing the way it did, rather than the typed path casting it and
    /// failing differently.
    /// </summary>
    [Test]
    public void Write_ArrayWithNonZeroLowerBound_FailsAsItDidOnTheBoxedPath()
    {
        var type = TypeConverter.ParseClickHouseType("Array(Int32)", TypeSettings.Default);
        var offset = Array.CreateInstance(typeof(int), [3], [1]);
        offset.SetValue(1, 1);

        Assert.That(() => Serialize(type, offset), Throws.InstanceOf<IndexOutOfRangeException>());
    }

    /// <summary>
    /// The write table is hand-written so that every generic instantiation is visible to NativeAOT and
    /// trimming, which means it does not maintain itself: giving a type an <c>ITypedWriter&lt;T&gt;</c> for a
    /// new value type and forgetting the entry silently leaves that element type boxing, with nothing else to
    /// catch it. Mirrors <c>ColumnSlotTests.Binders_CoverEveryTypedReadTarget</c> on the read side.
    /// </summary>
    [Test]
    public void TypedWriters_CoverEveryValueTypeTypedWriteTarget()
    {
        var declared = typeof(ClickHouseType).Assembly
            .GetTypes()
            .Where(t => typeof(ClickHouseType).IsAssignableFrom(t))
            .SelectMany(t => t.GetInterfaces())
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ITypedWriter<>))
            .Select(i => i.GetGenericArguments()[0])
            .Where(t => t.IsValueType)
            .Distinct()
            .ToArray();

        Assert.That(declared, Is.Not.Empty, "found no ITypedWriter<T> implementations at all — check the query");

        var table = (IDictionary)typeof(ArrayType)
            .GetField("TypedWriters", BindingFlags.NonPublic | BindingFlags.Static)
            .GetValue(null);

        Assert.That(declared.Where(t => !table.Contains(t)), Is.Empty,
            "every value type some ClickHouseType can write box-free needs an ArrayType.TypedWriters entry");
    }

    private static byte[] Serialize(ClickHouseType type, object value)
    {
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        type.Write(writer, value);
        writer.Flush();
        return stream.ToArray();
    }
}
