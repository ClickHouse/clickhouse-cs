using System;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using ClickHouse.Driver;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// Locks the contract of <c>ArrayType.Read</c>: whatever element type the array holds, the
/// returned CLR array must have exactly <c>UnderlyingType.FrameworkType.MakeArrayType()</c> as
/// its runtime type and preserve every element value (including nulls). This covers both the
/// typed fast path (leaf value/string element types) and the reflection fallback path
/// (composite / big-integer / big-decimal element types).
/// </summary>
[TestFixture]
public class ArrayTypeReadTests
{
    public static IEnumerable<TestCaseData> Cases()
    {
        // --- typed fast path: exact T[] must come back ---
        yield return Case("Array(Int8)", typeof(sbyte[]), new sbyte[] { -1, 0, 2 });
        yield return Case("Array(UInt8)", typeof(byte[]), new byte[] { 0, 1, 255 });
        yield return Case("Array(Int16)", typeof(short[]), new short[] { -1, 0, 300 });
        yield return Case("Array(UInt16)", typeof(ushort[]), new ushort[] { 0, 1, 65535 });
        yield return Case("Array(Int32)", typeof(int[]), new[] { 1, 2, 3 });
        yield return Case("Array(UInt32)", typeof(uint[]), new uint[] { 0, 1, 4000000000 });
        yield return Case("Array(Int64)", typeof(long[]), new[] { -1L, 0L, 5L });
        yield return Case("Array(UInt64)", typeof(ulong[]), new ulong[] { 0UL, 1UL, 9UL });
        yield return Case("Array(Float32)", typeof(float[]), new[] { 1.5f, -2.5f });
        yield return Case("Array(Float64)", typeof(double[]), new[] { 1.5d, -2.5d });
        yield return Case("Array(Bool)", typeof(bool[]), new[] { true, false, true });
        yield return Case("Array(UUID)", typeof(Guid[]), new[] { Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"), Guid.Empty });
        yield return Case("Array(String)", typeof(string[]), new[] { "a", "bb", string.Empty });
        yield return Case("Array(DateTime('UTC'))", typeof(DateTime[]), new[]
        {
            new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2021, 6, 15, 12, 30, 0, DateTimeKind.Utc),
        });

        // --- nullable fast path: the null sentinel must be restored, exact Nullable<T>[] / string[] ---
        yield return Case("Array(Nullable(Int32))", typeof(int?[]), new int?[] { 1, null, 3 });
        yield return Case("Array(Nullable(String))", typeof(string[]), new[] { "a", null, "c" });

        // --- nested arrays: element type is itself an array ---
        yield return Case("Array(Array(Int32))", typeof(int[][]), new[] { new[] { 1, 2 }, new[] { 3 } });

        // --- edge: empty array keeps its typed element ---
        yield return new TestCaseData("Array(Int32)", typeof(int[]), Array.Empty<int>())
        { TestName = "ArrayRead(Array(Int32) empty)" };

        // --- reflection fallback element types remain correct ---
        yield return Case("Array(Int128)", typeof(BigInteger[]), new BigInteger[] { 1, -2, 3 });
    }

    private static TestCaseData Case(string chType, Type expectedArrayType, object value)
        => new TestCaseData(chType, expectedArrayType, value) { TestName = $"ArrayRead({chType})" };

    [Test]
    [TestCaseSource(nameof(Cases))]
    public void Read_Array_ReturnsExactlyTypedArrayWithPreservedValues(string clickHouseType, Type expectedArrayType, object original)
    {
        var read = RoundtripRead(original, clickHouseType, TypeSettings.Default);

        Assert.That(read, Is.TypeOf(expectedArrayType), "returned array runtime type must equal element FrameworkType[]");
        TestUtilities.AssertEqual(original, read);
    }

    [Test]
    public void Read_DecimalArray_WithoutBigDecimal_ReturnsDecimalArray()
    {
        // With useBigDecimal=false the Decimal element FrameworkType is decimal, exercising the
        // decimal fast-path entry (Default uses ClickHouseDecimal, exercised via the fallback path).
        var settings = new TypeSettings(useBigDecimal: false, readStringsAsByteArrays: false, jsonTypeRegistry: null, jsonReadMode: JsonReadMode.Binary, jsonWriteMode: JsonWriteMode.String);
        var original = new[] { 1.23m, -4.50m, 0m };

        var read = RoundtripRead(original, "Array(Decimal(9, 2))", settings);

        Assert.That(read, Is.TypeOf<decimal[]>());
        TestUtilities.AssertEqual(original, read);
    }

    private static object RoundtripRead(object original, string clickHouseType, TypeSettings settings)
    {
        var type = TypeConverter.ParseClickHouseType(clickHouseType, settings);

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        using var reader = new ExtendedBinaryReader(stream);
        type.Write(writer, original);
        stream.Seek(0, SeekOrigin.Begin);
        var read = type.Read(reader);
        Assert.That(stream.Position, Is.EqualTo(stream.Length), "read must consume exactly the written bytes");
        return read;
    }
}
