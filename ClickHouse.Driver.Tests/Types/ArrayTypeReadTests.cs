using System;
using System.Collections.Generic;
using System.IO;
using ClickHouse.Driver;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// Locks the exact returned CLR array type of <c>ArrayType.Read</c>. SerialisationTests already
/// round-trips element values for every array type, but its element-wise equality does not pin the
/// array's runtime type — the contract the typed fast path must preserve (e.g. <c>int?[]</c> must
/// not collapse to <c>int[]</c> or <c>object[]</c>).
/// </summary>
[TestFixture]
public class ArrayTypeReadTests
{
    public static IEnumerable<TestCaseData> Cases()
    {
        // typed fast path: value, nullable value (null sentinel restored), shared string entry
        yield return Case("Array(Int32)", typeof(int[]), new[] { 1, 2, 3 });
        yield return Case("Array(Nullable(Int32))", typeof(int?[]), new int?[] { 1, null, 3 });
        yield return Case("Array(Nullable(String))", typeof(string[]), new[] { "a", null, "c" });

        // reflection fallback path still returns the exact nested type
        yield return Case("Array(Array(Int32))", typeof(int[][]), new[] { new[] { 1, 2 }, new[] { 3 } });

        // edge: empty array keeps its typed element
        yield return new TestCaseData("Array(Int32)", typeof(int[]), Array.Empty<int>())
        { TestName = "ArrayRead(Array(Int32) empty)" };
    }

    private static TestCaseData Case(string chType, Type expectedArrayType, object value)
        => new TestCaseData(chType, expectedArrayType, value) { TestName = $"ArrayRead({chType})" };

    [Test]
    [TestCaseSource(nameof(Cases))]
    public void Read_Array_ReturnsExactlyTypedArrayWithPreservedValues(string clickHouseType, Type expectedArrayType, object original)
    {
        var read = RoundtripRead(original, clickHouseType, TypeSettings.Default);

        Assert.That(read, Is.TypeOf(expectedArrayType));
        TestUtilities.AssertEqual(original, read);
    }

    [Test]
    public void Read_DecimalArray_WithoutBigDecimal_ReturnsDecimalArray()
    {
        // Default settings read Decimal as ClickHouseDecimal (fallback); useBigDecimal=false is the
        // only way the decimal fast-path entry is reached.
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
