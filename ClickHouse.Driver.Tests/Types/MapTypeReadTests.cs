using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// Pins the cost and the exact returned CLR type of <c>MapType.Read</c>. The dictionary is built by
/// a compiled per-shape factory rather than a reflection invoke, so both the per-call allocation and
/// the concrete <c>Dictionary&lt;TKey, TValue&gt;</c> it produces are part of the contract.
/// </summary>
[TestFixture]
public class MapTypeReadTests
{
    public static IEnumerable<TestCaseData> Cases()
    {
        yield return Case("Map(String, Int32)", typeof(Dictionary<string, int>), new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 });
        yield return Case("Map(Int64, Nullable(String))", typeof(Dictionary<long, string>), new Dictionary<long, string> { [1L] = "x", [2L] = null });
        yield return Case("Map(String, Int32)", typeof(Dictionary<string, int>), new Dictionary<string, int>());
    }

    private static TestCaseData Case(string chType, Type expectedType, object value)
        => new TestCaseData(chType, expectedType, value) { TestName = $"MapRead({chType}, {((IDictionary)value).Count} entries)" };

    [Test]
    [TestCaseSource(nameof(Cases))]
    public void Read_Map_ReturnsExactlyTypedDictionaryWithPreservedValues(string clickHouseType, Type expectedType, object original)
    {
        var type = TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        using var reader = new ExtendedBinaryReader(stream);
        type.Write(writer, original);
        stream.Seek(0, SeekOrigin.Begin);
        var read = type.Read(reader);

        Assert.That(stream.Position, Is.EqualTo(stream.Length), "read must consume exactly the written bytes");
        Assert.That(read, Is.TypeOf(expectedType));
        TestUtilities.AssertEqual(original, read);
    }

    [Test]
    public void Read_Map_DoesNotAllocateReflectionOverheadPerCall()
    {
        // Constructing the dictionary through Activator.CreateInstance(Type, params object[]) runs
        // binder-based constructor resolution on every call, costing a flat ~650 extra bytes per
        // read on top of the dictionary itself. The bound cleanly separates the two mechanisms.
        const int iterations = 1000;
        var type = TypeConverter.ParseClickHouseType("Map(String, Int32)", TypeSettings.Default);
        var value = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 };

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        using var reader = new ExtendedBinaryReader(stream);
        type.Write(writer, value);
        writer.Flush();

        stream.Seek(0, SeekOrigin.Begin);
        type.Read(reader); // warm up JIT and the compiled factory

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            stream.Seek(0, SeekOrigin.Begin);
            type.Read(reader);
        }
        var perRead = (GC.GetAllocatedBytesForCurrentThread() - before) / iterations;

        Assert.That(perRead, Is.LessThan(600), $"allocated {perRead} bytes per Map read");
    }
}
