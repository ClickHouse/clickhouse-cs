using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Text.Json.Nodes;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// Pins the rule for a JSON column which declares a path both as a value and as the parent of
/// another path: a row where both sides hold a value throws, and a row where one side holds
/// nothing reads the other. The rule is meant to be order-independent, so both wire orders are
/// covered.
/// </summary>
/// <remarks>
/// The row is decoded from bytes rather than read back from a server because a server sends the
/// deeper of two typed paths first, so no insert produces a row where the shallower one comes
/// first and the deeper one holds nothing. JsonTypeTests covers the orders an insert does produce.
/// </remarks>
[TestFixture]
public class JsonOverlappingPathReadTests
{
    private static readonly Dictionary<string, long> EmptyMap = new();
    private static readonly Dictionary<string, long?> AllNullMap = new() { ["x"] = null };
    private static readonly Dictionary<string, long> MapValue = new() { ["x"] = 1 };

    public static IEnumerable<TestCaseData> Cases()
    {
        // The parent is read first and holds a value, and the deeper path holds nothing: it gives
        // way, and the parent's value survives. Each case is a different shape of "nothing".
        yield return Case("ParentValueBeforeNullPath",
            "JSON(a Int64, `a.b` Nullable(Int64))", [("a", 5L), ("a.b", null)], "{\"a\":5}");

        yield return Case("ParentValueBeforeEmptyObjectPath",
            "JSON(a Int64, `a.b` Map(String, Int64))", [("a", 5L), ("a.b", EmptyMap)], "{\"a\":5}");

        yield return Case("ParentValueBeforeAllNullObjectPath",
            "JSON(a Int64, `a.b` Map(String, Nullable(Int64)))", [("a", 5L), ("a.b", AllNullMap)], "{\"a\":5}");

        // The collision is one level above the path which gives way, so the walk has to stop
        // before it builds a container for the remaining part.
        yield return Case("ParentValueBeforeDeeperNullPath",
            "JSON(a Int64, `a.b.c` Nullable(Int64))", [("a", 5L), ("a.b.c", null)], "{\"a\":5}");

        // The parent's value is an object or an array rather than a scalar. Neither can hold a
        // subtree, but nothing has to: the deeper path holds nothing.
        yield return Case("ObjectParentValueBeforeNullPath",
            "JSON(a Map(String, Int64), `a.b` Nullable(Int64))", [("a", MapValue), ("a.b", null)], "{\"a\":{\"x\":1}}");

        yield return Case("ArrayParentValueBeforeNullPath",
            "JSON(a Array(Int64), `a.b` Nullable(Int64))", [("a", new long[] { 1, 2 }), ("a.b", null)], "{\"a\":[1,2]}");

        // The collision is below a container the row built earlier, so the walk descends before it
        // reaches the value which gives way.
        yield return Case("NestedParentValueBeforeNullPath",
            "JSON(`a.b` Int64, `a.b.c` Nullable(Int64))", [("a.b", 5L), ("a.b.c", null)], "{\"a\":{\"b\":5}}");

        // Contrast: the deeper path holds a value, so the two collide and neither can be dropped.
        yield return Case("ParentValueBeforePathHoldingValue",
            "JSON(a Int64, `a.b` Nullable(Int64))", [("a", 5L), ("a.b", 7L)], null);

        // Contrast: a path which gives way leaves nothing behind, so the sibling read after it
        // still finds the parent's value and collides with it.
        yield return Case("PathHoldingValueAfterOneWhichGaveWay",
            "JSON(a Int64, `a.b` Nullable(Int64), `a.c` Nullable(Int64))",
            [("a", 5L), ("a.b", null), ("a.c", 7L)], null, expectedNestedPath: "a.c");

        // Contrast: the order a server produces is unchanged — the deeper path is read first, and
        // the leaf which lands on its subtree gives way or collides by the same rule.
        yield return Case("NullPathAfterDeeperValue",
            "JSON(a Nullable(Int64), `a.b` Nullable(Int64))", [("a.b", 7L), ("a", null)], "{\"a\":{\"b\":7}}");

        yield return Case("ValueAfterDeeperValue",
            "JSON(a Nullable(Int64), `a.b` Nullable(Int64))", [("a.b", 7L), ("a", 5L)], null);

        // AllowDuplicateJsonKeys keeps a collision readable by dropping one side, but a path which
        // holds nothing needs no such licence, and a subtree still has nowhere to go under a
        // scalar the row also carries.
        yield return Case("ParentValueBeforeNullPathWithDuplicateKeysAllowed",
            "JSON(a Int64, `a.b` Nullable(Int64))", [("a", 5L), ("a.b", null)], "{\"a\":5}", allowDuplicateJsonKeys: true);

        yield return Case("ParentValueBeforePathHoldingValueWithDuplicateKeysAllowed",
            "JSON(a Int64, `a.b` Nullable(Int64))", [("a", 5L), ("a.b", 7L)], null, allowDuplicateJsonKeys: true);
    }

    private static TestCaseData Case(string name, string columnType, (string Path, object Value)[] fields, string expectedJson, bool allowDuplicateJsonKeys = false, string expectedNestedPath = "a.b")
        => new TestCaseData(columnType, fields, allowDuplicateJsonKeys, expectedJson, expectedNestedPath) { TestName = name };

    [Test]
    [TestCaseSource(nameof(Cases))]
    public void Read_WithOverlappingPaths_ShouldKeepTheValueWhicheverOrderTheWireUses(
        string columnType, (string Path, object Value)[] fields, bool allowDuplicateJsonKeys, string expectedJson, string expectedNestedPath)
    {
        if (expectedJson is not null)
        {
            Assert.That(ReadRow(columnType, fields, allowDuplicateJsonKeys).ToJsonString(), Is.EqualTo(expectedJson));
            return;
        }

        var exception = Assert.Throws<SerializationException>(() => ReadRow(columnType, fields, allowDuplicateJsonKeys));

        Assert.That(exception.Message, Does.Contain("'a'"), exception.Message);
        Assert.That(exception.Message, Does.Contain($"'{expectedNestedPath}'"), exception.Message);
    }

    /// <summary>
    /// Decodes one JSON column value made of the given paths, in the given order.
    /// </summary>
    private static JsonObject ReadRow(string columnType, (string Path, object Value)[] fields, bool allowDuplicateJsonKeys)
    {
        var settings = TypeSettings.Default with { allowDuplicateJsonKeys = allowDuplicateJsonKeys };
        var type = (JsonType)TypeConverter.ParseClickHouseType(columnType, settings);

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        writer.Write7BitEncodedInt(fields.Length);
        foreach (var (path, value) in fields)
        {
            writer.Write(path);
            type.HintedTypes[path].Write(writer, value);
        }

        stream.Seek(0, SeekOrigin.Begin);
        using var reader = new ExtendedBinaryReader(stream);
        return (JsonObject)type.Read(reader);
    }
}
