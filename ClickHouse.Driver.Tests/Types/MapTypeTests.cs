using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

public class MapTypeTests : AbstractConnectionTestFixture
{
    public static IEnumerable<TestCaseData> MapReadCases()
    {
        // String key / Int32 value
        yield return new TestCaseData(
            "CAST(map('a', 1, 'b', 2, 'c', 3), 'Map(String, Int32)')",
            new Dictionary<string, int> { ["a"] = 1, ["b"] = 2, ["c"] = 3 });

        // Single entry (capacity == 1 boundary)
        yield return new TestCaseData(
            "CAST(map('only', 42), 'Map(String, Int32)')",
            new Dictionary<string, int> { ["only"] = 42 });

        // Numeric key / String value — different framework type than the cases above
        yield return new TestCaseData(
            "CAST(map(1, 'a', 2, 'b'), 'Map(UInt8, String)')",
            new Dictionary<byte, string> { [1] = "a", [2] = "b" });

        // Composite (Tuple) key — exercises Dictionary<Tuple<int,int>, int> construction
        yield return new TestCaseData(
            "CAST([((1, 2), 10), ((3, 4), 20)], 'Map(Tuple(Int32, Int32), Int32)')",
            new Dictionary<Tuple<int, int>, int>
            {
                [Tuple.Create(1, 2)] = 10,
                [Tuple.Create(3, 4)] = 20,
            });
    }

    [Test]
    [RequiredFeature(Feature.Map)]
    [TestCaseSource(nameof(MapReadCases))]
    public async Task ReadMap_WithVariousKeyValueTypes_ReturnsAllKeyValuePairs(string expression, IDictionary expected)
    {
        var result = await connection.ExecuteScalarAsync($"SELECT {expression}");
        AssertMapEquals(expected, result);
    }

    [Test]
    [RequiredFeature(Feature.Map)]
    public async Task ReadMap_Empty_ReturnsEmptyDictionary()
    {
        // Length 0: the dictionary must still be constructed correctly when pre-sized to zero.
        var result = await connection.ExecuteScalarAsync("SELECT CAST([], 'Map(String, Int32)')");
        AssertMapEquals(new Dictionary<string, int>(), result);
    }

    [Test]
    [RequiredFeature(Feature.Map)]
    public async Task ReadMap_LargeDictionary_ReturnsAllEntries()
    {
        // A large map exercises the pre-sizing path: without a capacity hint the dictionary
        // would rehash repeatedly as these 1000 entries are inserted.
        const int count = 1000;
        var expected = new Dictionary<ulong, ulong>(count);
        for (ulong i = 0; i < count; i++)
            expected[i] = i * 10;

        var result = await connection.ExecuteScalarAsync(
            $"SELECT CAST(arrayMap(i -> (i, i * 10), range({count})), 'Map(UInt64, UInt64)')");

        AssertMapEquals(expected, result);
    }

    // Type-agnostic dictionary comparison: same count and, for every expected key, the same value.
    private static void AssertMapEquals(IDictionary expected, object result)
    {
        Assert.That(result, Is.InstanceOf<IDictionary>());
        var actual = (IDictionary)result;
        Assert.That(actual.Count, Is.EqualTo(expected.Count), "Entry count mismatch");
        foreach (DictionaryEntry entry in expected)
        {
            Assert.That(actual.Contains(entry.Key), Is.True, $"Missing key {entry.Key}");
            Assert.That(actual[entry.Key], Is.EqualTo(entry.Value), $"Value mismatch for key {entry.Key}");
        }
    }
}
