using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Verifies per-block state prefixes against a real server. Each case spans multiple blocks so stale or
/// misaligned dictionaries, runtime type lists, and version words are observable.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MultiBlockStateIntegrationTests
{
    private const int Rows = 9;

    private static readonly CancellationToken None = CancellationToken.None;

    // Split nine ordered rows into deterministic blocks of 2,2,2,2,1.
    private static readonly Dictionary<string, string> SplitSettings = new(StringComparer.Ordinal)
    {
        ["max_block_size"] = "2",
        ["max_threads"] = "1",
        ["allow_experimental_dynamic_type"] = "1",
        ["allow_experimental_variant_type"] = "1",
    };

    private static IEnumerable<TestCaseData> StatefulColumns()
    {
        // Consecutive blocks reuse keys for different dictionary values.
        yield return new TestCaseData(
                "toLowCardinality(concat('v', toString(number % 3)))",
                NineRows(i => "v" + (i % 3)))
            .SetName("{m}(LowCardinality(String))");

        // Nullable adds a reserved dictionary slot.
        yield return new TestCaseData(
                "toLowCardinality(if(number % 3 = 0, NULL, concat('v', toString(number)))::Nullable(String))",
                NineRows(i => i % 3 == 0 ? null : "v" + i))
            .SetName("{m}(LowCardinality(Nullable(String)))");

        // The prefix belongs to the nested inner column.
        yield return new TestCaseData(
                "[toLowCardinality(concat('v', toString(number)))]",
                NineRows(i => new[] { "v" + i }))
            .SetName("{m}(Array(LowCardinality(String)))");

        yield return new TestCaseData(
                "map(toLowCardinality(concat('k', toString(number))), toUInt8(number))",
                NineRows(i => new[] { new KeyValuePair<string, byte>("k" + i, (byte)i) }))
            .SetName("{m}(Map(LowCardinality(String), UInt8))");

        // Consecutive child prefixes expose framing errors.
        yield return new TestCaseData(
                "tuple(toLowCardinality(concat('a', toString(number))), toLowCardinality(concat('b', toString(number))))",
                NineRows(i => ("a" + i, "b" + i)))
            .SetName("{m}(Tuple of two LowCardinality(String))");

        // Change Dynamic runtime types between blocks.
        yield return new TestCaseData(
                "CAST(if(number < 3, CAST(toInt64(number), 'Dynamic'), if(number < 6, CAST(concat('s', toString(number)), 'Dynamic'), CAST(toFloat64(number), 'Dynamic'))), 'Dynamic')",
                NineRows(i => i < 3 ? (long)i : i < 6 ? "s" + i : (double)i))
            .SetName("{m}(Dynamic whose runtime type list differs per block)");

        // Dynamic's null discriminator depends on the per-block type list.
        yield return new TestCaseData(
                "CAST(if(number % 3 = 1, CAST(NULL, 'Dynamic'), CAST(toInt64(number), 'Dynamic')), 'Dynamic')",
                NineRows(i => i % 3 == 1 ? null : (long)i))
            .SetName("{m}(Dynamic holding NULLs)");

        yield return new TestCaseData(
                "if(number % 2 = 0, CAST(toInt64(number), 'Variant(Int64, String)'), CAST(concat('s', toString(number)), 'Variant(Int64, String)'))",
                NineRows(i => i % 2 == 0 ? (long)i : "s" + i))
            .SetName("{m}(Variant(Int64, String))");

        yield return new TestCaseData(
                "CAST(concat('{\"a\":', toString(number), '}'), 'JSON')",
                NineRows(i => "{\"a\":" + i.ToString(CultureInfo.InvariantCulture) + "}"))
            .SetName("{m}(JSON)");
    }

    [TestCaseSource(nameof(StatefulColumns))]
    public async Task StreamAsync_StatefulColumnSplitAcrossBlocks_ReadsEveryRowInOrder(string expression, object[] expected)
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions { Settings = SplitSettings };

        int blocks = 0;
        var readBack = new List<object>(Rows);
        await foreach (Block block in client.StreamAsync(
            $"SELECT {expression} FROM system.numbers LIMIT {Rows}", options, cancellationToken: None))
        {
            blocks++;
            for (int row = 0; row < block.RowCount; row++)
            {
                readBack.Add(block[0].GetValue(row));
            }
        }

        Assert.Multiple(() =>
        {
            // Ensure the query exercised more than one state prefix.
            Assert.That(blocks, Is.GreaterThan(1), "max_block_size = 2 must split the result");
            Assert.That(readBack, Is.EqualTo(expected));
        });
    }

    /// <summary>
    /// Verifies that identical keys resolve against each block's dictionary.
    /// </summary>
    [Test]
    public async Task StreamAsync_LowCardinalitySplitAcrossBlocks_ResolvesEachBlockAgainstItsOwnDictionary()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions { Settings = SplitSettings };

        var dictionaries = new List<string[]>();
        var keys = new List<int[]>();
        var readBack = new List<object>(Rows);
        await foreach (Block block in client.StreamAsync(
            $"SELECT toLowCardinality(concat('v', toString(number % 3))) FROM system.numbers LIMIT {Rows}",
            options,
            cancellationToken: None))
        {
            var lowCardinality = (ILowCardinalityColumn<string>)block[0];

            // Copy values out of the borrowed block, excluding reserved slots.
            var distinct = new string[lowCardinality.Dictionary.RowCount - lowCardinality.ReservedSlotCount];
            for (int slot = 0; slot < distinct.Length; slot++)
            {
                distinct[slot] = lowCardinality.Dictionary[slot + lowCardinality.ReservedSlotCount];
            }

            dictionaries.Add(distinct);
            keys.Add(lowCardinality.Keys.ToArray());
            for (int row = 0; row < block.RowCount; row++)
            {
                readBack.Add(block[0].GetValue(row));
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(dictionaries, Has.Count.EqualTo(5));
            Assert.That(dictionaries[0], Is.EqualTo(new[] { "v0", "v1" }));
            Assert.That(dictionaries[1], Is.EqualTo(new[] { "v2", "v0" }), "its own dictionary, in its own order");
            Assert.That(dictionaries[2], Is.EqualTo(new[] { "v1", "v2" }));
            Assert.That(keys[0], Is.EqualTo(keys[1]), "identical keys, so only the per-block dictionary tells the values apart");
            Assert.That(readBack, Is.EqualTo(NineRows(i => "v" + (i % 3))));
        });
    }

    /// <summary>
    /// Verifies that each block uses its own <c>Dynamic</c> runtime type list.
    /// </summary>
    [Test]
    public async Task StreamAsync_DynamicSplitAcrossBlocks_DeclaresEachBlocksOwnRuntimeTypeList()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions { Settings = SplitSettings };

        var typeLists = new List<string[]>();
        var readBack = new List<object>(Rows);
        await foreach (Block block in client.StreamAsync(
            $"""
            SELECT CAST(if(number < 3, CAST(toInt64(number), 'Dynamic'),
                           if(number < 6, CAST(concat('s', toString(number)), 'Dynamic'),
                                          CAST(toFloat64(number), 'Dynamic'))), 'Dynamic')
            FROM system.numbers LIMIT {Rows}
            """,
            options,
            cancellationToken: None))
        {
            typeLists.Add(((IDynamicColumn)block[0]).TypeNames.ToArray());
            for (int row = 0; row < block.RowCount; row++)
            {
                readBack.Add(block[0].GetValue(row));
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(typeLists, Has.Count.EqualTo(5));

            // Rows 0-1, then 2-3 straddling the Int64/String change, then 4-5, then the Float64 rows.
            Assert.That(typeLists[0], Is.EquivalentTo(new[] { "Int64" }));
            Assert.That(typeLists[1], Is.EquivalentTo(new[] { "Int64", "String" }), "two types where the block before named one");
            Assert.That(typeLists[2], Is.EquivalentTo(new[] { "String" }), "and back to one");
            Assert.That(typeLists[3], Is.EquivalentTo(new[] { "Float64" }), "a type no earlier block named");
            Assert.That(typeLists[4], Is.EquivalentTo(new[] { "Float64" }));
            Assert.That(readBack, Is.EqualTo(NineRows(i => i < 3 ? (long)i : i < 6 ? "s" + i : (double)i)));
        });
    }

    private static object[] NineRows(Func<int, object> value)
    {
        var expected = new object[Rows];
        for (int i = 0; i < Rows; i++)
        {
            expected[i] = value(i);
        }

        return expected;
    }
}
