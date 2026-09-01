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
/// Reads of columns that carry per-block serialization state, split across more than one block. A column whose
/// body cannot be decoded without state read ahead of it — a <c>LowCardinality</c> dictionary, a <c>Dynamic</c>
/// runtime type list, a version word — puts that state in a per-block prefix, and
/// <c>BlockReader</c> reads one prefix per block whose row count is greater than zero. If that placement were
/// wrong, the first block would still decode and the second would desync: the reader would take body bytes for
/// a prefix, or a stale dictionary for the current one. So a single-block read proves nothing here, and every
/// case below asserts across at least two blocks.
///
/// <para>
/// Only a real server settles it. Reading back what this client wrote agrees with the client's own model of
/// where the prefix goes, whether or not the server shares it.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class MultiBlockStateIntegrationTests
{
    private const int Rows = 9;

    private static readonly CancellationToken None = CancellationToken.None;

    // max_block_size = 2 over 9 rows splits the result into blocks of 2,2,2,2,1, and max_threads = 1 keeps
    // system.numbers on one stream so the split points and the block order are both deterministic. The two
    // experimental flags let the Dynamic and Variant cases run and are inert for the others.
    private static readonly Dictionary<string, string> SplitSettings = new(StringComparer.Ordinal)
    {
        ["max_block_size"] = "2",
        ["max_threads"] = "1",
        ["allow_experimental_dynamic_type"] = "1",
        ["allow_experimental_variant_type"] = "1",
    };

    private static IEnumerable<TestCaseData> StatefulColumns()
    {
        // Cycling '% 3' through 2-row blocks means consecutive blocks hold different distinct sets, so a
        // dictionary held over from the previous block decodes to a wrong value instead of failing outright.
        yield return new TestCaseData(
                "toLowCardinality(concat('v', toString(number % 3)))",
                NineRows(i => "v" + (i % 3)))
            .SetName("{m}(LowCardinality(String))");

        // Nullable moves every dictionary key up by one reserved slot, so it decodes the prefix differently.
        yield return new TestCaseData(
                "toLowCardinality(if(number % 3 = 0, NULL, concat('v', toString(number)))::Nullable(String))",
                NineRows(i => i % 3 == 0 ? null : "v" + i))
            .SetName("{m}(LowCardinality(Nullable(String)))");

        // Nested one level down, where the prefix belongs to the inner column and not to the one named in the
        // block header.
        yield return new TestCaseData(
                "[toLowCardinality(concat('v', toString(number)))]",
                NineRows(i => new[] { "v" + i }))
            .SetName("{m}(Array(LowCardinality(String)))");

        yield return new TestCaseData(
                "map(toLowCardinality(concat('k', toString(number))), toUInt8(number))",
                NineRows(i => new[] { new KeyValuePair<string, byte>("k" + i, (byte)i) }))
            .SetName("{m}(Map(LowCardinality(String), UInt8))");

        // Two dictionary-bearing children in one column: the prefixes are consecutive, so reading one too few or
        // too many bytes for the first mis-frames the second.
        yield return new TestCaseData(
                "tuple(toLowCardinality(concat('a', toString(number))), toLowCardinality(concat('b', toString(number))))",
                NineRows(i => ("a" + i, "b" + i)))
            .SetName("{m}(Tuple of two LowCardinality(String))");

        // A Dynamic whose runtime type changes twice down the result, so the type list a block declares differs
        // from the one before it. StreamAsync_DynamicSplitAcrossBlocks... asserts those lists directly.
        yield return new TestCaseData(
                "CAST(if(number < 3, CAST(toInt64(number), 'Dynamic'), if(number < 6, CAST(concat('s', toString(number)), 'Dynamic'), CAST(toFloat64(number), 'Dynamic'))), 'Dynamic')",
                NineRows(i => i < 3 ? (long)i : i < 6 ? "s" + i : (double)i))
            .SetName("{m}(Dynamic whose runtime type list differs per block)");

        // NULL in a Dynamic is the discriminator one past the last runtime type, so it moves with the per-block
        // list rather than sitting at a fixed sentinel.
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
            // The precondition, asserted rather than assumed: a server that returned one block would make the
            // row comparison below pass while proving nothing about the second prefix.
            Assert.That(blocks, Is.GreaterThan(1), "max_block_size = 2 must split the result");
            Assert.That(readBack, Is.EqualTo(expected));
        });
    }

    /// <summary>
    /// The dictionary belongs to the block, not to the column: the same key means a different value in a
    /// different block. Cycling three values through 2-row blocks makes the keys identical block to block
    /// (<c>[1, 2]</c>) while the values behind them move, so a dictionary read once and reused would decode
    /// every block to block one's values and no length would look wrong.
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

            // Copied out of the borrowed block, and past the reserved leading slot so only the distinct data
            // values are compared.
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
    /// A <c>Dynamic</c> column's runtime type list is discovered per block, so a block declares only the types
    /// its own rows use: the list grows, shrinks, and here changes to a type no earlier block named. Every row
    /// still decodes, which is what pins the list to the block.
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
