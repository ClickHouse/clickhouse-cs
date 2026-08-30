using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers <see cref="ProjectedLowCardinalityColumn{T}"/>, the view <c>Block.ReadAs&lt;T&gt;</c> returns for a
/// low-cardinality column asked for a type other than its own. What it exists to guarantee is <em>how many
/// times</em> a conversion runs, which no server round-trip can observe: the values are identical either way, so
/// these tests count the conversions instead of comparing values. The reader is supplied directly rather than
/// compiled from a codec, so a test asserts the view's own bookkeeping and nothing else.
/// </summary>
[TestFixture]
public class ProjectedLowCardinalityColumnTests
{
    /// <summary>A three-entry dictionary: slot 0 reserved, then "alpha" and "beta".</summary>
    private static StringColumn Dictionary()
        => new("v", "String", "alphabeta"u8.ToArray(), new[] { 0, 0, 5, 9 }, rowCount: 3, pooled: false);

    [Test]
    public void Indexer_RowsSharingADictionaryEntry_ConvertsThatEntryOnce()
    {
        // Six rows over two distinct entries. Converting per row would run six times; per entry runs twice.
        using StringColumn dictionary = Dictionary();
        using var column = new LowCardinalityColumn<string>(
            "v", "LowCardinality(String)", dictionary, new[] { 1, 2, 1, 2, 1, 1 }, rowCount: 6, pooledKeys: false);

        var slots = new List<int>();
        using var projected = new ProjectedLowCardinalityColumn<string>(column, (dict, slot) =>
        {
            slots.Add(slot);
            return ((StringColumn)dict).GetString(slot, System.Text.Encoding.UTF8).ToUpperInvariant();
        });

        var read = new string[6];
        for (int row = 0; row < 6; row++)
        {
            read[row] = projected[row];
        }

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(new[] { "ALPHA", "BETA", "ALPHA", "BETA", "ALPHA", "ALPHA" }));
            Assert.That(slots, Is.EqualTo(new[] { 1, 2 }), "one conversion per distinct entry, in first-touch order");
            Assert.That(projected[0], Is.SameAs(projected[2]), "rows sharing an entry share its converted value");
        });
    }

    [Test]
    public void Indexer_EntryNoRowNames_IsNeverConverted()
    {
        // Conversion is on first touch, not up front, so a block of few rows over a large dictionary does not pay
        // for entries none of its rows name.
        using StringColumn dictionary = Dictionary();
        using var column = new LowCardinalityColumn<string>(
            "v", "LowCardinality(String)", dictionary, new[] { 2, 2 }, rowCount: 2, pooledKeys: false);

        var slots = new List<int>();
        using var projected = new ProjectedLowCardinalityColumn<string>(column, (dict, slot) =>
        {
            slots.Add(slot);
            return "converted";
        });

        _ = projected[0];
        _ = projected[1];

        Assert.That(slots, Is.EqualTo(new[] { 2 }), "slots 0 and 1 are in the dictionary but named by no row");
    }

    [Test]
    public void Indexer_NullableDictionary_ReadsTheNullSlotAsDefaultWithoutConvertingIt()
    {
        // Slot 0 of a nullable dictionary is the NULL marker, not a value: the placeholder the wire carries there
        // must never reach the reader, and a row keyed on it reads as default(T).
        using StringColumn dictionary = Dictionary();
        using var column = new NullableLowCardinalityReferenceColumn<string>(
            "v", "LowCardinality(Nullable(String))", dictionary, new[] { 2, 0, 1, 0 }, rowCount: 4, pooledKeys: false);

        var slots = new List<int>();
        using var projected = new ProjectedLowCardinalityColumn<string>(column, (dict, slot) =>
        {
            slots.Add(slot);
            return $"entry{slot}";
        });

        var read = new string[4];
        for (int row = 0; row < 4; row++)
        {
            read[row] = projected[row];
        }

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(new[] { "entry2", null, "entry1", null }));
            Assert.That(slots, Does.Not.Contain(0), "the NULL marker is not a value to convert");
        });
    }

    [Test]
    public void Values_Materialized_AgreesWithTheIndexerAndStillSharesEntries()
    {
        using StringColumn dictionary = Dictionary();
        using var column = new LowCardinalityColumn<string>(
            "v", "LowCardinality(String)", dictionary, new[] { 1, 2, 1 }, rowCount: 3, pooledKeys: false);

        int conversions = 0;
        using var projected = new ProjectedLowCardinalityColumn<string>(column, (dict, slot) =>
        {
            conversions++;
            return $"entry{slot}";
        });

        string[] materialized = projected.Values.ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(materialized, Is.EqualTo(new[] { "entry1", "entry2", "entry1" }));
            Assert.That(conversions, Is.EqualTo(2), "Values converts per entry too, not per row");
            Assert.That(materialized[0], Is.SameAs(materialized[2]));

            // The indexer answers from the materialized array once it exists, and must not disagree with it.
            Assert.That(projected[2], Is.SameAs(materialized[2]));
            Assert.That(conversions, Is.EqualTo(2), "and converts nothing further");
        });
    }

    [Test]
    public void Values_ZeroRowColumn_IsEmptyAndConvertsNothing()
    {
        using var dictionary = new StringColumn("v", "String", Array.Empty<byte>(), new int[1], rowCount: 0, pooled: false);
        using var column = new LowCardinalityColumn<string>(
            "v", "LowCardinality(String)", dictionary, Array.Empty<int>(), rowCount: 0, pooledKeys: false);

        int conversions = 0;
        using var projected = new ProjectedLowCardinalityColumn<string>(column, (dict, slot) =>
        {
            conversions++;
            return "converted";
        });

        Assert.Multiple(() =>
        {
            Assert.That(projected.RowCount, Is.Zero);
            Assert.That(projected.Values.IsEmpty, Is.True);
            Assert.That(conversions, Is.Zero);
        });
    }

    [Test]
    public void Indexer_RowOutsideTheColumn_Throws()
    {
        using StringColumn dictionary = Dictionary();
        using var column = new LowCardinalityColumn<string>(
            "v", "LowCardinality(String)", dictionary, new[] { 1, 2 }, rowCount: 2, pooledKeys: false);

        using var projected = new ProjectedLowCardinalityColumn<string>(column, (dict, slot) => "converted");

        Assert.Multiple(() =>
        {
            Assert.Throws<IndexOutOfRangeException>(() => _ = projected[2]);
            Assert.Throws<IndexOutOfRangeException>(() => _ = projected[-1]);
        });
    }

    [Test]
    public void GetValue_BoxedRow_IsTheSameConvertedEntryTheIndexerGives()
    {
        using StringColumn dictionary = Dictionary();
        using var column = new LowCardinalityColumn<string>(
            "v", "LowCardinality(String)", dictionary, new[] { 1, 2, 1 }, rowCount: 3, pooledKeys: false);

        int conversions = 0;
        using var projected = new ProjectedLowCardinalityColumn<string>(column, (dict, slot) =>
        {
            conversions++;
            return $"entry{slot}";
        });

        Assert.Multiple(() =>
        {
            Assert.That(projected.GetValue(0), Is.EqualTo("entry1"));
            Assert.That(projected.GetValue(2), Is.SameAs(projected[0]), "the boxed reading shares the entry too");
            Assert.That(conversions, Is.EqualTo(1));
        });
    }

    [Test]
    public void NameAndTypeName_AreTheSourceColumnsOwn()
    {
        using StringColumn dictionary = Dictionary();
        using var column = new LowCardinalityColumn<string>(
            "label", "LowCardinality(String)", dictionary, new[] { 1 }, rowCount: 1, pooledKeys: false);

        using var projected = new ProjectedLowCardinalityColumn<string>(column, (dict, slot) => "converted");

        Assert.Multiple(() =>
        {
            Assert.That(projected.Name, Is.EqualTo("label"));
            Assert.That(projected.TypeName, Is.EqualTo("LowCardinality(String)"));
        });
    }

    [Test]
    public void Dispose_LeavesTheSourceColumnReadable()
    {
        // The source and its dictionary belong to the block, so a reader disposing its own view must not empty them.
        using StringColumn dictionary = Dictionary();
        using var column = new LowCardinalityColumn<string>(
            "v", "LowCardinality(String)", dictionary, new[] { 1, 2 }, rowCount: 2, pooledKeys: false);

        var projected = new ProjectedLowCardinalityColumn<string>(column, (dict, slot) => $"entry{slot}");
        _ = projected[0];
        projected.Dispose();

        Assert.That(column[0], Is.EqualTo("alpha"), "the block's column still holds its data");
    }
}
