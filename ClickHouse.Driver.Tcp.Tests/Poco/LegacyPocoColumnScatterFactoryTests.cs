using System;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The tier selection of <see cref="LegacyPocoColumnScatterFactory"/>, the scatter of the reference path of the
/// differential tests, which stays until the old path is removed.
/// </summary>
[TestFixture]
public class LegacyPocoColumnScatterFactoryTests
{
    [Test]
    public void Create_IndexerTier_AlsoRunsUnderTheExpressionInterpreter()
    {
        // Why the indexer tier exists at all: a runtime without dynamic code interprets the tree instead of
        // compiling it, and an interpreted tree cannot hold a ReadOnlySpan<T>. A test host that has dynamic code
        // never takes that path, so the interpreter is asked for explicitly here. Otherwise the fallback ships
        // untested and only fails on NativeAOT.
        IColumn column = Ints("value", 1, -2);
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve("Int32", new ResolveContext());
        PocoMember member = PocoTypeDescriptor<Row<int>>.Build().Members[0];
        PocoColumnScatter<Row<int>> scatter = LegacyPocoColumnScatterFactory.Create<Row<int>>(column, codec, member, LegacyPocoScatterTier.Indexer, preferInterpretation: true);
        var rows = new[] { new Row<int>(), new Row<int>() };

        scatter(column, rows, start: 0, rows.Length, rowOffset: 0);

        Assert.That(Array.ConvertAll(rows, row => row.Value), Is.EqualTo(new[] { 1, -2 }));
    }

    [Test]
    public void SelectTier_StoredValuesColumnAndNoForcedTier_PrefersTheSpanTierWhereverTreesCompile()
    {
        // The interpreter cannot hold a ReadOnlySpan<T>, so the span tier is only offered where a tree becomes IL.
        LegacyPocoScatterTier expected = RuntimeFeature.IsDynamicCodeCompiled ? LegacyPocoScatterTier.Span : LegacyPocoScatterTier.Indexer;

        Assert.That(LegacyPocoColumnScatterFactory.SelectTier(null, Ints("value", 1)), Is.EqualTo(expected));
    }

    [Test]
    public void SelectTier_ColumnThatBuildsItsValuesAndNoForcedTier_PrefersTheIndexerTier()
    {
        // Hoisting Values is the point of the span tier, but for a column that builds its values on access that one
        // read materializes every row of the block and pins it, which is what the windowed materialization avoids.
        var built = new StringColumn("value", "String", new byte[] { 0x61 }, new[] { 0, 1 }, 1, pooled: false);

        Assert.That(LegacyPocoColumnScatterFactory.SelectTier(null, built), Is.EqualTo(LegacyPocoScatterTier.Indexer));
    }

    [Test]
    public void SelectTier_ForcedTier_IsHonored()
    {
        // Forced over both column shapes: a forced tier outranks the column's own preference, which is what lets the
        // parity harness compile the span tier for a column that would otherwise choose the indexer.
        var built = new StringColumn("value", "String", new byte[] { 0x61 }, new[] { 0, 1 }, 1, pooled: false);

        Assert.Multiple(() =>
        {
            foreach (LegacyPocoScatterTier tier in Enum.GetValues<LegacyPocoScatterTier>())
            {
                Assert.That(LegacyPocoColumnScatterFactory.SelectTier(tier, Ints("value", 1)), Is.EqualTo(tier), $"{tier} over stored values");
                Assert.That(LegacyPocoColumnScatterFactory.SelectTier(tier, built), Is.EqualTo(tier), $"{tier} over built values");
            }
        });
    }

    private static IColumn Ints(string name, params int[] values) => PrimitiveColumn<int>.FromValues(name, "Int32", values);
}
