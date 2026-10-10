using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Types.Converters;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The tiers of <see cref="PocoColumnScatterFactory"/>: which tier a runtime gets, that the tiers read the same rows and
/// get their own plans, and that the tier of a runtime without dynamic code builds no expression from the converter
/// tree. <see cref="PocoReadPlanTests"/> and <see cref="PocoReadPlanFillTests"/> read through each tier.
/// </summary>
[TestFixture]
public class PocoColumnScatterFactoryTests
{
    [Test]
    public void SelectTier_NoForcedTier_PrefersTheCompiledLoopWhereverTreesCompile()
    {
        // The compiled loop holds span locals, which the expression interpreter cannot run, so a runtime that
        // interprets trees gets the tier with no compiled code.
        PocoScatterTier expected = RuntimeFeature.IsDynamicCodeCompiled ? PocoScatterTier.Emit : PocoScatterTier.Fill;

        Assert.That(PocoColumnScatterFactory.SelectTier(null), Is.EqualTo(expected));
    }

    [Test]
    public void SelectTier_ForcedTier_IsHonored()
    {
        Assert.Multiple(() =>
        {
            foreach (PocoScatterTier tier in Enum.GetValues<PocoScatterTier>())
            {
                Assert.That(PocoColumnScatterFactory.SelectTier(tier), Is.EqualTo(tier));
            }
        });
    }

    // The tiers are cases of one loop rather than [TestCase]s, because the enum is internal and a public test
    // method cannot take it as a parameter. Iterating the enum also covers a tier added later for free.
    [Test]
    public void Materialize_EveryTier_ProducesTheSameRows()
    {
        // The tiers differ only in how they run the converter tree, so they must agree, also on the conversions, which
        // is why the block mixes a raw type, a projected one, a nullable and a composite. This doubles as the proof
        // that the tier with no compiled code, which a runtime without dynamic code falls back to, is equivalent.
        Assert.Multiple(() =>
        {
            foreach (PocoScatterTier tier in Enum.GetValues<PocoScatterTier>())
            {
                PocoReadPlanTests.MixedRow[] rows = PocoReadPlanTests.Materialize<PocoReadPlanTests.MixedRow>(PocoReadPlanTests.MixedBlock(), tier);

                Assert.That(Array.ConvertAll(rows, row => row.Id), Is.EqualTo(new[] { 1, 2 }), $"{tier}: Id");
                Assert.That(Array.ConvertAll(rows, row => row.Name), Is.EqualTo(new[] { "a", "b" }), $"{tier}: Name");
                Assert.That(Array.ConvertAll(rows, row => row.Stamp), Is.EqualTo(new[] { DateTime.UnixEpoch.AddSeconds(1_700_000_000), DateTime.UnixEpoch }), $"{tier}: Stamp");
                Assert.That(Array.ConvertAll(rows, row => row.Score), Is.EqualTo(new double?[] { 1.5, null }), $"{tier}: Score");
                Assert.That(Array.ConvertAll(rows, row => row.Tags), Is.EqualTo(new[] { new[] { "x", "y" }, Array.Empty<string>() }), $"{tier}: Tags");
                Assert.That(Array.ConvertAll(rows, row => row.Level), Is.EqualTo(new[] { PocoReadPlanTests.Level.Low, PocoReadPlanTests.Level.High }), $"{tier}: Level");
            }
        });
    }

    [Test]
    public void ReadPlanFor_DifferentForcedTiers_CompileTheirOwnPlans()
    {
        var registry = new PocoTypeRegistry();
        Block block = PocoReadPlanTests.BlockOf(1, PocoReadPlanTests.Ints("value", 1));

        PocoReadPlan<Row<int>> emit = registry.ReadPlanFor<Row<int>>(block, PocoScatterTier.Emit);
        PocoReadPlan<Row<int>> fill = registry.ReadPlanFor<Row<int>>(block, PocoScatterTier.Fill);

        Assert.That(fill, Is.Not.SameAs(emit));
    }


    // A setter that throws on row 0 and a NULL on row 1 (or the other order): the first failure in row order wins in
    // each tier, also in the Fill tier, which reads the whole window before it calls a setter.
    [TestCase(1, null, "SetterFailure: the setter refuses 1")]
    [TestCase(null, 1, "InvalidOperationException: Column 'Value' (Nullable(Int32)) is NULL at row 100 of the result")]
    public void Materialize_SetterThatThrowsAndANull_ThrowsTheFirstFailureInRowOrderInEveryTier(int? first, int? second, string expected)
    {
        var failures = new List<string>();
        foreach ((string name, Func<Block, PocoReadPlan<RefusingRow>> build) in new (string, Func<Block, PocoReadPlan<RefusingRow>>)[]
        {
            ("Emit", block => PocoReadPlan<RefusingRow>.Build(PocoTypeDescriptor<RefusingRow>.Build(), block, PocoScatterTier.Emit)),
            ("Fill", block => PocoReadPlan<RefusingRow>.Build(PocoTypeDescriptor<RefusingRow>.Build(), block, PocoScatterTier.Fill)),
        })
        {
            using Block block = PocoReadPlanTests.BlockOf(2, PocoReadPlanTests.Decoded(new ArrayColumn<int?>("Value", "Nullable(Int32)", new[] { first, second })));
            Exception failure = Assert.Catch(() => build(block).Materialize(block, new RefusingRow[2], 0, 2, rowOffset: 100), name);
            failures.Add($"{name}: {failure.GetType().Name}: {failure.Message}");
        }

        Assert.Multiple(() =>
        {
            foreach (string failure in failures)
            {
                Assert.That(failure, Does.Contain(": " + expected));
            }
        });
    }

    [Test]
    public void ForReader_FillTier_ReadsWithoutTheExpressionOfTheTree()
    {
        // A runtime without dynamic code interprets an expression tree, and the interpreter cannot run the span locals
        // that the trees emit. So the Fill tier must not ask the tree for its expression.
        IColumn column = PrimitiveColumn<int>.FromValues("value", "Int32", new[] { 10, 11, 12, 13 });
        PocoMember member = PocoTypeDescriptor<Row<int>>.Build().Members[0];
        PocoColumnScatter<Row<int>> scatter = PocoColumnScatterFactory.ForReader<Row<int>>(new FillOnlyReader(), column, member, PocoScatterTier.Fill);
        var rows = new[] { new Row<int>(), new Row<int>() };

        scatter(column, rows, start: 1, rows.Length, rowOffset: 1);

        Assert.That(Array.ConvertAll(rows, row => row.Value), Is.EqualTo(new[] { 11, 12 }));
    }

    [Test]
    public void ForReader_EmitTier_CompilesTheExpressionOfTheTree()
    {
        IColumn column = PrimitiveColumn<int>.FromValues("value", "Int32", new[] { 10 });
        PocoMember member = PocoTypeDescriptor<Row<int>>.Build().Members[0];

        Assert.Throws<NotSupportedException>(() => PocoColumnScatterFactory.ForReader<Row<int>>(new FillOnlyReader(), column, member, PocoScatterTier.Emit));
    }

    [Test]
    public async Task ForReader_WindowsOfOneLowCardinalityColumn_ConvertTheDictionaryOnce()
    {
        // A POCO read runs the scatter once for each window of rows, and each run binds the tree again (Fill) or runs the
        // setup of the compiled loop again (Emit). The dictionary of the column is converted once for all the windows.
        string[] text = { "x", "y", "x", "z", "y" };
        using IColumn column = await ConverterHarness.DecodeAsync("LowCardinality(String)", new ArrayColumn<string>("value", "LowCardinality(String)", text));
        PocoMember member = PocoTypeDescriptor<Row<string>>.Build().Members[0];

        foreach (PocoScatterTier tier in Enum.GetValues<PocoScatterTier>())
        {
            var leaf = new DictionaryEntryCacheTests.CountingReader<string>(ConverterDerivation.Default.Reader<string>("String", ConverterHarness.Context));
            PocoColumnScatter<Row<string>> scatter = PocoColumnScatterFactory.ForReader<Row<string>>(
                new DictionaryReader<string>(leaf, DictionaryOrder.FirstEntry), column, member, tier);
            var values = new string[column.RowCount];
            for (int start = 0; start < column.RowCount; start += 2)
            {
                var window = new[] { new Row<string>(), new Row<string>() };
                int count = Math.Min(window.Length, column.RowCount - start);
                scatter(column, window, start, count, rowOffset: start);
                for (int i = 0; i < count; i++)
                {
                    values[start + i] = window[i].Value;
                }
            }

            Assert.That(values, Is.EqualTo(text), $"{tier}");
            Assert.That(leaf.Fills, Is.EqualTo(1), $"{tier}: bulk reads of the dictionary");
        }
    }

    /// <summary>A row whose setter refuses the value 1.</summary>
    internal sealed class RefusingRow
    {
        private int value;

        public int Value
        {
            get => value;
            set => this.value = value == 1 ? throw new SetterFailure("the setter refuses 1") : value;
        }
    }

    /// <summary>The failure of <see cref="RefusingRow.Value"/>.</summary>
    internal sealed class SetterFailure : Exception
    {
        public SetterFailure(string message)
            : base(message)
        {
        }
    }

    /// <summary>The <c>Int32</c> leaf, without an expression.</summary>
    private sealed class FillOnlyReader : ColumnReader<int>
    {
        private readonly ColumnReader<int> leaf = ConverterDerivation.Default.Reader<int>("Int32", default);

        public override BoundReader<int> Bind(IColumn column) => leaf.Bind(column);

        public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
            => throw new NotSupportedException("This reader has no expression.");
    }
}
