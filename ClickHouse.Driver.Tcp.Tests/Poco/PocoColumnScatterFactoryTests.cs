using System;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The tiers of <see cref="PocoColumnScatterFactory"/>: which tier a runtime gets, and that the tier of a runtime
/// without dynamic code builds no expression from the converter tree. <see cref="PocoReadPlanTests"/> and
/// <see cref="PocoReadPlanFillTests"/> read through each tier.
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

    /// <summary>The <c>Int32</c> leaf, without an expression.</summary>
    private sealed class FillOnlyReader : ColumnReader<int>
    {
        private readonly ColumnReader<int> leaf = ConverterDerivation.Default.Reader<int>("Int32", default);

        public override BoundReader<int> Bind(IColumn column) => leaf.Bind(column);

        public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
            => throw new NotSupportedException("This reader has no expression.");
    }
}
