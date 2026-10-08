// SPIKE for ClickHouse/integrations#801. Not production code.
//
// The POCO tier over the same derivation as the columnar tier: per mapped column, bind the derived reader to
// the block's column, fill a window of property values in bulk, then scatter them with a compiled setter.
using System;
using System.Buffers;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Tcp.Types;

namespace WireConverterSpike;

internal sealed class ConverterPocoPlan<TRow>
    where TRow : class, new()
{
    private readonly Scatter[] scatters;

    private ConverterPocoPlan(Scatter[] scatters) => this.scatters = scatters;

    public static ConverterPocoPlan<TRow> Build(Block block)
    {
        var scatters = new Scatter[block.ColumnCount];
        for (int i = 0; i < scatters.Length; i++)
        {
            IColumn column = block[i];
            PropertyInfo property = typeof(TRow).GetProperty(column.Name);
            if (property is null)
            {
                continue;
            }

            Derivation derivation = ReadDerivation.Derive(column.TypeName, block.Context, property.PropertyType);
            if (!derivation.Succeeded)
            {
                throw new InvalidOperationException(derivation.Refusal);
            }

            Type scatter = typeof(Scatter<>).MakeGenericType(typeof(TRow), property.PropertyType);
            scatters[i] = (Scatter)Activator.CreateInstance(scatter, derivation.Reader, property);
        }

        return new ConverterPocoPlan<TRow>(scatters);
    }

    public void Materialize(Block block, TRow[] rows, int start, int count)
    {
        for (int i = 0; i < count; i++)
        {
            rows[i] = new TRow();
        }

        for (int i = 0; i < scatters.Length; i++)
        {
            scatters[i]?.Run(block[i], rows, start, count);
        }
    }

    private abstract class Scatter
    {
        public abstract void Run(IColumn column, TRow[] rows, int start, int count);
    }

    private sealed class Scatter<TProp> : Scatter
    {
        private readonly ColumnReader<TProp> reader;
        private readonly Action<TRow[], TProp[], int> assign;
        private IColumn lastColumn;
        private BoundReader<TProp> lastBound;

        public Scatter(ColumnReader<TProp> reader, PropertyInfo property)
        {
            this.reader = reader;
            assign = CompileAssign(property);
        }

        public override void Run(IColumn column, TRow[] rows, int start, int count)
        {
            // Bind once per block, so a windowed read converts a LowCardinality dictionary once.
            if (!ReferenceEquals(column, lastColumn))
            {
                lastBound = reader.Bind(column);
                lastColumn = column;
            }

            TProp[] values = ArrayPool<TProp>.Shared.Rent(count);
            try
            {
                lastBound.Fill(start, values.AsSpan(0, count));
                assign(rows, values, count);
            }
            finally
            {
                ArrayPool<TProp>.Shared.Return(values, clearArray: !typeof(TProp).IsValueType);
            }
        }

        // for (i = 0; i < count; i++) rows[i].P = values[i];
        private static Action<TRow[], TProp[], int> CompileAssign(PropertyInfo property)
        {
            ParameterExpression rows = Expression.Parameter(typeof(TRow[]), "rows");
            ParameterExpression values = Expression.Parameter(typeof(TProp[]), "values");
            ParameterExpression count = Expression.Parameter(typeof(int), "count");
            ParameterExpression i = Expression.Variable(typeof(int), "i");
            LabelTarget end = Expression.Label();
            Expression loop = Expression.Block(
                new[] { i },
                Expression.Assign(i, Expression.Constant(0)),
                Expression.Loop(
                    Expression.IfThenElse(
                        Expression.LessThan(i, count),
                        Expression.Block(
                            Expression.Assign(
                                Expression.Property(Expression.ArrayIndex(rows, i), property),
                                Expression.ArrayIndex(values, i)),
                            Expression.PostIncrementAssign(i)),
                        Expression.Break(end)),
                    end));
            return Expression.Lambda<Action<TRow[], TProp[], int>>(loop, rows, values, count).Compile();
        }
    }
}
