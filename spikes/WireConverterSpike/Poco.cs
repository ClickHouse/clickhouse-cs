// SPIKE for ClickHouse/integrations#801. Not production code.
//
// The POCO tier over the same derivation as the columnar tier: per mapped column, bind the derived reader to
// the block's column, fill a window of property values in bulk, then scatter them with a compiled setter.
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Tcp.Types;

namespace WireConverterSpike;

/// <summary>
/// A compiled constructor, as the current plan uses. A generic <c>new TRow()</c> compiles to
/// <c>Activator.CreateInstance&lt;TRow&gt;()</c>, which is slower and showed up as a 10% to 20% gap on
/// single-column reads.
/// </summary>
internal static class PocoActivator<TRow>
    where TRow : class, new()
{
    public static readonly Func<TRow> Create =
        Environment.GetEnvironmentVariable("SPIKE_NEW") == "generic"
            ? static () => new TRow()
            : Expression.Lambda<Func<TRow>>(Expression.New(typeof(TRow))).Compile();
}

internal sealed class ConverterPocoPlan<TRow>
    where TRow : class, new()
{
    private static readonly Func<TRow> Activate = PocoActivator<TRow>.Create;

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
        Func<TRow> create = Activate;
        for (int i = 0; i < count; i++)
        {
            rows[i] = create();
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

/// <summary>
/// The POCO tier with the derived tree compiled into the scatter loop (issue item 5): one compiled loop per
/// column, <c>for (i) { r = start + i; rows[i].P = &lt;tree at r&gt;; }</c>, with no buffer and one pass.
/// </summary>
internal sealed class FusedPocoPlan<TRow>
    where TRow : class, new()
{
    private static readonly Func<TRow> Activate = PocoActivator<TRow>.Create;

    private readonly Action<IColumn, TRow[], int, int>[] scatters;

    private FusedPocoPlan(Action<IColumn, TRow[], int, int>[] scatters) => this.scatters = scatters;

    public static FusedPocoPlan<TRow> Build(Block block)
    {
        var scatters = new Action<IColumn, TRow[], int, int>[block.ColumnCount];
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

            scatters[i] = (Action<IColumn, TRow[], int, int>)typeof(FusedPocoPlan<TRow>)
                .GetMethod(nameof(Compile), BindingFlags.NonPublic | BindingFlags.Static)
                .MakeGenericMethod(property.PropertyType)
                .Invoke(null, new[] { derivation.Reader, property });
        }

        return new FusedPocoPlan<TRow>(scatters);
    }

    public void Materialize(Block block, TRow[] rows, int start, int count)
    {
        Func<TRow> create = Activate;
        for (int i = 0; i < count; i++)
        {
            rows[i] = create();
        }

        for (int i = 0; i < scatters.Length; i++)
        {
            scatters[i]?.Invoke(block[i], rows, start, count);
        }
    }

    private static Action<IColumn, TRow[], int, int> Compile<TProp>(ColumnReader<TProp> reader, PropertyInfo property)
    {
        ParameterExpression column = Expression.Parameter(typeof(IColumn), "column");
        ParameterExpression rows = Expression.Parameter(typeof(TRow[]), "rows");
        ParameterExpression start = Expression.Parameter(typeof(int), "start");
        ParameterExpression count = Expression.Parameter(typeof(int), "count");
        ParameterExpression i = Expression.Variable(typeof(int), "i");
        ParameterExpression r = Expression.Variable(typeof(int), "r");

        var scope = new EmitScope();
        Expression value = reader.Emit(column, r, scope);

        LabelTarget end = Expression.Label();
        var body = new List<Expression>(scope.Prologue)
        {
            Expression.Assign(i, Expression.Constant(0)),
            Expression.Loop(
                Expression.IfThenElse(
                    Expression.LessThan(i, count),
                    Expression.Block(
                        Expression.Assign(r, Expression.Add(start, i)),
                        Expression.Assign(Expression.Property(Expression.ArrayIndex(rows, i), property), value),
                        Expression.PostIncrementAssign(i)),
                    Expression.Break(end)),
                end),
        };

        var locals = new List<ParameterExpression>(scope.Locals) { i, r };
        return Expression.Lambda<Action<IColumn, TRow[], int, int>>(Expression.Block(locals, body), column, rows, start, count).Compile();
    }
}
