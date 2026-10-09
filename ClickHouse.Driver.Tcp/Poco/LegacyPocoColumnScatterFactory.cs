using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The reference path of the POCO read scatter for the differential tests (<see cref="PocoReadPlan{T}.BuildLegacy"/>):
/// it compiles a loop that fills one POCO property from either an elementwise conversion
/// (<see cref="PocoValueProjection"/>) or a projected column of the codec. Only the tests call it. The client reads
/// through <see cref="PocoColumnScatterFactory"/>.
/// </summary>
internal static class LegacyPocoColumnScatterFactory
{
    private static readonly MethodInfo SpanAt = typeof(PocoSpan).GetMethod(nameof(PocoSpan.At), BindingFlags.Public | BindingFlags.Static);

    private static readonly MethodInfo CacheFor = typeof(ProjectedViewCache).GetMethod(nameof(ProjectedViewCache.For));

    /// <summary>
    /// Compiles the scatter for one column into one property.
    /// </summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <param name="column">A column of the shape the plan was built for, for its name, type and runtime shape.</param>
    /// <param name="codec">The column's codec, resolved the way the read resolved it.</param>
    /// <param name="member">The property the column maps to; must be settable.</param>
    /// <param name="forcedTier">A tier to compile regardless of the runtime, or null to choose one.</param>
    /// <param name="preferInterpretation">Whether to interpret the expression tree; used to test dynamic-code-free runtimes.</param>
    /// <returns>The compiled scatter.</returns>
    /// <exception cref="InvalidOperationException">The column's values cannot be read as the property's type, or the
    /// column does not surface its codec's element type.</exception>
    public static PocoColumnScatter<T> Create<T>(IColumn column, IColumnCodec codec, PocoMember member, LegacyPocoScatterTier? forcedTier, bool preferInterpretation = false)
        where T : class
    {
        Type elementType = codec.ElementType;
        Type typedColumn = typeof(IColumn<>).MakeGenericType(elementType);
        if (!typedColumn.IsInstanceOfType(column))
        {
            throw PocoReadErrors.NotSurfacingItsElementType(column, codec);
        }

        ParameterExpression columnParameter = Expression.Parameter(typeof(IColumn), "column");
        ParameterExpression rows = Expression.Parameter(typeof(T[]), "rows");
        ParameterExpression start = Expression.Parameter(typeof(int), "start");
        ParameterExpression rowCount = Expression.Parameter(typeof(int), "rowCount");
        ParameterExpression rowOffset = Expression.Parameter(typeof(long), "rowOffset");
        ParameterExpression row = Expression.Variable(typeof(int), "row");
        Expression columnRow = Expression.Add(start, row);

        var site = new PocoProjectionSite
        {
            ColumnName = column.Name,
            ColumnType = column.TypeName,
            PocoTypeName = typeof(T).Name,
            MemberName = member.MemberName,
            Row = Expression.Add(rowOffset, Expression.Convert(row, typeof(long))),
        };

        var locals = new List<ParameterExpression>(3) { row };
        var body = new List<Expression>(4);

        // Cache column-level projections across materialization windows so dictionaries and child columns are
        // converted once per source column.
        Expression assign;
        if (member.MemberType != elementType
            && codec.TryProjectColumnRead(member.MemberType, out ColumnReadProjection projection))
        {
            Type typedView = typeof(IColumn<>).MakeGenericType(member.MemberType);
            ParameterExpression view = Expression.Variable(typedView, "view");
            locals.Add(view);
            body.Add(Expression.Assign(
                view,
                Expression.Convert(
                    Expression.Call(Expression.Constant(new ProjectedViewCache(projection)), CacheFor, columnParameter),
                    typedView)));

            assign = Expression.Assign(
                Expression.Property(Expression.ArrayIndex(rows, row), member.Property),
                Expression.MakeIndex(view, typedView.GetProperty("Item", member.MemberType, new[] { typeof(int) }), new[] { columnRow }));
        }
        else
        {
            ParameterExpression value = Expression.Variable(elementType, "value");
            if (!PocoValueProjection.TryResolve(codec, value, member.MemberType, site, out Expression projected))
            {
                throw PocoReadErrors.NotReadableAs(column, codec, member, typeof(T));
            }

            Expression source = SourceOneValue(SelectTier(forcedTier, column), columnParameter, typedColumn, elementType, columnRow, locals, body);
            assign = Expression.Block(
                new[] { value },
                Expression.Assign(value, source),
                Expression.Assign(Expression.Property(Expression.ArrayIndex(rows, row), member.Property), projected));
        }

        // row = 0; while (row < rowCount) { <assign>; row++; }
        LabelTarget done = Expression.Label("done");
        body.Add(Expression.Assign(row, Expression.Constant(0)));
        body.Add(Expression.Loop(
            Expression.IfThenElse(
                Expression.LessThan(row, rowCount),
                Expression.Block(assign, Expression.PostIncrementAssign(row)),
                Expression.Break(done)),
            done));

        return Expression.Lambda<PocoColumnScatter<T>>(Expression.Block(locals, body), columnParameter, rows, start, rowCount, rowOffset)
            .Compile(preferInterpretation);
    }

    /// <summary>
    /// Uses the forced tier, or selects spans only when dynamic code is compiled and values are already stored.
    /// </summary>
    /// <param name="forcedTier">A tier to use in place of the choice, or null to choose.</param>
    /// <param name="column">The column to be read, consulted for whether its values are stored or built.</param>
    /// <returns>The tier to compile.</returns>
    internal static LegacyPocoScatterTier SelectTier(LegacyPocoScatterTier? forcedTier, IColumn column)
        => forcedTier ?? (RuntimeFeature.IsDynamicCodeCompiled && column is IStoredValuesColumn
            ? LegacyPocoScatterTier.Span
            : LegacyPocoScatterTier.Indexer);

    /// <summary>
    /// Builds one indexed read and adds its hoisted locals and prologue to the enclosing expression block.
    /// </summary>
    /// <param name="tier">The tier to source through.</param>
    /// <param name="column">The scatter's column parameter.</param>
    /// <param name="typedColumn">The <see cref="IColumn{T}"/> type over the codec's element type.</param>
    /// <param name="elementType">The codec's element type.</param>
    /// <param name="columnRow">The row of the column to read: the loop counter rebased by the scatter's start.</param>
    /// <param name="locals">The enclosing block's locals, added to by both tiers.</param>
    /// <param name="prologue">The statements before the loop, added to by both tiers.</param>
    /// <returns>An expression of type <paramref name="elementType"/>.</returns>
    private static Expression SourceOneValue(
        LegacyPocoScatterTier tier,
        ParameterExpression column,
        Type typedColumn,
        Type elementType,
        Expression columnRow,
        List<ParameterExpression> locals,
        List<Expression> prologue)
    {
        switch (tier)
        {
            case LegacyPocoScatterTier.Span:
                // The span is read once: IColumn<T>.Values recomputes it per access, and it cannot be cached in a
                // field, so hoisting it into a local is the whole point of the tier. For a jagged column
                // (Array/Map/Nested) Values materializes the block's rows into a cache, which the indexer would do
                // per row instead: the same work either way, plus one array of references here.
                ParameterExpression values = Expression.Variable(typeof(ReadOnlySpan<>).MakeGenericType(elementType), "values");
                locals.Add(values);
                prologue.Add(Expression.Assign(values, Expression.Property(Expression.Convert(column, typedColumn), "Values")));
                return Expression.Call(SpanAt.MakeGenericMethod(elementType), values, columnRow);

            default:
                ParameterExpression typed = Expression.Variable(typedColumn, "typed");
                locals.Add(typed);
                prologue.Add(Expression.Assign(typed, Expression.Convert(column, typedColumn)));
                return Expression.MakeIndex(typed, typedColumn.GetProperty("Item", elementType, new[] { typeof(int) }), new[] { columnRow });
        }
    }
}
