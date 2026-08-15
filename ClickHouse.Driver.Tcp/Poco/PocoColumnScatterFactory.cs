using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Copies one decoded column into one property of every row of a materialized block — the compiled unit of a POCO
/// read. Column-major rather than row-major: the block is already decoded, so the loop is taken over one column's
/// values, which keeps the cast and the span access out of the per-row work.
/// </summary>
/// <typeparam name="T">The POCO type.</typeparam>
/// <param name="column">The column to read, already known to the plan that built this delegate.</param>
/// <param name="rows">The rows to scatter into; at least <paramref name="rowCount"/> long, each already constructed.</param>
/// <param name="rowCount">The number of rows to fill.</param>
internal delegate void PocoColumnScatter<in T>(IColumn column, T[] rows, int rowCount);

/// <summary>
/// Compiles the per-column scatter delegates a <see cref="PocoReadPlan{T}"/> is made of. One delegate handles one
/// (column, property) pair for a whole block, with the value conversion (<see cref="PocoValueProjection"/>) inlined
/// into the loop rather than called through a delegate per row.
/// </summary>
internal static class PocoColumnScatterFactory
{
    private static readonly MethodInfo SpanAt = typeof(PocoSpan).GetMethod(nameof(PocoSpan.At), BindingFlags.Public | BindingFlags.Static);

    private static readonly MethodInfo ColumnGetValue = typeof(IColumn).GetMethod(nameof(IColumn.GetValue));

    /// <summary>
    /// Compiles the scatter for one column into one property.
    /// </summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <param name="column">A column of the shape the plan was built for, for its name, type and runtime shape.</param>
    /// <param name="codec">The column's codec, resolved the way the read resolved it.</param>
    /// <param name="member">The property the column maps to; must be settable.</param>
    /// <param name="forcedTier">A tier to compile regardless of the runtime, or null to choose one.</param>
    /// <param name="preferInterpretation">Compiles the tree to an interpreter rather than to IL. Not used in
    /// production — the runtime decides that — but it is the only way to exercise the path a runtime without dynamic
    /// code takes, which is the whole reason the span-free tiers exist.</param>
    /// <returns>The compiled scatter.</returns>
    /// <exception cref="InvalidOperationException">The column's values cannot be read as the property's type.</exception>
    public static PocoColumnScatter<T> Create<T>(IColumn column, IColumnCodec codec, PocoMember member, PocoScatterTier? forcedTier, bool preferInterpretation = false)
        where T : class
    {
        Type elementType = codec.ElementType;
        Type typedColumn = typeof(IColumn<>).MakeGenericType(elementType);
        PocoScatterTier tier = SelectTier(forcedTier, typedColumn.IsInstanceOfType(column));

        ParameterExpression columnParameter = Expression.Parameter(typeof(IColumn), "column");
        ParameterExpression rows = Expression.Parameter(typeof(T[]), "rows");
        ParameterExpression rowCount = Expression.Parameter(typeof(int), "rowCount");
        ParameterExpression row = Expression.Variable(typeof(int), "row");
        ParameterExpression value = Expression.Variable(elementType, "value");

        var site = new PocoProjectionSite
        {
            ColumnName = column.Name,
            ColumnType = column.TypeName,
            PocoTypeName = typeof(T).Name,
            MemberName = member.MemberName,
            Row = row,
        };

        if (!PocoValueProjection.TryResolve(codec, value, member.MemberType, site, out Expression projected))
        {
            throw NotReadableAs(column, codec, member, typeof(T));
        }

        var locals = new List<ParameterExpression>(3) { row };
        var body = new List<Expression>(4);
        Expression source = SourceOneValue(tier, columnParameter, typedColumn, elementType, row, locals, body);

        // row = 0; while (row < rowCount) { value = <source>; rows[row].P = <projected>; row++; }
        LabelTarget done = Expression.Label("done");
        body.Add(Expression.Assign(row, Expression.Constant(0)));
        body.Add(Expression.Loop(
            Expression.IfThenElse(
                Expression.LessThan(row, rowCount),
                Expression.Block(
                    new[] { value },
                    Expression.Assign(value, source),
                    Expression.Assign(Expression.Property(Expression.ArrayIndex(rows, row), member.Property), projected),
                    Expression.PostIncrementAssign(row)),
                Expression.Break(done)),
            done));

        return Expression.Lambda<PocoColumnScatter<T>>(Expression.Block(locals, body), columnParameter, rows, rowCount)
            .Compile(preferInterpretation);
    }

    /// <summary>
    /// Picks the tier to compile: the caller's, or the span tier wherever expression trees compile to IL. A tree
    /// holding a <c>ReadOnlySpan&lt;T&gt;</c> compiles but cannot be <em>interpreted</em>, which is the path taken
    /// when the runtime has no dynamic code (NativeAOT), so the span tier is not offered there.
    /// </summary>
    /// <param name="forcedTier">A tier to use in place of the choice, or null to choose. A column that does not
    /// surface its element type overrides it: neither typed tier can read one.</param>
    /// <param name="columnSurfacesElementType">Whether the column implements <see cref="IColumn{T}"/> over its
    /// codec's element type, which both typed tiers rely on.</param>
    /// <returns>The tier to compile.</returns>
    internal static PocoScatterTier SelectTier(PocoScatterTier? forcedTier, bool columnSurfacesElementType)
    {
        if (!columnSurfacesElementType)
        {
            return PocoScatterTier.Boxed;
        }

        return forcedTier ?? (RuntimeFeature.IsDynamicCodeCompiled ? PocoScatterTier.Span : PocoScatterTier.Indexer);
    }

    /// <summary>
    /// Builds the expression that yields the value at <paramref name="row"/>, adding whatever the tier hoists out
    /// of the loop (the span, or the cast column) to the enclosing block.
    /// </summary>
    /// <param name="tier">The tier to source through.</param>
    /// <param name="column">The scatter's column parameter.</param>
    /// <param name="typedColumn">The <see cref="IColumn{T}"/> type over the codec's element type.</param>
    /// <param name="elementType">The codec's element type.</param>
    /// <param name="row">The loop counter.</param>
    /// <param name="locals">The enclosing block's locals, added to by the typed tiers.</param>
    /// <param name="prologue">The statements before the loop, added to by the typed tiers.</param>
    /// <returns>An expression of type <paramref name="elementType"/>.</returns>
    private static Expression SourceOneValue(
        PocoScatterTier tier,
        ParameterExpression column,
        Type typedColumn,
        Type elementType,
        ParameterExpression row,
        List<ParameterExpression> locals,
        List<Expression> prologue)
    {
        switch (tier)
        {
            case PocoScatterTier.Span:
                // The span is read once: IColumn<T>.Values recomputes it per access, and it cannot be cached in a
                // field, so hoisting it into a local is the whole point of the tier. For a jagged column
                // (Array/Map/Nested) Values materializes the block's rows into a cache, which the indexer would do
                // per row instead — the same work either way, plus one array of references here.
                ParameterExpression values = Expression.Variable(typeof(ReadOnlySpan<>).MakeGenericType(elementType), "values");
                locals.Add(values);
                prologue.Add(Expression.Assign(values, Expression.Property(Expression.Convert(column, typedColumn), "Values")));
                return Expression.Call(SpanAt.MakeGenericMethod(elementType), values, row);

            case PocoScatterTier.Indexer:
                ParameterExpression typed = Expression.Variable(typedColumn, "typed");
                locals.Add(typed);
                prologue.Add(Expression.Assign(typed, Expression.Convert(column, typedColumn)));
                return Expression.MakeIndex(typed, typedColumn.GetProperty("Item", elementType, new[] { typeof(int) }), new[] { row });

            default:
                return Expression.Convert(Expression.Call(column, ColumnGetValue, row), elementType);
        }
    }

    /// <summary>
    /// The failure for a property the column cannot be read as (D6d): raised at plan build, so it names the shape
    /// rather than surfacing as a cast failure part-way through a result.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <param name="codec">The column's codec, for the types it can be read as.</param>
    /// <param name="member">The property that cannot be filled.</param>
    /// <param name="pocoType">The POCO type.</param>
    /// <returns>The exception to throw.</returns>
    private static Exception NotReadableAs(IColumn column, IColumnCodec codec, PocoMember member, Type pocoType)
    {
        IReadOnlyList<Type> readable = codec.ReadableElementTypes;
        var offered = new string[readable.Count];
        for (int i = 0; i < readable.Count; i++)
        {
            offered[i] = readable[i].ToString();
        }

        // A bare NULL or empty-array literal comes back as Nothing (or a composite of it): the column carries no type
        // of its own, so it reads only as object however nullable the property is. Changing the property cannot help,
        // and no other ClickHouse type name contains the word — hence the remedy is to type the column in the query.
        string remedy = column.TypeName.Contains("Nothing", StringComparison.Ordinal)
            ? "That column is an untyped NULL, so it carries no type to read as anything else: give it one in the query (for example CAST(NULL AS Nullable(String))), or exclude the property with [ClickHouseTcpNotMapped]."
            : "Give the property one of those types, exclude it with [ClickHouseTcpNotMapped], or read the column through the block-level API.";

        return new InvalidOperationException(
            $"Column '{column.Name}' ({column.TypeName}) maps to property '{pocoType.Name}.{member.MemberName}' of type {member.MemberType}, which it cannot be read as. " +
            $"It reads as {string.Join(" or ", offered)}. {remedy}");
    }
}
