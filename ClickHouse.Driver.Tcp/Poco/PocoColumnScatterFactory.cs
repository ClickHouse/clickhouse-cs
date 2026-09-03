using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Copies one decoded column into one property across a range of POCO rows.
/// </summary>
/// <typeparam name="T">The POCO type.</typeparam>
/// <param name="column">The column to read.</param>
/// <param name="rows">The rows to scatter into; at least <paramref name="rowCount"/> long, each already constructed.</param>
/// <param name="start">The first column row to read; destination rows start at zero.</param>
/// <param name="rowCount">The number of rows to fill.</param>
/// <param name="rowOffset">The result-wide index of <c>rows[0]</c>, used in failures.</param>
internal delegate void PocoColumnScatter<in T>(IColumn column, T[] rows, int start, int rowCount, long rowOffset);

/// <summary>
/// Compiles a per-column loop with <see cref="PocoValueProjection"/> inlined into each assignment.
/// </summary>
internal static class PocoColumnScatterFactory
{
    private static readonly MethodInfo SpanAt = typeof(PocoSpan).GetMethod(nameof(PocoSpan.At), BindingFlags.Public | BindingFlags.Static);

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
    public static PocoColumnScatter<T> Create<T>(IColumn column, IColumnCodec codec, PocoMember member, PocoScatterTier? forcedTier, bool preferInterpretation = false)
        where T : class
    {
        Type elementType = codec.ElementType;
        Type typedColumn = typeof(IColumn<>).MakeGenericType(elementType);
        if (!typedColumn.IsInstanceOfType(column))
        {
            throw NotSurfacingItsElementType(column, codec);
        }

        PocoScatterTier tier = SelectTier(forcedTier, column);

        ParameterExpression columnParameter = Expression.Parameter(typeof(IColumn), "column");
        ParameterExpression rows = Expression.Parameter(typeof(T[]), "rows");
        ParameterExpression start = Expression.Parameter(typeof(int), "start");
        ParameterExpression rowCount = Expression.Parameter(typeof(int), "rowCount");
        ParameterExpression rowOffset = Expression.Parameter(typeof(long), "rowOffset");
        ParameterExpression row = Expression.Variable(typeof(int), "row");
        ParameterExpression value = Expression.Variable(elementType, "value");

        var site = new PocoProjectionSite
        {
            ColumnName = column.Name,
            ColumnType = column.TypeName,
            PocoTypeName = typeof(T).Name,
            MemberName = member.MemberName,
            Row = Expression.Add(rowOffset, Expression.Convert(row, typeof(long))),
        };

        if (!PocoValueProjection.TryResolve(codec, value, member.MemberType, site, out Expression projected))
        {
            throw NotReadableAs(column, codec, member, typeof(T));
        }

        var locals = new List<ParameterExpression>(3) { row };
        var body = new List<Expression>(4);
        Expression source = SourceOneValue(tier, columnParameter, typedColumn, elementType, Expression.Add(start, row), locals, body);

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

        return Expression.Lambda<PocoColumnScatter<T>>(Expression.Block(locals, body), columnParameter, rows, start, rowCount, rowOffset)
            .Compile(preferInterpretation);
    }

    /// <summary>
    /// Uses the forced tier, or selects spans only when dynamic code is compiled and values are already stored.
    /// </summary>
    /// <param name="forcedTier">A tier to use in place of the choice, or null to choose.</param>
    /// <param name="column">The column to be read, consulted for whether its values are stored or built.</param>
    /// <returns>The tier to compile.</returns>
    internal static PocoScatterTier SelectTier(PocoScatterTier? forcedTier, IColumn column)
        => forcedTier ?? (RuntimeFeature.IsDynamicCodeCompiled && column is IStoredValuesColumn
            ? PocoScatterTier.Span
            : PocoScatterTier.Indexer);

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
        PocoScatterTier tier,
        ParameterExpression column,
        Type typedColumn,
        Type elementType,
        Expression columnRow,
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
                return Expression.Call(SpanAt.MakeGenericMethod(elementType), values, columnRow);

            default:
                ParameterExpression typed = Expression.Variable(typedColumn, "typed");
                locals.Add(typed);
                prologue.Add(Expression.Assign(typed, Expression.Convert(column, typedColumn)));
                return Expression.MakeIndex(typed, typedColumn.GetProperty("Item", elementType, new[] { typeof(int) }), new[] { columnRow });
        }
    }

    /// <summary>
    /// Reports a column that does not implement <see cref="IColumn{T}"/> for its codec's element type.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <param name="codec">The column's codec.</param>
    /// <returns>The exception to throw.</returns>
    private static Exception NotSurfacingItsElementType(IColumn column, IColumnCodec codec)
        => new InvalidOperationException(
            $"Column '{column.Name}' ({column.TypeName}) was read as {column.GetType()}, which does not implement IColumn<{codec.ElementType}> " +
            $"as its codec {codec.GetType()} declares. A POCO read sources every value through that interface, so the column cannot be read into a property.");

    /// <summary>
    /// Reports a property type the column cannot be read as.
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
        // so that case gets its own remedy.
        string remedy = NamesTheNothingType(TypeParser.Parse(column.TypeName))
            ? "That column is an untyped NULL, so it carries no type to read as anything else: give it one in the query (for example CAST(NULL AS Nullable(String))), or exclude the property with [ClickHouseTcpNotMapped]."
            : "Give the property one of those types, exclude it with [ClickHouseTcpNotMapped], or read the column through the block-level API.";

        return new InvalidOperationException(
            $"Column '{column.Name}' ({column.TypeName}) maps to property '{pocoType.Name}.{member.MemberName}' of type {member.MemberType}, which it cannot be read as. " +
            $"It reads as {string.Join(" or ", offered)}. {remedy}");
    }

    /// <summary>
    /// Whether a parsed type contains a real <c>Nothing</c> node, excluding labels or field names with that text.
    /// </summary>
    /// <param name="node">The parsed column type.</param>
    /// <returns>Whether the type is, or contains, <c>Nothing</c>.</returns>
    private static bool NamesTheNothingType(TypeNode node)
    {
        if (string.Equals(node.Name, NothingColumnCodec.Instance.TypeName, StringComparison.Ordinal))
        {
            return true;
        }

        for (int i = 0; i < node.Arguments.Count; i++)
        {
            if (NamesTheNothingType(node.Arguments[i]))
            {
                return true;
            }
        }

        return false;
    }
}
