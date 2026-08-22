using System;
using System.Collections.Generic;
using System.Reflection;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Transposes boxed <c>object[]</c> rows into the insert's target columns — the dynamic tier, positional by the
/// sample block's order, which is the order of the statement's column list (D6e).
///
/// <para>
/// Unlike the POCO path there is no plan to cache: nothing here is compiled, and the CLR type each column is
/// written in is decided by the values rather than by a type's properties. That choice is what lets one
/// <c>object[]</c> row source serve two callers whose rows mean the same thing in different spellings — a
/// <c>DateTime</c> column takes hand-written <see cref="DateTime"/> values as readily as the raw epoch seconds the
/// untyped <em>read</em> produces, so a read-then-reinsert round trip needs no conversion by the caller.
/// </para>
/// </summary>
internal static class PocoUntypedColumns
{
    private static readonly MethodInfo CreateBuilderMethod =
        typeof(PocoUntypedColumns).GetMethod(nameof(CreateBuilder), BindingFlags.NonPublic | BindingFlags.Static);

    /// <summary>
    /// Builds one column per target, each filled from its own position in every row.
    /// </summary>
    /// <param name="schema">The server's sample block, naming and typing the target columns.</param>
    /// <param name="rows">The rows, each non-null; at least <paramref name="rowCount"/> long.</param>
    /// <param name="rowCount">The number of rows to insert.</param>
    /// <returns>The columns, each owning a pooled buffer it returns when disposed.</returns>
    /// <exception cref="ArgumentException">A row has the wrong number of values.</exception>
    /// <exception cref="InvalidOperationException">A value's CLR type is not one the target column accepts, or a
    /// value is null for a column that cannot hold null.</exception>
    public static IReadOnlyList<IColumn> Build(Block schema, object[][] rows, int rowCount)
    {
        int columnCount = schema.ColumnCount;
        for (int row = 0; row < rowCount; row++)
        {
            if (rows[row].Length != columnCount)
            {
                throw new ArgumentException(
                    $"Row {row} has {rows[row].Length} values, but the insert targets {columnCount} column(s) ({DescribeTargets(schema)}). " +
                    $"Untyped rows are matched to the target columns by position, so every row must have one value per column.",
                    nameof(rows));
            }
        }

        var columns = new IColumn[columnCount];
        int built = 0;
        try
        {
            for (; built < columnCount; built++)
            {
                IColumn target = schema[built];
                IColumnCodec codec = schema.Codecs.Resolve(target.TypeName, ResolveContext.ForWrite);
                Type writeType = ChooseWriteType(codec, target, rows, rowCount, built);

                // Reflection constructs the builder and no more: the fill runs outside the Invoke, so a bad value
                // surfaces as its own exception rather than wrapped in a TargetInvocationException.
                var builder = (PocoColumnBuilder<object[]>)CreateBuilderMethod
                    .MakeGenericMethod(writeType)
                    .Invoke(null, new object[] { target.Name, target.TypeName, built, PocoWriteConversion.TakesNull(codec) });

                columns[built] = builder.Build(rows, rowCount);
            }
        }
        catch
        {
            for (int i = 0; i < built; i++)
            {
                columns[i].Dispose();
            }

            throw;
        }

        return columns;
    }

    /// <summary>
    /// Picks the CLR type a column is written in: the one the values themselves are in, matched against the types
    /// the target accepts.
    ///
    /// <para>
    /// Decided by the first row that has a value, because a boxed value knows its own type and nothing else here
    /// does. A column of nothing but nulls falls back to the target's canonical type, which is right either way — a
    /// <c>Nullable</c> target takes the nulls, and a non-nullable one has to report them, which the fill does
    /// naming the row.
    /// </para>
    /// </summary>
    /// <param name="codec">The target column's codec.</param>
    /// <param name="target">The target column, for diagnostics.</param>
    /// <param name="rows">The rows.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="index">The column's position in every row.</param>
    /// <returns>The write type.</returns>
    /// <exception cref="InvalidOperationException">The values' type is not one the target accepts.</exception>
    private static Type ChooseWriteType(IColumnCodec codec, IColumn target, object[][] rows, int rowCount, int index)
    {
        IReadOnlyList<Type> accepted = PocoWriteConversion.AcceptedWriteTypes(codec);
        if (accepted.Count == 0)
        {
            throw new InvalidOperationException(
                $"The target column '{target.Name}' has type '{target.TypeName}', which cannot be built from rows: insert it through the columnar API, which can build the column shape it needs.");
        }

        Type present = null;
        for (int row = 0; row < rowCount && present is null; row++)
        {
            present = rows[row][index]?.GetType();
        }

        if (present is null)
        {
            return accepted[0];
        }

        for (int i = 0; i < accepted.Count; i++)
        {
            // A boxed value is never a boxed Nullable<T> — the CLR boxes the underlying value — so a nullable write
            // type is matched by the type it wraps.
            if ((Nullable.GetUnderlyingType(accepted[i]) ?? accepted[i]).IsAssignableFrom(present))
            {
                return accepted[i];
            }
        }

        var offered = new string[accepted.Count];
        for (int i = 0; i < accepted.Count; i++)
        {
            offered[i] = accepted[i].ToString();
        }

        throw new InvalidOperationException(
            $"Column {index} ('{target.Name}', {target.TypeName}) was given values of type {present}, which it cannot be written from. " +
            $"It accepts {string.Join(" or ", offered)}.");
    }

    /// <summary>
    /// Builds the column builder for one position of every row, now that the write type is a type argument. The
    /// same builder the POCO path uses, over an unboxing fill rather than a compiled gather.
    /// </summary>
    /// <typeparam name="TWrite">The CLR type the target column is written in.</typeparam>
    /// <param name="name">The target column's name.</param>
    /// <param name="typeName">The target column's ClickHouse type.</param>
    /// <param name="index">The column's position in every row.</param>
    /// <param name="targetTakesNull">Whether the target column can carry a row with no value.</param>
    /// <returns>The builder.</returns>
    private static PocoColumnBuilder<object[]> CreateBuilder<TWrite>(string name, string typeName, int index, bool targetTakesNull)
    {
        // Both halves are needed. A target with no NULL of its own cannot take one however the write type spells it
        // — a String column rejects a null string, which would otherwise reach the codec and fault mid-block — and a
        // bare value type has nowhere to put one even where the target would accept it.
        bool acceptsNull = targetTakesNull && default(TWrite) is null;

        // A boxed value is never a boxed Nullable<T>, so a value is tested against the type the write type wraps —
        // the same rule that chose the write type. The cast itself is fine either way: unboxing to Nullable<T> from
        // a boxed T is exactly what unbox.any does.
        Type expected = Nullable.GetUnderlyingType(typeof(TWrite)) ?? typeof(TWrite);
        return new PocoColumnBuilder<object[], TWrite>(name, typeName, (source, count, destination) =>
        {
            for (int row = 0; row < count; row++)
            {
                object value = source[row][index];
                if (value is null)
                {
                    destination[row] = acceptsNull
                        ? default
                        : throw new InvalidOperationException(
                            $"Column {index} ('{name}', {typeName}) is null at row {row} of the insert, but it cannot hold null. " +
                            $"Make the column Nullable(...), or leave out the rows with no value.");
                    continue;
                }

                // The write type was chosen from the first row that had a value, so a later row in another type is
                // the caller mixing types in one column. Reported by row rather than left to an unbox failure.
                if (!expected.IsInstanceOfType(value))
                {
                    throw new InvalidOperationException(
                        $"Column {index} ('{name}', {typeName}) is written as {typeof(TWrite)}, but row {row} holds a {value.GetType()}. " +
                        $"Every value of one column must have the same CLR type.");
                }

                destination[row] = (TWrite)value;
            }
        });
    }

    private static string DescribeTargets(Block schema)
    {
        var names = new string[schema.ColumnCount];
        for (int i = 0; i < names.Length; i++)
        {
            names[i] = schema[i].Name;
        }

        return string.Join(", ", names);
    }
}
