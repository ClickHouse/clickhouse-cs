using System;
using System.Collections.Generic;
using System.Reflection;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Transposes positional <c>object[]</c> rows into typed columns. Values select the codec's CLR write type, allowing
/// both convenience values such as <see cref="DateTime"/> and canonical values returned by an untyped read.
/// </summary>
internal static class UntypedRowColumns
{
    private static readonly MethodInfo CreateBuilderMethod =
        typeof(UntypedRowColumns).GetMethod(nameof(CreateBuilder), BindingFlags.NonPublic | BindingFlags.Static);

    /// <summary>Builds one column per target from the corresponding value in each row.</summary>
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
                IColumnCodec codec = schema.Codecs.Resolve(target.TypeName, schema.Context);
                Type writeType = ChooseWriteType(codec, target, rows, rowCount, built);

                // Invoke only constructs the builder, so fill errors are not reflection-wrapped.
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
    /// Chooses the target's compatible CLR write type from the first non-null value. An all-null column uses the
    /// target's preferred type.
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
            // Nullable<T> boxes as T, so compare the underlying type.
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

    /// <summary>Builds the typed column gather for one position in each row.</summary>
    /// <typeparam name="TWrite">The CLR type the target column is written in.</typeparam>
    /// <param name="name">The target column's name.</param>
    /// <param name="typeName">The target column's ClickHouse type.</param>
    /// <param name="index">The column's position in every row.</param>
    /// <param name="targetTakesNull">Whether the target column can carry a row with no value.</param>
    /// <returns>The builder.</returns>
    private static PocoColumnBuilder<object[]> CreateBuilder<TWrite>(string name, string typeName, int index, bool targetTakesNull)
    {
        // Both the target and the CLR write type must represent null.
        bool acceptsNull = targetTakesNull && default(TWrite) is null;

        // Nullable<T> values arrive boxed as T but can still be unboxed into Nullable<T>.
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

                // Report mixed types with the offending row instead of a bare unbox failure.
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
