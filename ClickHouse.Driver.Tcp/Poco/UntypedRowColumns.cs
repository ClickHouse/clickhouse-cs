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

    /// <summary>
    /// Opens one insert: a column per target, filled from the corresponding value in each row, a block at a time.
    /// </summary>
    /// <remarks>
    /// Each column's CLR write type is chosen once for the whole insert, from the first row that has a value for
    /// it, because one buffer serves every block. A later value of another type is reported when its block is
    /// gathered.
    /// </remarks>
    /// <param name="schema">The server's sample block, naming and typing the target columns.</param>
    /// <param name="rows">The insert's rows; not owned by the source.</param>
    /// <param name="blockRows">The most rows one wire block will hold.</param>
    /// <returns>The source, owning its gather buffers until it is disposed.</returns>
    /// <exception cref="InvalidOperationException">A value's CLR type is not one the target column accepts, or the
    /// target cannot be built from rows at all.</exception>
    public static PocoInsertSource<object[]> CreateSource(Block schema, PocoRowBuffer<object[]> rows, int blockRows)
    {
        int columnCount = schema.ColumnCount;
        var builders = new PocoColumnBuilder<object[]>[columnCount];

        for (int i = 0; i < columnCount; i++)
        {
            IColumn target = schema[i];
            IColumnCodec codec = schema.Codecs.Resolve(target.TypeName, schema.Context);
            Type writeType = ChooseWriteType(codec, target, rows, i);

            builders[i] = (PocoColumnBuilder<object[]>)CreateBuilderMethod
                .MakeGenericMethod(writeType)
                .Invoke(null, new object[] { target.Name, target.TypeName, i, PocoWriteConversion.TakesNull(codec) });
        }

        return new UntypedInsertSource(builders, rows, blockRows, columnCount, rows.ParameterName);
    }

    /// <summary>
    /// Chooses the target's compatible CLR write type from the first non-null value. An all-null column uses the
    /// target's preferred type.
    /// </summary>
    /// <param name="codec">The target column's codec.</param>
    /// <param name="target">The target column, for diagnostics.</param>
    /// <param name="rows">The insert's rows.</param>
    /// <param name="index">The column's position in every row.</param>
    /// <returns>The write type.</returns>
    /// <exception cref="InvalidOperationException">The values' type is not one the target accepts.</exception>
    private static Type ChooseWriteType(IColumnCodec codec, IColumn target, PocoRowBuffer<object[]> rows, int index)
    {
        IReadOnlyList<Type> accepted = PocoWriteConversion.AcceptedWriteTypes(codec);
        if (accepted.Count == 0)
        {
            throw new InvalidOperationException(
                $"The target column '{target.Name}' has type '{target.TypeName}', which cannot be built from rows: insert it through the columnar API, which can build the column shape it needs.");
        }

        Type present = null;
        for (int row = 0; row < rows.Count && present is null; row++)
        {
            // A null or short row is reported when its block is gathered; here it simply holds no value.
            object[] values = rows.RowAt(row);
            present = values is not null && index < values.Length ? values[index]?.GetType() : null;
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
        return new PocoColumnBuilder<object[], TWrite>(name, typeName, (source, start, rowNumber, count, destination) =>
        {
            for (int slot = 0; slot < count; slot++)
            {
                object value = source[start + slot][index];
                if (value is null)
                {
                    destination[slot] = acceptsNull
                        ? default
                        : throw new InvalidOperationException(
                            $"Column {index} ('{name}', {typeName}) is null at row {rowNumber + slot} of the insert, but it cannot hold null. " +
                            $"Make the column Nullable(...), or leave out the rows with no value.");
                    continue;
                }

                // Report mixed types with the offending row instead of a bare unbox failure.
                if (!expected.IsInstanceOfType(value))
                {
                    throw new InvalidOperationException(
                        $"Column {index} ('{name}', {typeName}) is written as {typeof(TWrite)}, but row {rowNumber + slot} holds a {value.GetType()}. " +
                        $"Every value of one column must have the same CLR type.");
                }

                destination[slot] = (TWrite)value;
            }
        });
    }

    /// <summary>
    /// Matches every row of a block to the target columns by position, before any column reads it.
    /// </summary>
    private sealed class UntypedInsertSource : PocoInsertSource<object[]>
    {
        private readonly int columnCount;
        private readonly string parameterName;

        public UntypedInsertSource(
            PocoColumnBuilder<object[]>[] builders,
            PocoRowBuffer<object[]> rows,
            int blockRows,
            int columnCount,
            string parameterName)
            : base(builders, rows, blockRows)
        {
            this.columnCount = columnCount;
            this.parameterName = parameterName;
        }

        /// <inheritdoc/>
        protected override void CheckBlock(object[][] window, int offset, int rowNumber, int count)
        {
            for (int i = 0; i < count; i++)
            {
                int length = window[offset + i].Length;
                if (length != columnCount)
                {
                    throw new ArgumentException(
                        $"Row {rowNumber + i} has {length} values, but the insert targets {columnCount} column(s) ({DescribeTargets()}). " +
                        $"Untyped rows are matched to the target columns by position, so every row must have one value per column.",
                        parameterName);
                }
            }
        }

        private string DescribeTargets()
        {
            var names = new string[Columns.Count];
            for (int i = 0; i < names.Length; i++)
            {
                names[i] = Columns[i].Name;
            }

            return string.Join(", ", names);
        }
    }
}
