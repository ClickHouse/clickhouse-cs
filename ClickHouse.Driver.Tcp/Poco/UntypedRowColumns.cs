using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Chooses the CLR write type of one target column of an untyped row insert.
/// </summary>
/// <param name="codec">The target column's codec.</param>
/// <param name="target">The target column, for its name and type.</param>
/// <param name="rows">The insert's rows.</param>
/// <param name="index">The column's position in every row.</param>
/// <returns>The write type.</returns>
/// <exception cref="InvalidOperationException">The values' type is not one the target accepts.</exception>
internal delegate Type UntypedWriteTypeChooser(IColumnCodec codec, IColumn target, PocoRowBuffer<object[]> rows, int index);

/// <summary>
/// Transposes positional <c>object[]</c> rows into typed columns. The CLR type of the values selects each column's
/// write type, through the converter derivation, so convenience values such as <see cref="DateTime"/> and canonical
/// values returned by an untyped read are both written.
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
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public static PocoInsertSource<object[]> CreateSource(Block schema, PocoRowBuffer<object[]> rows, int blockRows)
    {
        ConverterDerivation derivation = schema.Codecs.Converters;
        ResolveContext context = schema.Context;
        return CreateSource(schema, rows, blockRows, (codec, target, values, index) => ChooseWriteType(derivation, context, codec, target, values, index));
    }

    /// <summary>Opens one insert, with the write type of each column from <paramref name="choose"/>.</summary>
    /// <param name="schema">The server's sample block, naming and typing the target columns.</param>
    /// <param name="rows">The insert's rows; not owned by the source.</param>
    /// <param name="blockRows">The most rows one wire block will hold.</param>
    /// <param name="choose">Chooses the CLR write type of each target column.</param>
    /// <returns>The source, owning its gather buffers until it is disposed.</returns>
    internal static PocoInsertSource<object[]> CreateSource(Block schema, PocoRowBuffer<object[]> rows, int blockRows, UntypedWriteTypeChooser choose)
    {
        int columnCount = schema.ColumnCount;
        var builders = new PocoColumnBuilder<object[]>[columnCount];

        for (int i = 0; i < columnCount; i++)
        {
            IColumn target = schema[i];
            IColumnCodec codec = schema.Codecs.Resolve(target.TypeName, schema.Context);
            Type writeType = choose(codec, target, rows, i);

            builders[i] = (PocoColumnBuilder<object[]>)CreateBuilderMethod
                .MakeGenericMethod(writeType)
                .Invoke(null, new object[] { target.Name, target.TypeName, i, PocoWriteConversion.TakesNull(codec) });
        }

        return new UntypedInsertSource(builders, rows, blockRows, columnCount, rows.ParameterName);
    }

    /// <summary>The CLR type of the first value of a column that is not null, or null when every value is null.</summary>
    /// <param name="rows">The insert's rows.</param>
    /// <param name="index">The column's position in every row.</param>
    /// <returns>The type, or null.</returns>
    internal static Type FirstValueType(PocoRowBuffer<object[]> rows, int index)
    {
        Type present = null;
        for (int row = 0; row < rows.Count && present is null; row++)
        {
            // A null or short row is reported when its block is gathered; here it simply holds no value.
            object[] values = rows.RowAt(row);
            present = values is not null && index < values.Length ? values[index]?.GetType() : null;
        }

        return present;
    }

    // The write type of a column, from its first value that is not null. A target that no CLR type of values fills is
    // reported before the values are read.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static Type ChooseWriteType(ConverterDerivation derivation, in ResolveContext context, IColumnCodec codec, IColumn target, PocoRowBuffer<object[]> rows, int index)
    {
        RequireBuildableFromRows(derivation, in context, codec, target);
        return WriteTypeFor(derivation, in context, codec, target, index, FirstValueType(rows, index));
    }

    /// <summary>
    /// Chooses the target's CLR write type for values of <paramref name="present"/>: the type that the converter
    /// derivation writes the values as. A cast rule gives the type it writes them as (<see cref="object"/> for a
    /// Variant), and a value type into a type that holds NULL is written as its nullable type, so a NULL row stays
    /// NULL. An all-null column uses the target's canonical type.
    /// </summary>
    /// <param name="derivation">The converter derivation of the sample block.</param>
    /// <param name="context">The resolution context of the sample block.</param>
    /// <param name="codec">The target column's codec.</param>
    /// <param name="target">The target column, for diagnostics.</param>
    /// <param name="index">The column's position in every row, for diagnostics.</param>
    /// <param name="present">The CLR type of the first value that is not null, or null when every value is null.</param>
    /// <returns>The write type.</returns>
    /// <exception cref="InvalidOperationException">The values' type is not one the target accepts, or no CLR type of
    /// values fills the target.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    internal static Type WriteTypeFor(ConverterDerivation derivation, in ResolveContext context, IColumnCodec codec, IColumn target, int index, Type present)
    {
        RequireBuildableFromRows(derivation, in context, codec, target);
        if (present is null)
        {
            return codec.ElementType;
        }

        Derivation derived = derivation.Derive(target.TypeName, in context, present, ConversionDirection.Write);
        if (!derived.Succeeded)
        {
            throw PocoWriteErrors.ValuesNotWritable(index, target, codec, present);
        }

        return derived.Converter is ICastWriter cast
            ? cast.TargetType
            : present.IsValueType && PocoWriteConversion.TakesNull(codec) ? typeof(Nullable<>).MakeGenericType(present) : present;
    }

    // A type that is not written from its canonical CLR type is written only from a column shape of its own.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static void RequireBuildableFromRows(ConverterDerivation derivation, in ResolveContext context, IColumnCodec codec, IColumn target)
    {
        if (!derivation.Derive(target.TypeName, in context, codec.ElementType, ConversionDirection.Write).Succeeded)
        {
            throw PocoWriteErrors.NotBuildableFromRows(target);
        }
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
