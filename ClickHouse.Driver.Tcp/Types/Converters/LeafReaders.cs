using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// A read leaf over a decoded column that stores one value of <typeparamref name="TCanon"/> for each row (its
/// <see cref="IColumn{T}.Values"/> span): the integers, the floats, <c>Bool</c>, the dates and times, <c>UUID</c>,
/// the IP addresses, the decimals, the enums and <c>Nothing</c>.
/// </summary>
/// <typeparam name="TCanon">The value that the decoded column stores.</typeparam>
/// <typeparam name="T">The CLR type that the leaf gives.</typeparam>
/// <typeparam name="TConv">The conversion from <typeparamref name="TCanon"/> to <typeparamref name="T"/>.</typeparam>
internal sealed class ValueLeafReader<TCanon, T, TConv> : ColumnReader<T>
    where TConv : struct, IReadConversion<TCanon, T>
{
    private static readonly PropertyInfo ValuesProperty = typeof(IColumn<TCanon>).GetProperty(nameof(IColumn<TCanon>.Values));

    private readonly TConv conversion;

    /// <summary>Initializes a leaf with its conversion.</summary>
    /// <param name="conversion">The conversion, with the state of the column type (for example its timezone).</param>
    public ValueLeafReader(TConv conversion) => this.conversion = conversion;

    /// <inheritdoc/>
    public override BoundReader<T> Bind(IColumn column) => new Bound(ColumnSurface.Of<IColumn<TCanon>>(column), conversion);

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression values = scope.Local(
            typeof(ReadOnlySpan<TCanon>),
            "values",
            Expression.Property(EmitScope.Surface<IColumn<TCanon>>(column), ValuesProperty));
        return conversion.Emit(EmitScope.ElementAt(values, row));
    }

    private sealed class Bound : BoundReader<T>
    {
        private readonly IColumn<TCanon> column;
        private readonly TConv conversion;

        public Bound(IColumn<TCanon> column, TConv conversion)
        {
            this.column = column;
            this.conversion = conversion;
        }

        public override void Fill(int start, Span<T> destination)
        {
            ReadOnlySpan<TCanon> source = column.Values.Slice(start, destination.Length);

            // The identity conversion has TCanon equal to T, so the stored span is already the result. The type
            // test depends only on the type arguments.
            if (typeof(TConv) == typeof(Identity<TCanon>))
            {
                MemoryMarshal.CreateReadOnlySpan(ref Unsafe.As<TCanon, T>(ref MemoryMarshal.GetReference(source)), source.Length)
                    .CopyTo(destination);
                return;
            }

            TConv convert = conversion;
            for (int i = 0; i < source.Length; i++)
            {
                destination[i] = convert.Convert(source[i]);
            }
        }
    }
}

/// <summary>
/// A read leaf over a decoded column that stores the bytes of all rows end to end, with an offset for each row
/// (<see cref="IStringColumn"/>): <c>String</c> and <c>JSON</c>.
/// </summary>
/// <typeparam name="T">The CLR type that the leaf gives.</typeparam>
/// <typeparam name="TConv">The conversion from the bytes of one row to <typeparamref name="T"/>.</typeparam>
internal sealed class StringLeafReader<T, TConv> : ColumnReader<T>
    where TConv : struct, IBytesReadConversion<T>
{
    /// <summary>The shared instance. The leaf has no state.</summary>
    public static readonly StringLeafReader<T, TConv> Instance = new();

    private static readonly MethodInfo ReadAtMethod =
        typeof(StringLeafReader<T, TConv>).GetMethod(nameof(ReadAt), BindingFlags.NonPublic | BindingFlags.Static);

    private static readonly MethodInfo TextMethod =
        typeof(StringLeafReader<T, TConv>).GetMethod(nameof(Text), BindingFlags.NonPublic | BindingFlags.Static);

    private StringLeafReader()
    {
    }

    /// <inheritdoc/>
    public override BoundReader<T> Bind(IColumn column) => new Bound(Text(column));

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression text = scope.Local(typeof(IStringColumn), "text", Expression.Call(TextMethod, Expression.Convert(column, typeof(IColumn))));
        ParameterExpression bytes = scope.Local(typeof(ReadOnlySpan<byte>), "bytes", Expression.Property(text, nameof(IStringColumn.Bytes)));
        ParameterExpression offsets = scope.Local(typeof(ReadOnlySpan<int>), "offsets", Expression.Property(text, nameof(IStringColumn.Offsets)));
        return Expression.Call(ReadAtMethod, bytes, offsets, row);
    }

    // The column's bytes and offsets. A byte[] reading of a column without them fails with a message that says where such
    // a column comes from (StringColumnCodec.NoWireBytes).
    private static IStringColumn Text(IColumn column)
        => column as IStringColumn
            ?? (typeof(T) == typeof(byte[]) ? throw StringColumnCodec.NoWireBytes(column) : ColumnSurface.Of<IStringColumn>(column));

    // Reads one row. The offsets span has one more entry than the column has rows, so a row past the end fails on it.
    private static T ReadAt(ReadOnlySpan<byte> bytes, ReadOnlySpan<int> offsets, int row)
    {
        int from = offsets[row];
        return default(TConv).Convert(bytes.Slice(from, offsets[row + 1] - from));
    }

    private sealed class Bound : BoundReader<T>
    {
        private readonly IStringColumn column;

        public Bound(IStringColumn column) => this.column = column;

        public override void Fill(int start, Span<T> destination)
        {
            ReadOnlySpan<byte> bytes = column.Bytes;
            ReadOnlySpan<int> offsets = column.Offsets.Slice(start, destination.Length + 1);
            TConv convert = default;
            for (int i = 0; i < destination.Length; i++)
            {
                int from = offsets[i];
                destination[i] = convert.Convert(bytes.Slice(from, offsets[i + 1] - from));
            }
        }
    }
}

/// <summary>
/// A read leaf over a <c>FixedString(N)</c> column. A decoded column (<see cref="FixedStringColumn"/>) stores the
/// <c>N</c> bytes of each row end to end, and the leaf reads them there. A column that a caller built gives one array
/// for each row through its indexer.
/// </summary>
/// <typeparam name="T">The CLR type that the leaf gives.</typeparam>
/// <typeparam name="TConv">The conversion from the bytes of one row to <typeparamref name="T"/>.</typeparam>
internal sealed class FixedStringLeafReader<T, TConv> : ColumnReader<T>
    where TConv : struct, IBytesReadConversion<T>
{
    /// <summary>The shared instance. The leaf has no state: each column states its own width.</summary>
    public static readonly FixedStringLeafReader<T, TConv> Instance = new();

    private static readonly MethodInfo ReadDenseMethod =
        typeof(FixedStringLeafReader<T, TConv>).GetMethod(nameof(ReadDense), BindingFlags.NonPublic | BindingFlags.Static);

    private static readonly MethodInfo ReadIndexedMethod =
        typeof(FixedStringLeafReader<T, TConv>).GetMethod(nameof(ReadIndexed), BindingFlags.NonPublic | BindingFlags.Static);

    private FixedStringLeafReader()
    {
    }

    /// <inheritdoc/>
    public override BoundReader<T> Bind(IColumn column) => column is FixedStringColumn dense
        ? new DenseBound(dense)
        : new IndexedBound(ColumnSurface.Of<IColumn<byte[]>>(column));

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression rows = scope.Local(typeof(IColumn<byte[]>), "rows", EmitScope.Surface<IColumn<byte[]>>(column));
        ParameterExpression dense = scope.Local(typeof(FixedStringColumn), "dense", Expression.TypeAs(rows, typeof(FixedStringColumn)));
        return Expression.Condition(
            Expression.ReferenceNotEqual(dense, Expression.Constant(null, typeof(FixedStringColumn))),
            Expression.Call(ReadDenseMethod, dense, row),
            Expression.Call(ReadIndexedMethod, rows, row));
    }

    private static T ReadDense(FixedStringColumn column, int row) => default(TConv).Convert(column.GetBytes(row));

    private static T ReadIndexed(IColumn<byte[]> column, int row) => default(TConv).Convert(column[row]);

    private sealed class DenseBound : BoundReader<T>
    {
        private readonly FixedStringColumn column;

        public DenseBound(FixedStringColumn column) => this.column = column;

        public override void Fill(int start, Span<T> destination)
        {
            int size = column.Size;
            ReadOnlySpan<byte> rows = column.GetBytes(start, destination.Length);
            TConv convert = default;
            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = convert.Convert(rows.Slice(i * size, size));
            }
        }
    }

    private sealed class IndexedBound : BoundReader<T>
    {
        private readonly IColumn<byte[]> column;

        public IndexedBound(IColumn<byte[]> column) => this.column = column;

        public override void Fill(int start, Span<T> destination)
        {
            if ((uint)start > (uint)column.RowCount || destination.Length > column.RowCount - start)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(start),
                    $"Rows [{start}, {start + (long)destination.Length}) lie outside the {column.RowCount} row(s) of column '{column.Name}'.");
            }

            TConv convert = default;
            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = convert.Convert(column[start + i]);
            }
        }
    }
}
