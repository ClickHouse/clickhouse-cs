using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>Array(X)</c> read as <c>T[]</c>. Each row gets a new array, which the child fills with one bulk read of the
/// row's elements. An empty row gives <see cref="Array.Empty{T}"/>.
/// </summary>
/// <typeparam name="T">The CLR type of one element.</typeparam>
internal sealed class ArrayReader<T> : ColumnReader<T[]>
{
    private static readonly MethodInfo RowAtMethod =
        typeof(ArrayReader<T>).GetMethod(nameof(RowAt), BindingFlags.NonPublic | BindingFlags.Static);

    private static readonly MethodInfo BindMethod =
        typeof(ColumnReader<T>).GetMethod(nameof(ColumnReader<T>.Bind));

    private readonly ColumnReader<T> inner;

    /// <summary>Initializes the reader over the reader of the element type.</summary>
    /// <param name="inner">Reads the flat element column of the <c>Array</c> column.</param>
    public ArrayReader(ColumnReader<T> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override BoundReader<T[]> Bind(IColumn column)
    {
        IArrayColumn array = ColumnSurface.Of<IArrayColumn>(column);
        return new Bound(array, inner.Bind(array.Inner));
    }

    /// <inheritdoc/>
    // The setup binds the child to the flat element column, and each row is one bulk read of the child.
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression array = scope.Local(typeof(IArrayColumn), "array", EmitScope.Surface<IArrayColumn>(column));
        ParameterExpression elements = scope.Local(
            typeof(BoundReader<T>),
            "elements",
            Expression.Call(Expression.Constant(inner, typeof(ColumnReader<T>)), BindMethod, Expression.Property(array, nameof(IArrayColumn.Inner))));
        ParameterExpression offsets = scope.Local(typeof(ReadOnlySpan<int>), "offsets", Expression.Property(array, nameof(IArrayColumn.Offsets)));
        return Expression.Call(RowAtMethod, elements, offsets, row);
    }

    // The offsets span has one more entry than the column has rows: row r holds [offsets[r], offsets[r + 1]).
    private static T[] RowAt(BoundReader<T> elements, ReadOnlySpan<int> offsets, int row)
    {
        int from = offsets[row];
        int length = offsets[row + 1] - from;
        if (length == 0)
        {
            return Array.Empty<T>();
        }

        var values = new T[length];
        elements.Fill(from, values);
        return values;
    }

    private sealed class Bound : BoundReader<T[]>
    {
        private readonly IArrayColumn column;
        private readonly BoundReader<T> elements;

        public Bound(IArrayColumn column, BoundReader<T> elements)
        {
            this.column = column;
            this.elements = elements;
        }

        public override void Fill(int start, Span<T[]> destination)
        {
            ReadOnlySpan<int> offsets = column.Offsets.Slice(start, destination.Length + 1);
            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = RowAt(elements, offsets, i);
            }
        }
    }
}
