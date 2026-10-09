using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// Reads a decoded column through its indexer (<c>IColumn&lt;T&gt;[row]</c>), as its canonical type
/// <typeparamref name="T"/>: the reader of the types that read only as their canonical type (Variant, Dynamic, Nested,
/// QBit, the geo types and <c>Tuple()</c>). The decoded columns of Variant, Dynamic, Nested, QBit and the geo types make
/// each value when it is read, and their <see cref="IColumn{T}.Values"/> makes the values of all the rows into a cache
/// that the column keeps. Through the indexer, a read of some rows makes only the values of those rows. (A
/// <c>Tuple()</c> column stores its values.)
/// </summary>
/// <typeparam name="T">The canonical type of the column.</typeparam>
internal sealed class IndexedReader<T> : ColumnReader<T>
{
    /// <summary>The shared instance. The reader has no state.</summary>
    public static readonly IndexedReader<T> Instance = new();

    private static readonly PropertyInfo Indexer = typeof(IColumn<T>).GetProperty("Item", typeof(T), new[] { typeof(int) });

    private IndexedReader()
    {
    }

    /// <inheritdoc/>
    public override BoundReader<T> Bind(IColumn column) => new Bound(ColumnSurface.Of<IColumn<T>>(column));

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression typed = scope.Local(typeof(IColumn<T>), "typed", EmitScope.Surface<IColumn<T>>(column));
        return Expression.MakeIndex(typed, Indexer, new[] { row });
    }

    private sealed class Bound : BoundReader<T>
    {
        private readonly IColumn<T> column;

        public Bound(IColumn<T> column) => this.column = column;

        public override void Fill(int start, Span<T> destination)
        {
            if ((uint)start > (uint)column.RowCount || destination.Length > column.RowCount - start)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(start),
                    $"Rows [{start}, {start + (long)destination.Length}) lie outside the {column.RowCount} row(s) of column '{column.Name}'.");
            }

            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = column[start + i];
            }
        }
    }
}
