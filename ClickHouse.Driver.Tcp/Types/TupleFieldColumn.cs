using System;
using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A lazy write-path view that projects one element position out of a flat column of <c>ValueTuple</c> rows, so a
/// child codec can write that element's column straight from the ergonomic tuple column without un-transposing all
/// elements into per-child buffers first. Each access boxes the row's tuple and its element (the accepted cost of
/// the tuple transpose today; a typed accessor would remove it). The view is read per element — the child columns
/// are strided through the tuples, so there is no contiguous span. It borrows the source: disposing it does nothing.
/// </summary>
/// <typeparam name="T">The element type at this position.</typeparam>
internal sealed class TupleFieldColumn<T> : IColumn<T>
{
    private readonly IColumn source;
    private readonly int fieldIndex;

    /// <summary>Initializes a projection of element <paramref name="fieldIndex"/> out of <paramref name="source"/>.</summary>
    /// <param name="typeName">The element type name the view reports (the child codec's own type).</param>
    /// <param name="source">The flat column of <c>ValueTuple</c> rows.</param>
    /// <param name="fieldIndex">The zero-based element position to project.</param>
    public TupleFieldColumn(string typeName, IColumn source, int fieldIndex)
    {
        TypeName = typeName;
        this.source = source;
        this.fieldIndex = fieldIndex;
    }

    /// <inheritdoc/>
    public string Name => source.Name;

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => source.RowCount;

    /// <inheritdoc/>
    public T this[int row] => (T)((ITuple)source.GetValue(row))[fieldIndex];

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>Not supported: the element is strided through the tuple rows, so there is no contiguous span.</summary>
    /// <exception cref="NotSupportedException">Always — read the view per element.</exception>
    public ReadOnlySpan<T> Values => throw new NotSupportedException($"{nameof(TupleFieldColumn<T>)} has no contiguous span; read it per element.");

    /// <inheritdoc/>
    public void Dispose()
    {
    }
}
