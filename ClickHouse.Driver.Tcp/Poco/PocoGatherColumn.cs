using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// One target column's gather destination: a buffer rented once for an insert and re-presented as the values of
/// each wire block in turn. A compiled gather fills <see cref="Buffer"/>, then <see cref="Publish"/> takes the
/// rows it wrote as this column's own, so the buffer is reused for every block instead of one being rented per
/// block.
/// </summary>
/// <typeparam name="T">The CLR type the target column is written in.</typeparam>
internal sealed class PocoGatherColumn<T> : IColumn<T>, ISpanColumn<T>
{
    private T[] buffer;
    private int rowCount;

    /// <summary>Rents a buffer for one block's values.</summary>
    /// <param name="name">The target column's name.</param>
    /// <param name="typeName">The target column's ClickHouse type.</param>
    /// <param name="capacity">The most rows one block will hold.</param>
    public PocoGatherColumn(string name, string typeName, int capacity)
    {
        Name = name;
        TypeName = typeName;
        buffer = ArrayPool<T>.Shared.Rent(capacity);
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => rowCount;

    /// <summary>The buffer to gather into. It is longer than a block, so only the published rows are values.</summary>
    public T[] Buffer => buffer;

    /// <inheritdoc/>
    public ReadOnlySpan<T> Values => new(buffer, 0, rowCount);

    /// <inheritdoc/>
    ReadOnlySpan<T> ISpanColumn<T>.Span => Values;

    /// <inheritdoc/>
    // Index through the logical span: the buffer holds a whole block, so a direct buffer[row] would return the
    // previous block's value for an out-of-range row instead of throwing.
    public T this[int row] => Values[row];

    /// <inheritdoc/>
    public object GetValue(int row) => Values[row];

    /// <summary>Takes the first <paramref name="count"/> buffered values as this column's rows.</summary>
    /// <param name="count">The number of rows the gather filled.</param>
    public void Publish(int count) => rowCount = count;

    /// <inheritdoc/>
    public void Dispose()
    {
        rowCount = 0;
        if (buffer.Length != 0)
        {
            // Clear reference-bearing element types so a returned array does not pin what it last held.
            ArrayPool<T>.Shared.Return(buffer, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
        }

        buffer = Array.Empty<T>();
    }
}
