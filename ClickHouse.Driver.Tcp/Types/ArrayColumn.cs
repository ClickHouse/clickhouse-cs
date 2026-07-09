using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A column backed by a typed array, for element types that are not a direct reinterpret of the wire bytes —
/// <c>String</c> (variable-length) and converted types such as <c>DateTime</c>. <see cref="Values"/> is a
/// borrowed view over the array.
/// </summary>
/// <typeparam name="T">The CLR element type.</typeparam>
internal sealed class ArrayColumn<T> : IColumn<T>
{
    private readonly T[] values;
    private readonly int offset;
    private readonly int length;

    /// <summary>Initializes a column that takes ownership of the values array.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="values">The decoded values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="values"/> is null.</exception>
    public ArrayColumn(string name, string typeName, T[] values)
        : this(name, typeName, values, offset: 0, values?.Length ?? 0)
    {
    }

    private ArrayColumn(string name, string typeName, T[] values, int offset, int length)
    {
        Name = name;
        TypeName = typeName;
        this.values = values ?? throw new ArgumentNullException(nameof(values));
        this.offset = offset;
        this.length = length;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => length;

    /// <inheritdoc/>
    public ReadOnlySpan<T> Values => new ReadOnlySpan<T>(values, offset, length);

    /// <inheritdoc/>
    public T this[int row] => values[offset + row];

    /// <inheritdoc/>
    public object GetValue(int row) => values[offset + row];

    /// <inheritdoc/>
    public void Dispose()
    {
        // The values array is GC-managed, not pooled; nothing to release.
    }
}
