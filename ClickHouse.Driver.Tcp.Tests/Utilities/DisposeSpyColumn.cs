using System;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// A minimal leaf <see cref="IColumn{T}"/> backed by an array that records how many times it was disposed. Used by
/// the composite codec tests to assert a borrowed child column is not disposed by a wrapper that only borrows it,
/// and that an owned child column is disposed exactly once.
/// </summary>
internal sealed class DisposeSpyColumn<T> : IColumn<T>
{
    private readonly T[] values;

    public DisposeSpyColumn(string name, string typeName, T[] values)
    {
        Name = name;
        TypeName = typeName;
        this.values = values;
    }

    public int DisposeCount { get; private set; }

    public string Name { get; }

    public string TypeName { get; }

    public int RowCount => values.Length;

    public ReadOnlySpan<T> Values => values;

    public T this[int row] => values[row];

    public object GetValue(int row) => values[row];

    public void Dispose() => DisposeCount++;
}
