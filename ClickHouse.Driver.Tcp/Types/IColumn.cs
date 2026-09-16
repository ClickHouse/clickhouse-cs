using System;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// A named, typed column whose storage is borrowed for the owning <see cref="Block"/>'s lifetime. Use
/// <see cref="IColumn{T}"/> for typed access; the block owns disposal.
/// </summary>
public interface IColumn : IDisposable
{
    /// <summary>The column name from the block header.</summary>
    string Name { get; }

    /// <summary>
    /// The ClickHouse type string from the block header (e.g. <c>UInt64</c>, <c>String</c>). Null for a column
    /// built by <see cref="ClickHouseTcpColumn"/>, which has no header: an insert takes the type from the
    /// target's schema, so the caller never states one.
    /// </summary>
    string TypeName { get; }

    /// <summary>The number of values in the column.</summary>
    int RowCount { get; }

    /// <summary>Returns the value at <paramref name="row"/>, boxed. Prefer the typed <see cref="IColumn{T}.Values"/> on the fast path.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The value at that row.</returns>
    object GetValue(int row);

    /// <summary>The <c>T</c> in this column's <see cref="IColumn{T}"/> interface.</summary>
    Type ElementType => ColumnElementTypes.Of(GetType());
}

/// <summary>
/// A decoded column with typed access. <see cref="Values"/> is a borrowed view valid for the lifetime of the
/// block: process it in place, or copy it (e.g. <c>ToArray()</c>) to retain the data beyond the block.
/// </summary>
/// <typeparam name="T">The CLR element type the column's ClickHouse type maps to.</typeparam>
public interface IColumn<T> : IColumn
{
    /// <summary>
    /// The values, in row order — a borrowed span, not owned by the caller. The span is recomputed on each
    /// access (it cannot be cached in a field, being a ref struct); in a hot loop, read it into a local once
    /// and iterate that rather than re-reading this property per element.
    /// </summary>
    ReadOnlySpan<T> Values { get; }

    /// <summary>The value at <paramref name="row"/>.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The value at that row.</returns>
    T this[int row] { get; }
}
