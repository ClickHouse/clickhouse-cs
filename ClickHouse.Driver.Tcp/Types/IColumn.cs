using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A decoded column: a named, typed sequence of values read from one block. The generic <see cref="IColumn{T}"/>
/// exposes the values; this non-generic view lets a block hold columns of mixed element types and read a value
/// without knowing its static type.
///
/// <para>
/// A column's storage may be a pooled buffer, so it is disposable and its values are borrowed for the block's
/// lifetime. The owning <see cref="Format.Block"/> disposes its columns; a consumer must not read a column
/// after the block is released (see <see cref="Format.Block"/> for the borrowing contract).
/// </para>
/// </summary>
public interface IColumn : IDisposable
{
    /// <summary>The column name from the block header.</summary>
    string Name { get; }

    /// <summary>The ClickHouse type string from the block header (e.g. <c>UInt64</c>, <c>String</c>).</summary>
    string TypeName { get; }

    /// <summary>The number of values in the column.</summary>
    int RowCount { get; }

    /// <summary>Returns the value at <paramref name="row"/>, boxed. Prefer the typed <see cref="IColumn{T}.Values"/> on the fast path.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The value at that row.</returns>
    object GetValue(int row);
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
