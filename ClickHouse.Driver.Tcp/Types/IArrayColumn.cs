using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Exposes a decoded <c>Array(T)</c> as a flat inner column and row offsets without requiring its CLR element
/// type. Row <c>i</c> occupies <c>[Offsets[i], Offsets[i + 1])</c> in <see cref="Inner"/>. The inner column and
/// offsets borrow block storage and are valid only while the block is alive.
/// </summary>
public interface IArrayColumn : IColumn
{
    /// <summary>
    /// Every row's elements concatenated end-to-end. Its row count is the total element count. The block owns
    /// and disposes this borrowed column.
    /// </summary>
    IColumn Inner { get; }

    /// <summary>
    /// The row offsets into <see cref="Inner"/>. The span starts at 0 and has one more entry than the row count.
    /// </summary>
    ReadOnlySpan<int> Offsets { get; }
}

/// <summary>
/// Adds typed access to the flat inner column. Use the non-generic <see cref="IArrayColumn"/> when the element
/// type is not known; a pattern with the wrong type argument does not match.
/// </summary>
/// <typeparam name="TElement">The inner element type; each row is a run of <typeparamref name="TElement"/>.</typeparam>
public interface IArrayColumn<TElement> : IArrayColumn, IColumn<TElement[]>
{
    /// <summary>
    /// The flat inner column, typed. Use this rather than <see cref="InnerValues"/> when
    /// <typeparamref name="TElement"/> is itself a composite and the span's type would be the materialized form.
    /// See <see cref="IArrayColumn.Inner"/> for the layout and the borrowing rule.
    /// </summary>
    new IColumn<TElement> Inner { get; }

    /// <summary>
    /// Every row's elements concatenated end-to-end. Materializing this span can allocate for string-like or
    /// composite elements; use <see cref="Inner"/> to inspect those without materializing them.
    /// </summary>
    ReadOnlySpan<TElement> InnerValues { get; }

    /// <inheritdoc/>
    IColumn IArrayColumn.Inner => Inner;
}
