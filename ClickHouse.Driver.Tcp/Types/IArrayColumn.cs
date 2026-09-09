using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The columnar read surface of a decoded <c>Array(T)</c> column, without its inner CLR element type. An array
/// column materializes each row as a freshly allocated array (see the column's <c>Values</c>/indexer), which is
/// convenient and lets the arrays outlive the block, but allocates one array per row. This interface exposes the
/// underlying flat wire layout instead — the elements of every row concatenated end-to-end in one inner column,
/// plus the per-row offsets into that run — so a caller that only reads can iterate without allocating a row
/// array.
///
/// <para>
/// Row <c>i</c>'s elements are the <c>Offsets[i]</c> (inclusive) to <c>Offsets[i + 1]</c> (exclusive) range of
/// <see cref="Inner"/>. Match this interface when the element type is not known in advance, then match
/// <see cref="Inner"/> for the reading you want: an <c>Array(DateTime)</c>'s inner column is an
/// <see cref="IDateTimeColumn"/>, an <c>Array(Tuple(...))</c>'s an <see cref="ITupleColumn"/>, an
/// <c>Array(Array(T))</c>'s another <see cref="IArrayColumn"/>, so a nested composite can be walked all the way
/// down with no type argument known and nothing materialized.
/// </para>
///
/// <para>
/// The inner column's storage and <see cref="Offsets"/> are borrowed views over the owning block: read them in
/// place, and copy out (e.g. <c>ToArray()</c>) only what must outlive the block.
/// </para>
/// </summary>
public interface IArrayColumn : IColumn
{
    /// <summary>
    /// The flat inner column: every row's elements concatenated end-to-end. Its row count is the total element
    /// count across all rows, not the array column's row count. A borrowed view valid only while the owning block
    /// is alive — it is the block's to dispose, never the caller's.
    /// </summary>
    IColumn Inner { get; }

    /// <summary>
    /// The per-row offsets into <see cref="Inner"/>: <c>[0]</c> is 0 and <c>[i + 1]</c> is the exclusive end of
    /// row <c>i</c>'s slice; the span has one more entry than the column has rows. A borrowed span valid only
    /// while the owning block is alive.
    /// </summary>
    ReadOnlySpan<int> Offsets { get; }
}

/// <summary>
/// A decoded <c>Array(T)</c> column whose element type is known, adding the typed <see cref="Inner"/> and the
/// flat <see cref="InnerValues"/> span to <see cref="IArrayColumn"/>.
///
/// <para>
/// Row <c>i</c>'s elements are <c>InnerValues.Slice(Offsets[i], Offsets[i + 1] - Offsets[i])</c>. Extends the
/// column contract for <typeparamref name="TElement"/>[] (so <see cref="IColumn{T}.Values"/> and the indexer give
/// the allocating per-row arrays); obtain this zero-copy view by pattern-matching a column, e.g.
/// <c>if (column is IArrayColumn&lt;uint&gt; array)</c>. Naming the wrong element type compiles with no warning
/// and never matches, so match the non-generic <see cref="IArrayColumn"/> when the element type is not certain.
/// </para>
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
    /// Every row's elements concatenated end-to-end — the flat wire layout, paired with
    /// <see cref="IArrayColumn.Offsets"/>. A borrowed span valid only while the owning block is alive.
    ///
    /// <para>
    /// Free only where <typeparamref name="TElement"/> is a fixed-width value type. For a string-like or
    /// composite element the span's type is the <em>materialized</em> form, so producing it allocates per element
    /// — a <c>string</c> each for <c>Array(String)</c>, an inner array each for <c>Array(Array(T))</c>. Prefer
    /// <see cref="Inner"/> in those cases and recurse into the element type's own columnar view.
    /// </para>
    /// </summary>
    ReadOnlySpan<TElement> InnerValues { get; }

    /// <inheritdoc/>
    IColumn IArrayColumn.Inner => Inner;
}
