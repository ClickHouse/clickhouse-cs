using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The columnar read surface of a decoded <c>Array(T)</c> column. An array column materializes each row as a
/// freshly allocated <typeparamref name="TElement"/>[] (see the column's <c>Values</c>/indexer), which is
/// convenient and lets the arrays outlive the block, but allocates one array per row. This interface exposes the
/// underlying flat wire layout instead — the elements of every row concatenated end-to-end, plus the per-row
/// offsets into that run — so a caller that only reads can iterate without allocating a row array.
///
/// <para>
/// Row <c>i</c>'s elements are <c>InnerValues.Slice(Offsets[i], Offsets[i + 1] - Offsets[i])</c>. Both spans are
/// borrowed views over the owning block's storage: read them in place, and copy out (e.g. <c>ToArray()</c>) only
/// what must outlive the block.
/// </para>
///
/// <para>
/// Extends the column contract for <typeparamref name="TElement"/>[] (so <see cref="IColumn{T}.Values"/> and the
/// indexer give the allocating per-row arrays); obtain this zero-copy view by pattern-matching a column, e.g.
/// <c>if (column is IArrayColumn&lt;uint&gt; array)</c>.
/// </para>
/// </summary>
/// <typeparam name="TElement">The inner element type; each row is a run of <typeparamref name="TElement"/>.</typeparam>
public interface IArrayColumn<TElement> : IColumn<TElement[]>
{
    /// <summary>
    /// The flat inner column itself — every row's elements concatenated end-to-end, as a column rather than a
    /// span. Use this when <typeparamref name="TElement"/> is itself a composite and
    /// <see cref="InnerValues"/> would hand back materialized values: for <c>Array(Tuple(...))</c> the inner
    /// column pattern-matches to <see cref="ITupleColumn"/>, for <c>Array(Array(T))</c> to another
    /// <see cref="IArrayColumn{TElement}"/>, and so on, so a nested composite can be walked all the way down
    /// without materializing an intermediate level. A borrowed view valid only while the owning block is alive —
    /// it is the block's to dispose, never the caller's.
    /// </summary>
    IColumn<TElement> Inner { get; }

    /// <summary>
    /// Every row's elements concatenated end-to-end — the flat wire layout, paired with <see cref="Offsets"/>. A
    /// borrowed span valid only while the owning block is alive.
    ///
    /// <para>
    /// Free only where <typeparamref name="TElement"/> is a fixed-width value type. For a string-like or composite
    /// element the span's type is the <em>materialized</em> form, so producing it allocates per element — a
    /// <c>string</c> each for <c>Array(String)</c>, an inner array each for <c>Array(Array(T))</c>. Prefer
    /// <see cref="Inner"/> in those cases and recurse into the element type's own columnar view.
    /// </para>
    /// </summary>
    ReadOnlySpan<TElement> InnerValues { get; }

    /// <summary>
    /// The per-row offsets into <see cref="InnerValues"/>: <c>[0]</c> is 0 and <c>[i + 1]</c> is the exclusive end
    /// of row <c>i</c>'s slice; the span has one more entry than the column has rows. A borrowed span valid only
    /// while the owning block is alive.
    /// </summary>
    ReadOnlySpan<int> Offsets { get; }
}
