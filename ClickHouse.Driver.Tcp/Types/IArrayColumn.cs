using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The zero-copy read surface of a decoded <c>Array(T)</c> column. An array column materializes each row as a
/// freshly allocated <typeparamref name="TElement"/>[] (see the column's <c>Values</c>/indexer), which is
/// convenient and lets the arrays outlive the block, but allocates one array per row. This interface exposes the
/// underlying flat wire layout instead — the elements of every row concatenated end-to-end, plus the per-row
/// offsets into that run — so a caller that only reads can iterate without allocating.
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
    /// Every row's elements concatenated end-to-end — the flat wire layout, paired with <see cref="Offsets"/>. A
    /// borrowed span valid only while the owning block is alive.
    /// </summary>
    ReadOnlySpan<TElement> InnerValues { get; }

    /// <summary>
    /// The per-row offsets into <see cref="InnerValues"/>: <c>[0]</c> is 0 and <c>[i + 1]</c> is the exclusive end
    /// of row <c>i</c>'s slice; the span has one more entry than the column has rows. A borrowed span valid only
    /// while the owning block is alive.
    /// </summary>
    ReadOnlySpan<int> Offsets { get; }
}
