using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The zero-copy read surface of a decoded <c>Map(K, V)</c> column. A map column materializes each row as a freshly
/// allocated <see cref="KeyValuePair{TKey, TValue}"/>[] (see the column's <c>Values</c>/indexer), which is
/// convenient and lets the pairs outlive the block, but allocates an array — and a pair struct per entry — for
/// every row. This interface exposes the underlying wire layout instead: two flat, positionally aligned columns
/// holding every row's keys and every row's values concatenated end-to-end, plus the per-row offsets that delimit
/// them. It is byte-identical to <c>Array(Tuple(K, V))</c>, so the shape is the same one
/// <see cref="IArrayColumn{TElement}"/> exposes, with the run split across a key and a value column.
///
/// <para>
/// Row <c>i</c>'s entries are <c>Offsets[i]</c> (inclusive) to <c>Offsets[i + 1]</c> (exclusive) in both
/// <see cref="KeyColumn"/> and <see cref="ValueColumn"/>: entry <c>j</c> of that range pairs
/// <c>KeyColumn[j]</c> with <c>ValueColumn[j]</c>. Reading the two columns directly also lets a caller take only
/// the keys, or only the values, without building the pairs at all. Both are borrowed views over the owning
/// block's storage, as is <see cref="Offsets"/>: read them in place, and copy out only what must outlive the block.
/// </para>
///
/// <para>
/// The key and value columns preserve duplicate keys and entry order exactly as the wire carried them — the same
/// reason a row materializes as a pair array rather than a <see cref="Dictionary{TKey, TValue}"/>, which would
/// silently collapse duplicates. Either may itself be a composite (<c>Map(String, Array(UInt32))</c>), in which
/// case it pattern-matches to that type's own columnar view. Obtain this view by pattern-matching a column, e.g.
/// <c>if (column is IMapColumn&lt;string, uint&gt; map)</c>.
/// </para>
/// </summary>
/// <typeparam name="TKey">The key codec's CLR element type.</typeparam>
/// <typeparam name="TValue">The value codec's CLR element type.</typeparam>
public interface IMapColumn<TKey, TValue> : IColumn<KeyValuePair<TKey, TValue>[]>
{
    /// <summary>
    /// The flat key column: every row's keys concatenated end-to-end, addressed through <see cref="Offsets"/> and
    /// positionally aligned with <see cref="ValueColumn"/>. Its row count is the total entry count across all
    /// rows, not the map's row count. A borrowed view valid only while the owning block is alive.
    /// </summary>
    IColumn<TKey> KeyColumn { get; }

    /// <summary>
    /// The flat value column: every row's values concatenated end-to-end, aligned entry-for-entry with
    /// <see cref="KeyColumn"/>. A borrowed view valid only while the owning block is alive.
    /// </summary>
    IColumn<TValue> ValueColumn { get; }

    /// <summary>
    /// The per-row offsets into <see cref="KeyColumn"/> and <see cref="ValueColumn"/>: <c>[0]</c> is 0 and
    /// <c>[i + 1]</c> is the exclusive end of row <c>i</c>'s entries; the span has one more entry than the column
    /// has rows. A borrowed span valid only while the owning block is alive.
    /// </summary>
    ReadOnlySpan<int> Offsets { get; }
}
