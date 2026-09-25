using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Exposes a decoded <c>Map(K, V)</c> as aligned key and value columns plus row offsets. Row <c>i</c> occupies
/// <c>[Offsets[i], Offsets[i + 1])</c> in both columns. This representation preserves duplicate keys and entry
/// order. The columns and offsets borrow storage from the owning block.
/// </summary>
public interface IMapColumn : IColumn
{
    /// <summary>
    /// The flat key column: every row's keys concatenated end-to-end, addressed through <see cref="Offsets"/> and
    /// positionally aligned with <see cref="ValueColumn"/>. Its row count is the total entry count across all
    /// rows, not the map's row count. A borrowed view valid only while the owning block is alive — it is the
    /// block's to dispose, never the caller's.
    /// </summary>
    IColumn KeyColumn { get; }

    /// <summary>
    /// The flat value column: every row's values concatenated end-to-end, aligned entry-for-entry with
    /// <see cref="KeyColumn"/>. Borrowed on the same terms as <see cref="KeyColumn"/>.
    /// </summary>
    IColumn ValueColumn { get; }

    /// <summary>
    /// The per-row offsets into <see cref="KeyColumn"/> and <see cref="ValueColumn"/>: <c>[0]</c> is 0 and
    /// <c>[i + 1]</c> is the exclusive end of row <c>i</c>'s entries; the span has one more entry than the column
    /// has rows. A borrowed span valid only while the owning block is alive.
    /// </summary>
    ReadOnlySpan<int> Offsets { get; }
}

/// <summary>
/// Adds typed access to the key and value columns. Use the non-generic <see cref="IMapColumn"/> when their CLR
/// types are not known.
/// </summary>
/// <typeparam name="TKey">The key codec's CLR element type.</typeparam>
/// <typeparam name="TValue">The value codec's CLR element type.</typeparam>
public interface IMapColumn<TKey, TValue> : IMapColumn, IColumn<KeyValuePair<TKey, TValue>[]>
{
    /// <summary>The flat key column, typed. See <see cref="IMapColumn.KeyColumn"/> for the layout and the borrowing rule.</summary>
    new IColumn<TKey> KeyColumn { get; }

    /// <summary>The flat value column, typed. See <see cref="IMapColumn.ValueColumn"/> for the layout and the borrowing rule.</summary>
    new IColumn<TValue> ValueColumn { get; }

    /// <inheritdoc/>
    IColumn IMapColumn.KeyColumn => KeyColumn;

    /// <inheritdoc/>
    IColumn IMapColumn.ValueColumn => ValueColumn;
}
