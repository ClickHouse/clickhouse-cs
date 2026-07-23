using System;
using System.Buffers;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A ClickHouse <c>Map(K, V)</c> column: it pairs two flat inner columns — every row's keys and every row's
/// values concatenated end-to-end, positionally aligned — with a per-row offsets array, and surfaces each row as
/// a freshly materialized <see cref="KeyValuePair{TKey, TValue}"/>[]. This is the dense shape the wire uses
/// (offsets + a keys stream + a values stream, byte-identical to <c>Array(Tuple(K, V))</c>), so it is also the
/// zero-copy source for writing — handed back to the <c>Map(K, V)</c> codec it serializes straight from the two
/// inner columns and offsets without rebuilding anything.
///
/// <para>
/// Each row surfaces as a <see cref="KeyValuePair{TKey, TValue}"/>[] rather than a <see cref="Dictionary{TKey, TValue}"/>
/// so that duplicate keys and pair order — both meaningful on the wire — round-trip intact; a dictionary would
/// silently collapse duplicates. <typeparamref name="TKey"/> and <typeparamref name="TValue"/> may themselves be
/// composites (<c>Map(String, Array(UInt32))</c>, <c>Map(String, Tuple(...))</c>) — whatever the key/value codecs
/// surface.
/// </para>
///
/// <para>
/// The inner columns' storage and the offsets are borrowed for this column's lifetime; the inner columns are
/// disposed and the offsets returned (when pooled) on <see cref="Dispose"/>. Each <see cref="this"/> access
/// copies the row's pairs into a new array, so retain those arrays freely, but read the column itself only while
/// the owning block is alive.
/// </para>
/// </summary>
/// <typeparam name="TKey">The key codec's CLR element type.</typeparam>
/// <typeparam name="TValue">The value codec's CLR element type.</typeparam>
internal sealed class MapColumn<TKey, TValue> : IColumn<KeyValuePair<TKey, TValue>[]>
{
    private readonly IColumn<TKey> keys;
    private readonly IColumn<TValue> values;
    private readonly int rowCount;
    private readonly bool pooledOffsets;
    private int[] offsets;
    private KeyValuePair<TKey, TValue>[][] cache;

    // Whether Dispose disposes the key / value column. Both true by default (this column owns its inner columns);
    // RestrictOwnership flips one off when a densified wrapper keeps that child by reference from another column.
    private bool ownsKeys = true;
    private bool ownsValues = true;

    /// <summary>Initializes a map column over the flat key and value columns and their shared per-row offsets.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The full <c>Map(...)</c> type string.</param>
    /// <param name="keys">The key column holding every row's keys concatenated end-to-end.</param>
    /// <param name="values">The value column holding every row's values concatenated end-to-end, aligned with <paramref name="keys"/>.</param>
    /// <param name="offsets">The per-row offsets: <c>offsets[0]</c> is 0 and <c>offsets[i + 1]</c> is the exclusive end of row <c>i</c>'s pairs; must have at least <paramref name="rowCount"/> + 1 entries.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooledOffsets">Whether <paramref name="offsets"/> was rented and should be returned on dispose.</param>
    public MapColumn(string name, string typeName, IColumn<TKey> keys, IColumn<TValue> values, int[] offsets, int rowCount, bool pooledOffsets)
    {
        Name = name;
        TypeName = typeName;
        this.keys = keys ?? throw new ArgumentNullException(nameof(keys));
        this.values = values ?? throw new ArgumentNullException(nameof(values));
        this.offsets = offsets ?? throw new ArgumentNullException(nameof(offsets));
        this.rowCount = rowCount;
        this.pooledOffsets = pooledOffsets;
    }

    /// <summary>
    /// Restricts which inner columns <see cref="Dispose"/> disposes, overriding the default of owning both. Used
    /// when a densified map rebuilds only one of the key/value columns and keeps the other by reference (owned by
    /// the source column), so disposing this wrapper frees only the column it built. Must be called before the
    /// column is observed.
    /// </summary>
    internal void RestrictOwnership(bool keysOwned, bool valuesOwned)
    {
        ownsKeys = keysOwned;
        ownsValues = valuesOwned;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => rowCount;

    /// <summary>The flat key column (every row's keys concatenated) — a zero-copy write source.</summary>
    internal IColumn<TKey> KeyColumn => keys;

    /// <summary>The flat value column (every row's values concatenated, aligned with <see cref="KeyColumn"/>) — a zero-copy write source.</summary>
    internal IColumn<TValue> ValueColumn => values;

    /// <summary>
    /// The per-row offsets, sliced to <see cref="RowCount"/> + 1 entries: <c>[0]</c> is 0 and <c>[i + 1]</c> is
    /// the exclusive end of row <c>i</c>'s slice of the key/value columns — a zero-copy write source.
    /// </summary>
    internal ReadOnlySpan<int> Offsets => offsets.AsSpan(0, rowCount + 1);

    /// <summary>
    /// The rows as key/value-pair arrays, materialized once and cached. Prefer the indexer when only some rows are
    /// needed, to avoid building the whole jagged array.
    /// </summary>
    public ReadOnlySpan<KeyValuePair<TKey, TValue>[]> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new KeyValuePair<TKey, TValue>[rowCount][];
                for (int i = 0; i < rowCount; i++)
                {
                    decoded[i] = Materialize(i);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, rowCount);
        }
    }

    /// <inheritdoc/>
    public KeyValuePair<TKey, TValue>[] this[int row] => cache is not null ? cache[row] : Materialize(row);

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <inheritdoc/>
    public void Dispose()
    {
        if (ownsKeys)
        {
            keys.Dispose();
        }

        if (ownsValues)
        {
            values.Dispose();
        }

        if (pooledOffsets && offsets.Length != 0)
        {
            ArrayPool<int>.Shared.Return(offsets);
        }

        offsets = Array.Empty<int>();
        cache = null;
    }

    /// <summary>Copies row <paramref name="row"/>'s slice of the key and value columns into a new pair array, preserving order and duplicate keys.</summary>
    private KeyValuePair<TKey, TValue>[] Materialize(int row)
    {
        int start = offsets[row];
        int length = offsets[row + 1] - start;
        if (length == 0)
        {
            return Array.Empty<KeyValuePair<TKey, TValue>>();
        }

        var pairs = new KeyValuePair<TKey, TValue>[length];
        for (int i = 0; i < length; i++)
        {
            pairs[i] = new KeyValuePair<TKey, TValue>(keys[start + i], values[start + i]);
        }

        return pairs;
    }
}
