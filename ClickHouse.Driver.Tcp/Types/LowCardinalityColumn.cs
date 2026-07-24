using System;
using System.Buffers;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A ClickHouse <c>LowCardinality(T)</c> column: it pairs a small dictionary of distinct inner values with a
/// per-row array of keys (indices into the dictionary), and surfaces each row as the inner CLR value
/// <c>dict[keys[row]]</c>. This is the dense shape the wire uses (a dictionary stream plus a keys stream), so it
/// is also the zero-copy source for writing — handed back to the <c>LowCardinality(T)</c> codec it re-emits the
/// dictionary and keys without rebuilding anything.
///
/// <para>
/// The dictionary reserves leading slots the server never surfaces as data: for a non-nullable inner,
/// <c>dict[0]</c> holds the inner type's default value and real values start at <c>dict[1]</c>. Those reserved
/// slots are ordinary dictionary entries here — a row's key simply never points at an unused reserve.
/// </para>
///
/// <para>
/// The dictionary column's storage and the keys are borrowed for this column's lifetime; the dictionary is
/// disposed and the keys returned (when pooled) on <see cref="Dispose"/>. Read the column only while the owning
/// block is alive.
/// </para>
/// </summary>
/// <typeparam name="T">The inner codec's CLR element type; each row surfaces as <typeparamref name="T"/>.</typeparam>
internal sealed class LowCardinalityColumn<T> : IColumn<T>
{
    private readonly IColumn<T> dictionary;
    private readonly int rowCount;
    private readonly bool pooledKeys;
    private int[] keys;
    private T[] cache;

    /// <summary>Initializes a low-cardinality column over a dictionary column and its per-row keys.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The full <c>LowCardinality(...)</c> type string.</param>
    /// <param name="dictionary">The dictionary column holding the distinct values (including any reserved slots).</param>
    /// <param name="keys">The per-row indices into <paramref name="dictionary"/>; must have at least <paramref name="rowCount"/> entries.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooledKeys">Whether <paramref name="keys"/> was rented and should be returned on dispose.</param>
    public LowCardinalityColumn(string name, string typeName, IColumn<T> dictionary, int[] keys, int rowCount, bool pooledKeys)
    {
        Name = name;
        TypeName = typeName;
        this.dictionary = dictionary ?? throw new ArgumentNullException(nameof(dictionary));
        this.keys = keys ?? throw new ArgumentNullException(nameof(keys));
        this.rowCount = rowCount;
        this.pooledKeys = pooledKeys;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => rowCount;

    /// <summary>The dictionary of distinct values (including reserved slots) — the zero-copy write source.</summary>
    internal IColumn<T> Dictionary => dictionary;

    /// <summary>The per-row keys, sliced to <see cref="RowCount"/> entries — the zero-copy write source.</summary>
    internal ReadOnlySpan<int> Keys => keys.AsSpan(0, rowCount);

    /// <summary>
    /// The rows as values, materialized once and cached. Prefer the indexer when only some rows are needed, to
    /// avoid reconstructing the whole column.
    /// </summary>
    public ReadOnlySpan<T> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new T[rowCount];
                ReadOnlySpan<T> dict = dictionary.Values;
                for (int i = 0; i < rowCount; i++)
                {
                    decoded[i] = dict[keys[i]];
                }

                cache = decoded;
            }

            return cache.AsSpan(0, rowCount);
        }
    }

    /// <inheritdoc/>
    public T this[int row] => cache is not null ? cache[row] : dictionary[keys[row]];

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <inheritdoc/>
    public void Dispose()
    {
        dictionary.Dispose();
        if (pooledKeys && keys.Length != 0)
        {
            ArrayPool<int>.Shared.Return(keys);
        }

        keys = Array.Empty<int>();
        cache = null;
    }
}
