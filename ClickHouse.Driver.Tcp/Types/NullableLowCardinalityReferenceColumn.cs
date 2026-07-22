using System;
using System.Buffers;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A ClickHouse <c>LowCardinality(Nullable(T))</c> column for a reference-type inner (e.g.
/// <c>LowCardinality(Nullable(String))</c>): it pairs a dictionary of distinct inner values with a per-row array of
/// keys, and surfaces each row as the inner value <c>dict[keys[row]]</c>, or <see langword="null"/> where the key
/// points at the reserved NULL slot (<c>key 0</c>).
///
/// <para>
/// The dictionary is the bare inner type <typeparamref name="T"/> (there is no null-map in the dictionary stream);
/// nullability is expressed positionally through the reserved slot 0. The dictionary reserves two leading slots —
/// <c>dict[0]</c> is the NULL marker and <c>dict[1]</c> the inner default — so real distinct values start at
/// <c>dict[2]</c>. This is the dense shape the wire uses, so it is also the zero-copy source for writing.
/// </para>
///
/// <para>
/// The dictionary column's storage and the keys are borrowed for this column's lifetime; the dictionary is disposed
/// and the keys returned (when pooled) on <see cref="Dispose"/>. Read the column only while the owning block is alive.
/// </para>
/// </summary>
/// <typeparam name="T">The inner reference type; each row surfaces as <typeparamref name="T"/> or <see langword="null"/>.</typeparam>
internal sealed class NullableLowCardinalityReferenceColumn<T> : IColumn<T>, IDenseLowCardinality<T>
    where T : class
{
    private readonly IColumn<T> dictionary;
    private readonly int rowCount;
    private readonly bool pooledKeys;
    private int[] keys;
    private T[] cache;

    /// <summary>Initializes a nullable low-cardinality column over a dictionary column and its per-row keys.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The full <c>LowCardinality(Nullable(...))</c> type string.</param>
    /// <param name="dictionary">The dictionary column holding the distinct values (including the reserved slots).</param>
    /// <param name="keys">The per-row indices into <paramref name="dictionary"/>; must have at least <paramref name="rowCount"/> entries.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooledKeys">Whether <paramref name="keys"/> was rented and should be returned on dispose.</param>
    public NullableLowCardinalityReferenceColumn(string name, string typeName, IColumn<T> dictionary, int[] keys, int rowCount, bool pooledKeys)
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

    /// <inheritdoc/>
    public IColumn<T> Dictionary => dictionary;

    /// <inheritdoc/>
    public ReadOnlySpan<int> Keys => keys.AsSpan(0, rowCount);

    /// <summary>The rows, materialized once and cached, with <see langword="null"/> at the reserved-NULL-slot rows.</summary>
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
                    int key = keys[i];
                    decoded[i] = key == 0 ? null : dict[key];
                }

                cache = decoded;
            }

            return cache.AsSpan(0, rowCount);
        }
    }

    /// <inheritdoc/>
    public T this[int row] => cache is not null ? cache[row] : keys[row] == 0 ? null : dictionary[keys[row]];

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
