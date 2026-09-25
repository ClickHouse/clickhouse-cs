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
    /// <exception cref="ArgumentNullException"><paramref name="dictionary"/> or <paramref name="keys"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="keys"/> holds fewer than <paramref name="rowCount"/> entries.</exception>
    public NullableLowCardinalityReferenceColumn(string name, string typeName, IColumn<T> dictionary, int[] keys, int rowCount, bool pooledKeys)
    {
        Name = name;
        TypeName = typeName;
        this.dictionary = dictionary ?? throw new ArgumentNullException(nameof(dictionary));
        this.keys = keys ?? throw new ArgumentNullException(nameof(keys));
        this.rowCount = rowCount;
        this.pooledKeys = pooledKeys;

        // The row count cannot be derived here: the dictionary holds one entry per distinct value (plus any reserved
        // slots), and the keys are typically a pooled buffer longer than the column. So the one input that can
        // disagree is checked instead — with a short keys array the per-row lookup would read past its end.
        if (keys.Length < rowCount)
        {
            throw new ArgumentException(
                $"The keys for column '{name}' ({typeName}) hold {keys.Length} entries, fewer than the {rowCount} rows.",
                nameof(keys));
        }
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

    /// <inheritdoc/>
    // Two reserved slots: the NULL marker at 0 and the inner default at 1, so real values start at 2.
    public int ReservedSlotCount => 2;

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
    // Index through the RowCount-sliced keys, not the raw buffer: the buffer is usually a pooled array longer than
    // the column, and a stale key left in its tail by a previous read is a perfectly valid dictionary index — so an
    // out-of-range row would quietly return a real value from the dictionary instead of throwing.
    public T this[int row]
    {
        get
        {
            if (cache is not null)
            {
                return cache[row];
            }

            // Through Values rather than the dictionary's indexer, so an entry built on access (a decoded string)
            // is shared by every row holding it instead of rebuilt per row. See LowCardinalityColumn.
            int key = Keys[row];
            return key == 0 ? null : dictionary.Values[key];
        }
    }

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
