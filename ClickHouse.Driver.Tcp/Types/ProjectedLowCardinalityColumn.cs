using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A read-side view over a <c>LowCardinality</c> column that converts each distinct <em>dictionary entry</em>
/// once and resolves every row to the entry its key names — what <see cref="Block.ReadAs{T}(string)"/> returns
/// for a low-cardinality column asked for a type other than its own. Converting per row instead would convert one
/// entry again for every row that shares it, which is exactly the cost the type exists to avoid: a million rows
/// over a five-entry dictionary need five conversions, not a million.
///
/// <para>
/// Entries are converted on first touch rather than up front, so a block of few rows over a large dictionary
/// converts only the entries its rows name. The source column and its dictionary are borrowed and stay the
/// block's to dispose; this view owns only the arrays it fills.
/// </para>
/// </summary>
/// <typeparam name="T">The projected element type.</typeparam>
internal sealed class ProjectedLowCardinalityColumn<T> : IColumn<T>
{
    private readonly ILowCardinalityColumn source;
    private readonly Func<IColumn, int, T> readEntry;
    private readonly T[] entries;
    private readonly bool[] converted;
    private T[] materialized;

    /// <summary>Initializes a view over <paramref name="source"/>'s dictionary.</summary>
    /// <param name="source">The decoded low-cardinality column to convert from.</param>
    /// <param name="readEntry">Reads one entry of <paramref name="source"/>'s dictionary as <typeparamref name="T"/>.</param>
    public ProjectedLowCardinalityColumn(ILowCardinalityColumn source, Func<IColumn, int, T> readEntry)
    {
        this.source = source ?? throw new ArgumentNullException(nameof(source));
        this.readEntry = readEntry ?? throw new ArgumentNullException(nameof(readEntry));

        int dictionarySize = source.Dictionary.RowCount;
        entries = new T[dictionarySize];
        converted = new bool[dictionarySize];

        // Slot 0 of a nullable dictionary is the NULL marker rather than a value, so a row keyed on it reads as
        // default(T) and the placeholder the wire carries there is never converted.
        if (source.ReservedSlotCount == 2 && dictionarySize > 0)
        {
            converted[0] = true;
        }
    }

    /// <inheritdoc/>
    public string Name => source.Name;

    /// <inheritdoc/>
    public string TypeName => source.TypeName;

    /// <inheritdoc/>
    public int RowCount => source.RowCount;

    /// <summary>
    /// The projected values, one per row, materialized once and cached. Every row still shares its entry's
    /// converted value, so this array holds <see cref="IColumn.RowCount"/> references to at most as many distinct
    /// values as the dictionary has entries. It is this view's own, not a pooled buffer, so it is not returned on
    /// <see cref="Dispose"/> — but it is only as valid as the source column it was read from, so build it while
    /// the owning block is alive.
    /// </summary>
    public ReadOnlySpan<T> Values
    {
        get
        {
            if (materialized is null)
            {
                var values = new T[source.RowCount];
                ReadOnlySpan<int> keys = source.Keys;
                for (int i = 0; i < values.Length; i++)
                {
                    values[i] = Entry(keys[i]);
                }

                materialized = values;
            }

            return materialized;
        }
    }

    /// <inheritdoc/>
    public T this[int row] => materialized is not null ? materialized[row] : Entry(source.Keys[row]);

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>
    /// Releases nothing: the source column and its dictionary belong to the block that produced them, and
    /// emptying them here would take the block's data away from every other reader of it.
    /// </summary>
    public void Dispose()
    {
    }

    // The key was validated against the dictionary size when the column was read, so it indexes both arrays.
    private T Entry(int slot)
    {
        if (!converted[slot])
        {
            entries[slot] = readEntry(source.Dictionary, slot);
            converted[slot] = true;
        }

        return entries[slot];
    }
}
