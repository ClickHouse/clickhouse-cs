using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Exposes a decoded <c>LowCardinality(T)</c> column as a dictionary plus one key per row. Row <c>i</c> reads
/// <c>Dictionary[Keys[i]]</c>. The dictionary, keys, and their storage are borrowed from the owning block.
/// Use <see cref="ReservedSlotCount"/> to distinguish data entries from reserved default and NULL entries.
/// </summary>
public interface ILowCardinalityColumn : IColumn
{
    /// <summary>
    /// The distinct values, including reserved leading entries. The block owns and disposes this borrowed column.
    /// </summary>
    IColumn Dictionary { get; }

    /// <summary>
    /// One key per row — an index into <see cref="Dictionary"/>. A borrowed span valid only while the owning block
    /// is alive.
    /// </summary>
    ReadOnlySpan<int> Keys { get; }

    /// <summary>
    /// The number of reserved leading entries: 1 for a non-nullable inner and 2 for a nullable inner. For a
    /// nullable inner, key 0 represents NULL.
    /// </summary>
    int ReservedSlotCount { get; }
}

/// <summary>
/// Adds typed access to the dictionary. <typeparamref name="T"/> is the non-nullable inner type, including for
/// <c>LowCardinality(Nullable(T))</c>. Use the non-generic interface when that type is not known.
/// </summary>
/// <typeparam name="T">The dictionary's CLR element type (the bare inner type; never made nullable).</typeparam>
public interface ILowCardinalityColumn<T> : ILowCardinalityColumn
{
    /// <summary>The dictionary, typed. See <see cref="ILowCardinalityColumn.Dictionary"/> for the reserved slots and the borrowing rule.</summary>
    new IColumn<T> Dictionary { get; }

    /// <inheritdoc/>
    IColumn ILowCardinalityColumn.Dictionary => Dictionary;
}
