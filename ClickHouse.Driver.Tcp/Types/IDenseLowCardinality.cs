using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The dense wire shape of a <c>LowCardinality</c> column — a dictionary column plus per-row keys — exposed so the
/// codec can re-emit a decoded column with no rebuild. Implemented only by the nullable low-cardinality columns:
/// the non-nullable <see cref="LowCardinalityColumn{T}"/> is deliberately <em>not</em> a dense source under a
/// nullable codec, because its dictionary reserves a single default slot rather than the two slots
/// (NULL + default) a <c>LowCardinality(Nullable(T))</c> dictionary requires — re-emitting it verbatim would make
/// the reader read slot 0 back as NULL.
/// </summary>
/// <typeparam name="T">The dictionary's CLR element type (the bare inner type; never made nullable).</typeparam>
internal interface IDenseLowCardinality<T>
{
    /// <summary>The dictionary of distinct values, including the reserved leading slots.</summary>
    IColumn<T> Dictionary { get; }

    /// <summary>The per-row keys, sliced to the column's row count — indices into <see cref="Dictionary"/>.</summary>
    ReadOnlySpan<int> Keys { get; }
}
