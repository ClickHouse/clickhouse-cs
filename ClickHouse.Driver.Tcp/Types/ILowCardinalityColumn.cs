using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The zero-copy read surface of a decoded <c>LowCardinality(T)</c> or <c>LowCardinality(Nullable(T))</c> column:
/// a dictionary of distinct values plus one key per row indexing into it. That is exactly the wire layout, and it
/// is what makes the type worth reading columnar — the materialized <c>Values</c>/indexer surface resolves every
/// row to its dictionary entry, so a column of a million rows over a five-entry dictionary materializes a million
/// values. Reading the dictionary and the keys instead lets a caller group, count, or filter on the keys and touch
/// each distinct value once.
///
/// <para>
/// Row <c>i</c>'s value is <c>Dictionary[Keys[i]]</c>. The dictionary carries reserved leading slots that never
/// appear as data, and <em>how many</em> depends on nullability: for a non-nullable inner, <c>Dictionary[0]</c>
/// holds the inner type's default and real values start at <c>[1]</c>; for a nullable inner, <c>[0]</c> is the NULL
/// marker and <c>[1]</c> the default, so real values start at <c>[2]</c>. So a key of <c>0</c> means NULL only for
/// a <c>LowCardinality(Nullable(T))</c> column — check the column's <see cref="IColumn.TypeName"/>, or compare
/// against the materialized value, before reading a key as a null marker.
/// </para>
///
/// <para>
/// The type parameter is the dictionary's element type — the bare inner type, never made nullable — so both
/// <c>LowCardinality(Int32)</c> and <c>LowCardinality(Nullable(Int32))</c> present as
/// <c>ILowCardinalityColumn&lt;int&gt;</c> even though their <see cref="IColumn{T}"/> surfaces differ
/// (<c>IColumn&lt;int&gt;</c> and <c>IColumn&lt;int?&gt;</c> respectively). Both the dictionary column and the keys
/// are borrowed views over the owning block's storage: read them in place, and copy out only what must outlive the
/// block. Obtain this view by pattern-matching a column, e.g. <c>if (column is ILowCardinalityColumn&lt;int&gt; lc)</c>.
/// </para>
/// </summary>
/// <typeparam name="T">The dictionary's CLR element type (the bare inner type; never made nullable).</typeparam>
public interface ILowCardinalityColumn<T> : IColumn
{
    /// <summary>
    /// The dictionary of distinct values, including the reserved leading slots described on this interface. Its row
    /// count is the dictionary size, not the column's row count. A borrowed view valid only while the owning block
    /// is alive.
    /// </summary>
    IColumn<T> Dictionary { get; }

    /// <summary>
    /// One key per row — an index into <see cref="Dictionary"/>. A borrowed span valid only while the owning block
    /// is alive.
    /// </summary>
    ReadOnlySpan<int> Keys { get; }
}
