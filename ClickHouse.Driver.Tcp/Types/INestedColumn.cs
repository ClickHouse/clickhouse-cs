using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The zero-copy read surface of a decoded <c>Nested(name1 T1, ..., namen Tn)</c> column carried as one wire column
/// (the <c>flatten_nested = 0</c> form). A nested column is a table-within-a-row: each row holds a variable number
/// of records, and each record has one value per named field. On the wire — and here — that is stored field-major:
/// one flat column per field holding every row's elements for that field concatenated end-to-end, plus a single
/// per-row offsets array shared by all fields (within a row every field has the same element count, so one set of
/// row boundaries delimits them all). It is byte-identical to <c>Array(Tuple(T1, ..., Tn))</c>.
///
/// <para>
/// This interface is the primary access path, not merely a fast one. Because a <c>Nested</c> can carry any number
/// of fields, there is no single generic per-row value type for it, so the <see cref="IColumn{T}"/> surface has to
/// fall back to <c>object[][]</c> — one boxed <c>object[]</c> of field values per record, per row. Reading through
/// <see cref="GetField(int)"/> and <see cref="Offsets"/> instead is both typed and allocation-free.
/// </para>
///
/// <para>
/// Row <c>i</c>'s elements for a field are that field's column sliced from <c>Offsets[i]</c> (inclusive) to
/// <c>Offsets[i + 1]</c> (exclusive); the same range applies to every field, so element <c>j</c> of that range
/// across all fields makes up one record. Field columns and <see cref="Offsets"/> are borrowed views over the
/// owning block's storage: read them in place, and copy out only what must outlive the block. Obtain this view by
/// pattern-matching a column, e.g. <c>if (column is INestedColumn nested)</c>.
/// </para>
/// </summary>
public interface INestedColumn : IColumn
{
    /// <summary>The number of named fields.</summary>
    int FieldCount { get; }

    /// <summary>The field names, in declaration order, aligned with the field columns.</summary>
    IReadOnlyList<string> FieldNames { get; }

    /// <summary>
    /// The per-row offsets, shared by every field: <c>[0]</c> is 0 and <c>[i + 1]</c> is the exclusive end of row
    /// <c>i</c>'s elements in <em>every</em> field column; the span has one more entry than the column has rows. A
    /// borrowed span valid only while the owning block is alive.
    /// </summary>
    ReadOnlySpan<int> Offsets { get; }

    /// <summary>
    /// The flat column for the field at <paramref name="index"/>: every row's elements for that field concatenated
    /// end-to-end. Its row count is the total element count across all rows — the same for every field — not the
    /// nested column's row count. A field whose type is itself a composite pattern-matches to that type's own
    /// columnar view. A borrowed view valid only while the owning block is alive.
    /// </summary>
    /// <param name="index">The zero-based field index.</param>
    /// <returns>That field's flat column.</returns>
    IColumn GetField(int index);

    /// <summary>The flat column for the field named <paramref name="name"/>, matched ordinally.</summary>
    /// <param name="name">The field name.</param>
    /// <returns>That field's flat column.</returns>
    /// <exception cref="KeyNotFoundException">No field has that name.</exception>
    IColumn GetField(string name);
}
