using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The columnar read surface of a decoded <c>Nullable(T)</c> column, without its inner CLR element type. A
/// nullable column materializes each row as the inner value or <see langword="null"/> (see the column's
/// <c>Values</c>/indexer), which is the convenient form; this interface exposes the underlying wire layout
/// instead — the dense inner column, which holds a decoded value at <em>every</em> row (a placeholder where the
/// row is null), plus the per-row null-map that says which of those rows are actually null.
///
/// <para>
/// Row <c>i</c> is null when <c>NullMap[i] != 0</c>, and otherwise holds the value at row <c>i</c> of
/// <see cref="Inner"/>. Match this interface when the inner element type is not known in advance, then match the
/// inner column for the reading you want: a <c>Nullable(DateTime)</c>'s inner column is an
/// <see cref="IDateTimeColumn"/>, so the calendar reading needs no knowledge of the storage width.
/// </para>
///
/// <code>
/// if (column is INullableColumn nullable &amp;&amp; nullable.Inner is IDateTimeColumn timestamps)
/// {
///     DateTimeOffset? value = nullable.NullMap[row] != 0 ? null : timestamps.GetDateTimeOffset(row);
/// }
/// </code>
///
/// <para>
/// The wrapper itself does <b>not</b> implement <see cref="IDateTimeColumn"/> or <see cref="ITimeColumn"/>: those
/// return a value for every row, and a null row has none. The inner column does, and the null-map says which of
/// its rows to trust.
/// </para>
///
/// <para>
/// Both the map and the inner column's storage are borrowed views over the owning block: read them in place, and
/// copy out only what must outlive the block.
/// </para>
///
/// <para>
/// This is not a general "is this column nullable?" test. Only a type spelled <c>Nullable(T)</c> on the wire has
/// a null-map to expose; a type that encodes absence some other way will not implement it even though its values
/// can be null.
/// </para>
/// </summary>
public interface INullableColumn : IColumn
{
    /// <summary>
    /// The dense inner column: one decoded value per row, with an arbitrary placeholder at the rows the null-map
    /// marks null (the wire carries a value there too, so its content is meaningless — always consult
    /// <see cref="NullMap"/> before reading a row). A borrowed view valid only while the owning block is alive —
    /// it is the block's to dispose, never the caller's.
    /// </summary>
    IColumn Inner { get; }

    /// <summary>
    /// The per-row null-map: a non-zero byte marks the row null. One entry per row, aligned with
    /// <see cref="Inner"/>. A borrowed span valid only while the owning block is alive.
    /// </summary>
    ReadOnlySpan<byte> NullMap { get; }
}

/// <summary>
/// A decoded <c>Nullable(T)</c> column whose inner CLR element type is known, adding the typed
/// <see cref="Inner"/> to <see cref="INullableColumn"/>. Reading the inner column and the null-map directly
/// avoids the per-row null check and, for a value-type inner, the <see cref="Nullable{T}"/> wrapper: a caller
/// that only needs the non-null rows can scan the map and index the inner column's own
/// <see cref="IColumn{T}.Values"/> span.
///
/// <para>
/// The type parameter is the <em>inner</em> element type, not the nullable one — a <c>Nullable(Int32)</c> column
/// is an <c>INullableColumn&lt;int&gt;</c> (whose <see cref="IColumn{T}"/> surface is
/// <c>IColumn&lt;int?&gt;</c>), and a <c>Nullable(String)</c> column is an
/// <c>INullableColumn&lt;string&gt;</c>. Obtain this view by pattern-matching a column, e.g.
/// <c>if (column is INullableColumn&lt;int&gt; nullable)</c>.
/// </para>
///
/// <para>
/// Naming the wrong type argument (<c>INullableColumn&lt;int?&gt;</c> for that column) compiles with no warning
/// and never matches, so the caller silently falls back to whatever its <c>else</c> branch does. Match the
/// non-generic <see cref="INullableColumn"/> when the inner type is not certain.
/// </para>
/// </summary>
/// <typeparam name="T">The inner (non-nullable) element type; <see cref="Inner"/> is a column of these.</typeparam>
public interface INullableColumn<T> : INullableColumn
{
    /// <summary>The dense inner column, typed. See <see cref="INullableColumn.Inner"/> for the layout and the borrowing rule.</summary>
    new IColumn<T> Inner { get; }

    /// <inheritdoc/>
    IColumn INullableColumn.Inner => Inner;
}
