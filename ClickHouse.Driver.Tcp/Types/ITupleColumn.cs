using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The read surface of a decoded <c>Tuple(...)</c> column. A tuple is stored on the wire — and here — as its N
/// element columns laid side by side, one per tuple element in declaration order; the column materializes each
/// row as a <c>ValueTuple</c> pairing the elements (see the typed <see cref="IColumn{T}.Values"/>). This
/// interface also exposes the underlying per-element child columns, so a caller can read a single element's
/// values without materializing the whole tuple, and can recurse into a child that is itself a composite (e.g. a
/// tuple nesting an <see cref="IArrayColumn{TElement}"/>).
///
/// <para>
/// When returned by the client, child columns are borrowed views over the owning block's storage:
/// read them in place, and copy out (e.g. <c>ToArray()</c>) only what must outlive the block.
/// </para>
/// </summary>
public interface ITupleColumn : IColumn
{
    /// <summary>
    /// The child columns, one per tuple element, in declaration order. Each is an <see cref="IColumn{T}"/> of
    /// that element's type with the tuple's row count. Borrowed views valid only while the owning block is alive.
    /// </summary>
    IReadOnlyList<IColumn> Children { get; }

    /// <summary>
    /// The element names for a named tuple (<c>Tuple(a Int32, b String)</c>) — one entry per element, aligned with
    /// <see cref="Children"/>. <b>Empty, never null</b>, for an unnamed tuple (<c>Tuple(Int32, String)</c>), so it
    /// can be enumerated without a null check; test <see cref="System.Collections.Generic.IReadOnlyCollection{T}.Count"/>
    /// to tell the two apart. A tuple is named or unnamed as a whole — the server rejects a type that names some
    /// elements and not others — so every entry of a non-empty list is a name. Names are metadata only: they do
    /// not affect the wire layout or the materialized <c>ValueTuple</c> value.
    /// </summary>
    IReadOnlyList<string> FieldNames { get; }
}
