using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The columnar read surface of a decoded <c>Dynamic</c> column: the runtime type-name list discovered on the wire, a
/// per-row discriminator stream, and one child column per runtime type, each holding only the values of the rows that
/// selected it, in row order. It is a read view only — implementing it does not make a column insertable, because the
/// codec's zero-copy write path accepts only columns this driver decoded, whose invariants it has checked.
///
/// <para>
/// Like <see cref="IVariantColumn"/>, a <c>Dynamic</c> has no useful materialized element type — its
/// <see cref="IColumn{T}"/> surface is <c>IColumn&lt;object&gt;</c>, so every row read through it is boxed — and the
/// columnar view is the typed way in. Row <c>i</c>'s value lives at <c>LocalIndices[i]</c> within
/// <c>GetTypeColumn(Discriminators[i])</c>. Unlike <c>Variant</c>, whose NULL discriminator is the fixed value 255,
/// a <c>Dynamic</c> marks NULL with <see cref="TypeCount"/> — one past the last type — because the type list is
/// discovered per block rather than declared. A NULL row occupies no slot in any child.
/// </para>
///
/// <para>
/// The type names are the wire's own spelling of each runtime type, in discriminator order, so a caller can decide
/// how to read a child without inspecting its values. Child columns and both spans are borrowed views over the
/// owning block's storage: read them in place, and copy out only what must outlive the block. Obtain this view by
/// pattern-matching a column, e.g. <c>if (column is IDynamicColumn dynamicColumn)</c>.
/// </para>
/// </summary>
public interface IDynamicColumn : IColumn
{
    /// <summary>
    /// The number of runtime types; also the NULL discriminator value, since NULL is encoded as one past the last
    /// type rather than a fixed sentinel.
    /// </summary>
    int TypeCount { get; }

    /// <summary>
    /// The runtime type names, in wire (discriminator) order — the ClickHouse type string for each child column,
    /// which is how a caller knows what to read that child as. Read-only; the underlying storage is not exposed.
    /// </summary>
    IReadOnlyList<string> TypeNames { get; }

    /// <summary>One discriminator per row; <see cref="TypeCount"/> marks a NULL row.</summary>
    ReadOnlySpan<int> Discriminators { get; }

    /// <summary>
    /// Each row's index into its selected type's child column (the count of that discriminator in the rows before
    /// it), precomputed once; a NULL row's entry is <c>-1</c>. Lets a caller price or address a row in O(1)
    /// rather than rescanning the discriminators.
    /// </summary>
    ReadOnlySpan<int> LocalIndices { get; }

    /// <summary>
    /// The child column for the given discriminator (holding the values of the rows that selected it). A borrowed
    /// view valid only while the owning block is alive — it is the block's to dispose, never the caller's.
    /// </summary>
    /// <param name="discriminator">The runtime-type index. Must be a real type index: the NULL discriminator (<see cref="TypeCount"/>) selects no column, so guard for it before calling.</param>
    /// <returns>That type's child column.</returns>
    /// <exception cref="IndexOutOfRangeException"><paramref name="discriminator"/> is negative or not less than <see cref="TypeCount"/> — which includes passing the NULL discriminator.</exception>
    IColumn GetTypeColumn(int discriminator);
}
