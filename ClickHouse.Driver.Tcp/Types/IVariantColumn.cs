using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The columnar read surface of a decoded <c>Variant(T1, ..., Tn)</c> column: a per-row discriminator stream plus one
/// child column per alternative type, each holding only the values of the rows that selected it, in row order. That
/// is exactly the wire layout. It is a read view only — implementing it does not make a column insertable, because
/// the codec's zero-copy write path accepts only columns this driver decoded, whose invariants it has checked.
///
/// <para>
/// Unlike the other composites, a <c>Variant</c> has no useful materialized element type — the
/// <see cref="IColumn{T}"/> surface is <c>IColumn&lt;object&gt;</c>, so every row read through it is boxed. Reading
/// columnar avoids that: dispatch on <see cref="Discriminators"/>, then read the selected type's child column,
/// which is typed. Row <c>i</c>'s value lives at <c>LocalIndices[i]</c> within
/// <c>GetTypeColumn(Discriminators[i])</c>, unless its discriminator is
/// <see cref="NullDiscriminator"/>, in which case the row is NULL and occupies no slot in any child.
/// </para>
///
/// <para>
/// Child columns and both spans are borrowed views over the owning block's storage: read them in place, and copy out
/// only what must outlive the block. A child whose type is itself a composite pattern-matches to that type's own
/// columnar view. Obtain this view by pattern-matching a column, e.g. <c>if (column is IVariantColumn variant)</c>.
/// </para>
/// </summary>
public interface IVariantColumn : IColumn
{
    /// <summary>The discriminator value marking a NULL row; it selects no alternative type.</summary>
    public const byte NullDiscriminator = 255;

    /// <summary>The number of alternative types.</summary>
    int TypeCount { get; }

    /// <summary>One discriminator per row; <see cref="NullDiscriminator"/> marks a NULL row.</summary>
    ReadOnlySpan<byte> Discriminators { get; }

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
    /// <param name="discriminator">The alternative-type index. Must be a real type index: <see cref="NullDiscriminator"/> selects no column, so guard for it before calling.</param>
    /// <returns>That type's child column.</returns>
    /// <exception cref="IndexOutOfRangeException"><paramref name="discriminator"/> is negative or not less than <see cref="TypeCount"/> — which includes passing <see cref="NullDiscriminator"/>.</exception>
    IColumn GetTypeColumn(int discriminator);
}
