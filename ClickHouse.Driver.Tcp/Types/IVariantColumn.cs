using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Exposes a decoded <c>Variant</c> column as borrowed row discriminators, local indices, and one child column
/// per alternative. <see cref="NullDiscriminator"/> marks NULL rows.
/// </summary>
public interface IVariantColumn : IColumn
{
    /// <summary>The discriminator value marking a NULL row; it selects no alternative type.</summary>
    public const byte NullDiscriminator = 255;

    /// <summary>The number of alternative types.</summary>
    int TypeCount { get; }

    /// <summary>
    /// The alternative type names, in discriminator order — the ClickHouse type string of each child column, so a
    /// caller knows what a discriminator selects without inspecting the values. Entry <c>i</c> is the same string
    /// as <c>GetTypeColumn(i).TypeName</c>.
    ///
    /// <para>
    /// This is the only route to the mapping. The server canonicalizes the alternatives into name-sorted order and
    /// that sorted order is the discriminator order, so the order the type was declared in says nothing; and the
    /// type string on the column header need not list the alternatives at all — a <c>Geometry</c> column's header
    /// is the single word <c>Geometry</c>.
    /// </para>
    /// </summary>
    IReadOnlyList<string> TypeNames { get; }

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
