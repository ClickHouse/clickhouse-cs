using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Exposes a decoded <c>Dynamic</c> column as borrowed type names, row discriminators, local indices, and one
/// child column per runtime type. <see cref="TypeCount"/> is the NULL discriminator.
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
