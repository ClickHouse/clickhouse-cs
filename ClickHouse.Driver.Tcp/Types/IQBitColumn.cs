using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The columnar read surface of a decoded <c>QBit(T, N)</c> column. A QBit row is an <c>N</c>-element vector
/// stored with its <b>bit planes transposed</b>: rather than the elements of a row sitting together, the column
/// holds one bitmap per bit position, and a bitmap carries that one bit of every element of every row. That is
/// what lets a vector search read only the high-order planes and compute an approximate distance at reduced
/// precision, which is how the server's <c>L2DistanceTransposed</c> / <c>cosineDistanceTransposed</c> work.
///
/// <para>
/// The default <see cref="IColumn{T}"/> view undoes the transposition and hands back a per-row
/// <see cref="float"/>[] (or <see cref="double"/>[] for <c>QBit(Float64, N)</c>), which is convenient but
/// reverses the layout the type exists to provide. This interface exposes the planes as stored, so a caller
/// computing a reduced-precision distance can read the few planes it needs without materializing any vector.
/// </para>
///
/// <para>
/// Obtain it by pattern-matching a column, e.g. <c>if (column is IQBitColumn qbit)</c>. It is not generic: the
/// planes are raw bits, so plane access does not depend on whether the elements surface as
/// <see cref="float"/> or <see cref="double"/>.
/// </para>
/// </summary>
public interface IQBitColumn : IColumn
{
    /// <summary>The number of elements in each row's vector — the <c>N</c> of <c>QBit(T, N)</c>.</summary>
    int Dimension { get; }

    /// <summary>
    /// The number of bit planes, which is the width of one element on the wire: 16 for <c>BFloat16</c>, 32 for
    /// <c>Float32</c>, 64 for <c>Float64</c>.
    /// </summary>
    int BitWidth { get; }

    /// <summary>
    /// The bytes one row occupies within a single plane — <c>ceil(Dimension / 8)</c>. Element <c>i</c> of a row
    /// sits at bit <c>i % 8</c> of byte <c>i / 8</c> of the row's slice, least-significant bit first. When
    /// <see cref="Dimension"/> is not a multiple of 8 the high bits of the last byte are unused.
    /// </summary>
    int BytesPerRow { get; }

    /// <summary>
    /// One bit plane: the bit at position <paramref name="bit"/> of every element of every row, as
    /// <see cref="IColumn.RowCount"/> consecutive <see cref="BytesPerRow"/>-byte bitmaps. Row <c>r</c>'s bitmap is
    /// the slice <c>[r * BytesPerRow, (r + 1) * BytesPerRow)</c>.
    ///
    /// <para>
    /// <paramref name="bit"/> is the bit's <em>significance</em> within the stored element, so
    /// <c>BitWidth - 1</c> is the sign bit and 0 the least significant mantissa bit; the most significant planes
    /// are the ones a reduced-precision distance wants. (The wire stores the planes in the opposite order, most
    /// significant first — this accessor hides that.) For <c>QBit(BFloat16, N)</c> the positions are those of the
    /// 16-bit brain-float, not of the widened <see cref="float"/> the values surface as.
    /// </para>
    ///
    /// <para>
    /// A borrowed span over the owning block's storage, valid only while the block is alive: read it in place and
    /// copy out only what must outlive the block.
    /// </para>
    /// </summary>
    /// <param name="bit">The bit position, from 0 (least significant) to <see cref="BitWidth"/> - 1 (the sign bit).</param>
    /// <returns>The plane's bitmaps, <c>RowCount * BytesPerRow</c> bytes.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bit"/> is outside [0, <see cref="BitWidth"/>).</exception>
    ReadOnlySpan<byte> GetPlane(int bit);
}
