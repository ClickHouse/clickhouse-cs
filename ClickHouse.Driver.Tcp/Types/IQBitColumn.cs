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
/// <see cref="float"/>[] (<see cref="double"/>[] for <c>QBit(Float64, N)</c>, <see cref="sbyte"/>[] for
/// <c>QBit(Int8, N)</c>), which is convenient but
/// reverses the layout the type exists to provide. This interface exposes the planes as stored, so a caller
/// computing a reduced-precision distance can read the few planes it needs without materializing any vector.
/// </para>
///
/// <para>
/// Obtain it by pattern-matching a column, e.g. <c>if (column is IQBitColumn qbit)</c>. It is not generic: the
/// planes are raw bits, so plane access does not depend on which CLR type the elements surface as.
/// </para>
/// </summary>
public interface IQBitColumn : IColumn
{
    /// <summary>The number of elements in each row's vector — the <c>N</c> of <c>QBit(T, N)</c>.</summary>
    int Dimension { get; }

    /// <summary>
    /// The number of bit planes, which is the width of one element on the wire: 8 for <c>Int8</c>, 16 for
    /// <c>BFloat16</c>, 32 for <c>Float32</c>, 64 for <c>Float64</c>.
    /// </summary>
    int BitWidth { get; }

    /// <summary>
    /// The elements one group of planes covers — the <c>stride</c> of <c>QBit(T, N, stride)</c>, and equal to
    /// <see cref="Dimension"/> for the two-argument type, which is the only form this client decodes today.
    ///
    /// <para>
    /// ClickHouse 26.7 added an optional stride that splits a row into <see cref="GroupCount"/> independent
    /// groups of <c>stride</c> elements, each carrying its own full set of <see cref="BitWidth"/> planes. A
    /// column of that shape currently fails to resolve, so <see cref="Stride"/> always reports
    /// <see cref="Dimension"/> — it exists so a caller reading planes is written against the general layout
    /// rather than against the single-group special case.
    /// </para>
    /// </summary>
    int Stride { get; }

    /// <summary>
    /// The number of plane groups a row is split into, <c>Dimension / Stride</c>. Always 1 today; see
    /// <see cref="Stride"/>.
    /// </summary>
    int GroupCount { get; }

    /// <summary>
    /// The bytes one row occupies within a single plane of a single group — <c>ceil(Stride / 8)</c>, which is
    /// <c>ceil(Dimension / 8)</c> while <see cref="GroupCount"/> is 1.
    ///
    /// <para>
    /// Within group <c>g</c>, element <c>i</c> of a row sits at bit <c>i % 8</c> of byte
    /// <c>BytesPerRow - 1 - i / 8</c>, counting <c>i</c> from the start of the group: the bits within a byte run
    /// least significant first, but the bytes run in the <b>reverse</b> of the element order, so elements 0-7 are
    /// in the <em>last</em> byte. Equivalently, the row's bitmap is the big-endian encoding of a
    /// <c>BytesPerRow</c>-byte integer whose bit <c>i</c> is element <c>i</c>. When <see cref="Stride"/> is not a
    /// multiple of 8 the unused bits are the high bits of byte 0.
    /// </para>
    /// </summary>
    int BytesPerRow { get; }

    /// <summary>
    /// One bit plane: the bit at position <paramref name="bit"/> of every element of every row, as
    /// <see cref="IColumn.RowCount"/> consecutive <see cref="BytesPerRow"/>-byte bitmaps. Row <c>r</c>'s bitmap is
    /// the slice <c>[r * BytesPerRow, (r + 1) * BytesPerRow)</c>.
    ///
    /// <para>
    /// Defined only for a single-group column, which is every column this client decodes today. On a strided
    /// column a plane is <see cref="GroupCount"/> disjoint runs and no single span can be "the plane", so this
    /// throws rather than quietly hand back one group's worth; use <see cref="GetPlane(int, int)"/> there.
    /// </para>
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
    /// <exception cref="InvalidOperationException"><see cref="GroupCount"/> is not 1, so a plane is not one contiguous run.</exception>
    ReadOnlySpan<byte> GetPlane(int bit);

    /// <summary>
    /// One bit plane of one group: the bit at position <paramref name="bit"/> of every element in group
    /// <paramref name="group"/>, for every row, as <see cref="IColumn.RowCount"/> consecutive
    /// <see cref="BytesPerRow"/>-byte bitmaps. Group <c>g</c> covers elements
    /// <c>[g * Stride, (g + 1) * Stride)</c>.
    ///
    /// <para>
    /// This is the general form; <see cref="GetPlane(int)"/> is the <see cref="GroupCount"/> == 1 shorthand for
    /// it. Prefer this overload in code that should keep working when strided columns become readable.
    /// </para>
    ///
    /// <para>
    /// A borrowed span over the owning block's storage, valid only while the block is alive.
    /// </para>
    /// </summary>
    /// <param name="bit">The bit position, from 0 (least significant) to <see cref="BitWidth"/> - 1 (the sign bit).</param>
    /// <param name="group">The group index, from 0 to <see cref="GroupCount"/> - 1.</param>
    /// <returns>The group's plane bitmaps, <c>RowCount * BytesPerRow</c> bytes.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bit"/> or <paramref name="group"/> is out of range.</exception>
    ReadOnlySpan<byte> GetPlane(int bit, int group);
}
