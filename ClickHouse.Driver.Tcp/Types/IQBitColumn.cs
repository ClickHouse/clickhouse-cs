using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Exposes the transposed bit planes of a decoded <c>QBit(T, N)</c> column without materializing row vectors.
/// The corresponding typed column exposes <see cref="float"/>[] for <c>BFloat16</c> and <c>Float32</c>,
/// <see cref="double"/>[] for <c>Float64</c>, or <see cref="sbyte"/>[] for <c>Int8</c> on ClickHouse 26.7+.
/// </summary>
public interface IQBitColumn : IColumn
{
    /// <summary>Gets the number of elements in each row.</summary>
    int Dimension { get; }

    /// <summary>
    /// Gets the number of planes: 8 for <c>Int8</c>, 16 for <c>BFloat16</c>, 32 for <c>Float32</c>, or 64 for
    /// <c>Float64</c>.
    /// </summary>
    int BitWidth { get; }

    /// <summary>
    /// Gets the number of elements represented by each plane group. Columns decoded by this driver report
    /// <see cref="Dimension"/> because strided <c>QBit(T, N, stride)</c> columns, available since ClickHouse
    /// 26.7, are unsupported.
    /// </summary>
    int Stride { get; }

    /// <summary>Gets <c>Dimension / Stride</c>, the number of plane groups in each row.</summary>
    int GroupCount { get; }

    /// <summary>
    /// Gets the size of one row bitmap, <c>ceil(Stride / 8)</c> bytes. Element <c>i</c> within a group is bit
    /// <c>i % 8</c> of byte <c>BytesPerRow - 1 - i / 8</c>. Unused bits are the high bits of the first byte.
    /// </summary>
    int BytesPerRow { get; }

    /// <summary>
    /// Gets one plane as consecutive row bitmaps. Row <c>r</c> occupies
    /// <c>[r * BytesPerRow, (r + 1) * BytesPerRow)</c>. The bit number denotes significance, with 0 the least
    /// significant stored bit and <c>BitWidth - 1</c> the sign bit. For <c>BFloat16</c>, it refers to the 16-bit
    /// stored value rather than the widened <see cref="float"/>. The returned span borrows the column's storage
    /// and is valid only for the owning block's lifetime.
    /// </summary>
    /// <param name="bit">The bit position in the stored element.</param>
    /// <returns><c>RowCount * BytesPerRow</c> bytes.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bit"/> is outside
    /// [0, <see cref="BitWidth"/>).</exception>
    /// <exception cref="InvalidOperationException">The column contains more than one plane group.</exception>
    ReadOnlySpan<byte> GetPlane(int bit);

    /// <summary>
    /// Gets one plane for one group as consecutive row bitmaps. Group <c>g</c> covers elements
    /// <c>[g * Stride, (g + 1) * Stride)</c>. Bit numbering matches <see cref="GetPlane(int)"/>. The returned span
    /// borrows the column's storage and is valid only for the owning block's lifetime.
    /// </summary>
    /// <param name="bit">The bit position in the stored element.</param>
    /// <param name="group">The zero-based group index.</param>
    /// <returns><c>RowCount * BytesPerRow</c> bytes.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bit"/> or <paramref name="group"/> is out
    /// of range.</exception>
    ReadOnlySpan<byte> GetPlane(int bit, int group);
}
