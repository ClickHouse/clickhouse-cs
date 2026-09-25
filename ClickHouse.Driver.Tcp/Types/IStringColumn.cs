using System;
using System.Text;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Provides lossless access to a decoded <c>String</c> column's bytes. ClickHouse strings may contain arbitrary
/// bytes; the ordinary <see cref="IColumn{T}"/> surface decodes UTF-8 and can replace invalid sequences. Rows are
/// slices of <see cref="Bytes"/> delimited by <see cref="Offsets"/>. These spans borrow block storage.
/// <c>Block.ReadAs&lt;byte[]&gt;</c> and <c>byte[]</c> POCO properties instead return copied rows; dictionary-backed
/// rows may share the same array and must be treated as read-only.
/// </summary>
public interface IStringColumn : IColumn<string>
{
    /// <summary>
    /// Every row's bytes concatenated end-to-end, addressed through <see cref="Offsets"/>. A borrowed span valid
    /// only while the owning block is alive.
    /// </summary>
    ReadOnlySpan<byte> Bytes { get; }

    /// <summary>
    /// The per-row offsets into <see cref="Bytes"/>: <c>[0]</c> is 0 and <c>[i + 1]</c> is the exclusive end of row
    /// <c>i</c>; the span has one more entry than the column has rows. A borrowed span valid only while the owning
    /// block is alive.
    /// </summary>
    ReadOnlySpan<int> Offsets { get; }

    /// <summary>The raw, undecoded bytes of one row — a slice of <see cref="Bytes"/>, copied nowhere.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>That row's bytes, borrowed for the block's lifetime.</returns>
    /// <exception cref="IndexOutOfRangeException"><paramref name="row"/> is negative or not less than <see cref="IColumn.RowCount"/>.</exception>
    ReadOnlySpan<byte> GetBytes(int row);

    /// <summary>Decodes one row's bytes under <paramref name="encoding"/>, rather than the UTF-8 the indexer uses.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <param name="encoding">The encoding to decode with.</param>
    /// <returns>The decoded string, owned by the caller.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="encoding"/> is null.</exception>
    /// <exception cref="IndexOutOfRangeException"><paramref name="row"/> is negative or not less than <see cref="IColumn.RowCount"/>.</exception>
    string GetString(int row, Encoding encoding);
}
