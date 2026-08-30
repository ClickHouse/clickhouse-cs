using System;
using System.Text;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The read surface of a decoded <c>String</c> column, over the bytes the wire carried. A ClickHouse
/// <c>String</c> is a byte string, not text: it holds any byte sequence, need not be UTF-8, and may contain
/// embedded NULs. The <see cref="IColumn{T}"/> surface decodes each row as UTF-8, which is what almost every
/// caller wants and is lossy for anything else — a byte that UTF-8 cannot spell comes back as U+FFFD, and the
/// original byte is gone. This interface is the undamaged reading.
///
/// <para>
/// The column keeps every row's bytes in one blob with per-row offsets — not the wire's own form, which
/// interleaves a length prefix with each row's bytes, but the decoded storage the reader builds from it. Row
/// <c>i</c> is <c>Bytes.Slice(Offsets[i], Offsets[i + 1] - Offsets[i])</c>, and <see cref="GetBytes"/> is that
/// slice.
/// <see cref="GetString"/> decodes a row under an encoding of the caller's choosing, for data that is text in
/// something other than UTF-8.
/// </para>
///
/// <para>
/// Every span here is a borrowed view over the owning block's storage: read it in place, and copy out
/// (<c>ToArray()</c>) only what must outlive the block. Obtain this view by pattern-matching a column, e.g.
/// <c>if (column is IStringColumn text)</c>. To write bytes back, build the column from a <c>byte[]</c> per row
/// (<see cref="ClickHouseTcpColumn.Create{T}(string, T[])"/>), which a <c>String</c> target accepts and stores
/// verbatim.
/// </para>
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
