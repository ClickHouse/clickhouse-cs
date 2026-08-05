using System;
using System.Buffers;
using System.Text;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A <c>FixedString(N)</c> column: every row is exactly <c>N</c> bytes on the wire (no length prefix), so the
/// rows are kept back-to-back in one pooled blob at a fixed stride and a row's bytes are the slice
/// <c>[row * N, (row + 1) * N)</c>. Like <c>String</c>, <c>FixedString</c> is byte-oriented (not necessarily
/// UTF-8, and commonly holds fixed binary such as hashes), so the bytes are retained verbatim and a caller
/// chooses how to read each row: the raw bytes (<see cref="GetBytes(int)"/>, zero-copy), a string under an
/// explicit encoding (<see cref="GetString(int, Encoding)"/>), or — the default <see cref="IColumn{T}"/> view —
/// a per-row <see cref="byte"/> array. A decoded row always has exactly <c>N</c> bytes, any trailing zeros a
/// shorter stored value was padded with included.
///
/// <para>
/// The blob is rented from <see cref="ArrayPool{T}"/> and returned on <see cref="Dispose"/>; like every column,
/// the bytes and any span returned by <see cref="GetBytes(int)"/> are borrowed for the block's lifetime. Copy
/// out (<see cref="GetString(int, Encoding)"/> or <c>GetBytes(row).ToArray()</c>) to retain.
/// </para>
/// </summary>
internal sealed class FixedStringColumn : IColumn<byte[]>
{
    private readonly int size;
    private readonly int rowCount;
    private readonly bool pooled;
    private byte[] blob;
    private byte[][] cache;

    /// <summary>Initializes a column over a raw-bytes blob laid out at a fixed stride.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string (e.g. <c>FixedString(16)</c>).</param>
    /// <param name="size">The fixed per-row byte width <c>N</c>.</param>
    /// <param name="blob">The concatenated row bytes (may be longer than used); row <c>i</c> is <c>[i * N, (i + 1) * N)</c>.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooled">Whether <paramref name="blob"/> was rented and should be returned on dispose.</param>
    public FixedStringColumn(string name, string typeName, int size, byte[] blob, int rowCount, bool pooled)
    {
        Name = name;
        TypeName = typeName;
        this.size = size;
        this.blob = blob ?? throw new ArgumentNullException(nameof(blob));
        this.rowCount = rowCount;
        this.pooled = pooled;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => rowCount;

    /// <summary>The fixed per-row byte width <c>N</c> — the stride the rows sit at in the blob.</summary>
    public int Size => size;

    /// <summary>
    /// The rows as per-row <see cref="byte"/> arrays, materialized once and cached. Prefer
    /// <see cref="GetBytes(int)"/> to avoid allocating one array per row when the bytes can be read in place.
    /// </summary>
    public ReadOnlySpan<byte[]> Values
    {
        get
        {
            if (cache is null)
            {
                // Rent rather than allocate: this is a convenience view consumers copy out of, so it only needs
                // to live until Dispose returns it to the pool. Single-consumer per connection, so the lazy fill
                // needs no synchronization. The rented buffer may be longer than rowCount; Values slices to it.
                byte[][] decoded = ArrayPool<byte[]>.Shared.Rent(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    decoded[i] = GetBytes(i).ToArray();
                }

                cache = decoded;
            }

            return cache.AsSpan(0, rowCount);
        }
    }

    /// <inheritdoc/>
    // The cache is rented and may be longer than rowCount, so slice before indexing to keep an out-of-range row
    // failing fast rather than returning a stale slot; the uncached path is bounded by GetBytes.
    public byte[] this[int row] => cache is not null ? cache.AsSpan(0, rowCount)[row] : GetBytes(row).ToArray();

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>Returns the raw bytes of a row as a zero-copy slice of the blob (borrowed), always <c>N</c> bytes.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The row's bytes.</returns>
    public ReadOnlySpan<byte> GetBytes(int row)
    {
        // Bound the row against rowCount, not the blob: the blob is rented and may be longer, so slicing it
        // directly would let an out-of-range row read a stale pooled region instead of failing fast.
        if ((uint)row >= (uint)rowCount)
        {
            throw new IndexOutOfRangeException();
        }

        return blob.AsSpan(row * size, size);
    }

    /// <summary>
    /// Returns the bytes of the row range <c>[start, start + length)</c> as one zero-copy slice of the blob
    /// (borrowed), exactly <c>length * N</c> bytes. The rows sit back-to-back at the same stride the wire uses, so
    /// a codec can blit a whole range in one copy rather than walking it row by row.
    /// </summary>
    /// <param name="start">The zero-based first row of the range.</param>
    /// <param name="length">The number of rows in the range.</param>
    /// <returns>The range's bytes.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The range lies outside the column's rows.</exception>
    public ReadOnlySpan<byte> GetBytes(int start, int length)
    {
        // Bound the range against rowCount, not the blob: the blob is rented and may be longer, so slicing it
        // directly would let an over-long range read a stale pooled region instead of failing fast. The products
        // cannot overflow — the read path sized the blob with a checked rowCount * size, and this range fits in it.
        if (start < 0 || length < 0 || start + (long)length > rowCount)
        {
            // Blame whichever argument is itself out of range; a pair that is only jointly too long is the range's
            // fault, so anchor that on start.
            throw new ArgumentOutOfRangeException(
                length < 0 ? nameof(length) : nameof(start),
                $"Rows [{start}, {start + (long)length}) lie outside the {rowCount} row(s) of column '{Name}'.");
        }

        return blob.AsSpan(start * size, length * size);
    }

    /// <summary>Decodes a row's bytes to a string under the given encoding.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <param name="encoding">The encoding to decode with.</param>
    /// <returns>The decoded string.</returns>
    public string GetString(int row, Encoding encoding)
    {
        ArgumentNullException.ThrowIfNull(encoding);
        return encoding.GetString(GetBytes(row));
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (pooled && blob.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(blob);
        }

        blob = Array.Empty<byte>();

        if (cache is not null)
        {
            // The elements are byte[] references, so clear on return to avoid the pool pinning decoded rows.
            ArrayPool<byte[]>.Shared.Return(cache, clearArray: true);
            cache = null;
        }
    }
}
