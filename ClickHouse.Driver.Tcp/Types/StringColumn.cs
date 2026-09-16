using System;
using System.Buffers;
using System.Text;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A <c>String</c> column that retains the raw wire bytes and decodes to text on demand. ClickHouse
/// <c>String</c> is byte-oriented (not necessarily UTF-8, and may contain embedded NULs), so the bytes are kept
/// verbatim in one pooled blob with per-row offsets, and a caller chooses how to read each row: the raw bytes
/// (<see cref="GetBytes"/>, zero-copy), a string under an explicit encoding (<see cref="GetString(int, Encoding)"/>),
/// or — the default <see cref="IColumn{T}"/> view — a UTF-8 string.
///
/// <para>
/// The blob and offsets are rented from <see cref="ArrayPool{T}"/> and returned on <see cref="Dispose"/>; like
/// every column, the bytes and any span returned by <see cref="GetBytes"/> are borrowed for the block's
/// lifetime. Copy out (<see cref="GetString(int, Encoding)"/> or <c>GetBytes(row).ToArray()</c>) to retain.
/// </para>
/// </summary>
internal sealed class StringColumn : IColumn<string>
{
    private readonly int rowCount;
    private readonly bool pooled;
    private byte[] blob;
    private int[] offsets;
    private string[] utf8Cache;

    /// <summary>Initializes a column over a raw-bytes blob and its per-row offsets.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="blob">The concatenated row bytes (may be longer than used).</param>
    /// <param name="offsets">The per-row start offsets; must have <paramref name="rowCount"/> + 1 entries, the last being the total byte length.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooled">Whether <paramref name="blob"/> and <paramref name="offsets"/> were rented and should be returned on dispose.</param>
    public StringColumn(string name, string typeName, byte[] blob, int[] offsets, int rowCount, bool pooled)
    {
        Name = name;
        TypeName = typeName;
        this.blob = blob ?? throw new ArgumentNullException(nameof(blob));
        this.offsets = offsets ?? throw new ArgumentNullException(nameof(offsets));
        this.rowCount = rowCount;
        this.pooled = pooled;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => rowCount;

    /// <summary>
    /// The rows decoded as UTF-8, materialized once and cached. Prefer <see cref="GetBytes"/> or
    /// <see cref="GetString(int, Encoding)"/> to avoid building this array when only some rows, the raw bytes,
    /// or a different encoding are needed.
    /// </summary>
    public ReadOnlySpan<string> Values
    {
        get
        {
            if (utf8Cache is null)
            {
                // Rent rather than allocate: this is a convenience view consumers copy out of, so it only needs
                // to live until Dispose returns it to the pool. Single-consumer per connection, so the lazy fill
                // needs no synchronization. The rented buffer may be longer than rowCount; Values slices to it.
                string[] decoded = ArrayPool<string>.Shared.Rent(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    decoded[i] = Utf8(i);
                }

                utf8Cache = decoded;
            }

            return utf8Cache.AsSpan(0, rowCount);
        }
    }

    /// <inheritdoc/>
    // The cache is rented and may be longer than rowCount, so slice before indexing to keep an out-of-range row
    // failing fast rather than returning a stale slot; the uncached path is bounded by GetBytes.
    public string this[int row] => utf8Cache is not null ? utf8Cache.AsSpan(0, rowCount)[row] : Utf8(row);

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>Returns the raw, undecoded bytes of a row as a zero-copy slice of the blob (borrowed).</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The row's bytes.</returns>
    public ReadOnlySpan<byte> GetBytes(int row)
    {
        // Bound the row against the logical offsets (rowCount + 1 entries): the offsets array is rented and may be
        // longer, so indexing it directly would let an out-of-range row read a stale offset pair instead of throwing.
        ReadOnlySpan<int> bounds = offsets.AsSpan(0, rowCount + 1);
        return blob.AsSpan(bounds[row], bounds[row + 1] - bounds[row]);
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
        if (pooled)
        {
            if (blob.Length != 0)
            {
                ArrayPool<byte>.Shared.Return(blob);
            }

            if (offsets.Length != 0)
            {
                ArrayPool<int>.Shared.Return(offsets);
            }
        }

        blob = Array.Empty<byte>();
        offsets = Array.Empty<int>();

        if (utf8Cache is not null)
        {
            // string holds references, so clear on return to avoid the pool pinning decoded strings.
            ArrayPool<string>.Shared.Return(utf8Cache, clearArray: true);
            utf8Cache = null;
        }
    }

    private string Utf8(int row) => Encoding.UTF8.GetString(GetBytes(row));
}
