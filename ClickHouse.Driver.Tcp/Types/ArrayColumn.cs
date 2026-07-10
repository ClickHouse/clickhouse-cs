using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Converts the raw little-endian column bytes into the destination span of decoded values. The source is the
/// whole column body; the destination is exactly one entry per row. Invoked once per column, so the per-element
/// loop lives inside the fill rather than being dispatched per value.
/// </summary>
/// <typeparam name="T">The decoded CLR element type.</typeparam>
/// <param name="source">The raw column bytes.</param>
/// <param name="destination">The span to fill with decoded values, one per row.</param>
internal delegate void ColumnValueFill<T>(ReadOnlySpan<byte> source, Span<T> destination);

/// <summary>
/// A column backed by a typed array, for element types that are not a direct reinterpret of the wire bytes —
/// <c>String</c> (variable-length) and converted types such as <c>DateTime</c>. <see cref="Values"/> is a
/// borrowed view over the array.
/// </summary>
/// <typeparam name="T">The CLR element type.</typeparam>
internal sealed class ArrayColumn<T> : IColumn<T>, ISpanColumn<T>
{
    /// <summary>
    /// The backing array may be rented from <see cref="ArrayPool{T}"/> (via <see cref="ReadAsync"/>) and returned on
    /// <see cref="Dispose"/>, so it can be larger than the data; the logical range is tracked separately and
    /// <see cref="Values"/> slices to it. A caller-supplied array (the constructor) is self-owned and not pooled.
    /// A view returned by <see cref="Slice"/> shares the array at an offset and is never pooled. Once disposed
    /// the values must not be read.
    /// </summary>
    private readonly int offset;
    private readonly int length;
    private readonly bool pooled;
    private T[] values;

    /// <summary>Initializes a column that takes ownership of a caller-supplied (self-owned, unpooled) values array.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="values">The decoded values; its length is the row count.</param>
    /// <exception cref="ArgumentNullException"><paramref name="values"/> is null.</exception>
    public ArrayColumn(string name, string typeName, T[] values)
        : this(name, typeName, values ?? throw new ArgumentNullException(nameof(values)), offset: 0, values.Length, pooled: false)
    {
    }

    private ArrayColumn(string name, string typeName, T[] values, int offset, int length, bool pooled)
    {
        Name = name;
        TypeName = typeName;
        this.values = values;
        this.offset = offset;
        this.length = length;
        this.pooled = pooled;
    }

    /// <summary>
    /// Wraps the first <paramref name="length"/> elements of a caller-managed buffer (which may be longer, e.g.
    /// rented from <see cref="ArrayPool{T}"/>) as a non-owning column. The buffer is not returned on
    /// <see cref="Dispose"/>; the caller owns its lifetime.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="buffer">The backing buffer; only <paramref name="length"/> elements are exposed.</param>
    /// <param name="length">The logical row count.</param>
    /// <returns>A non-owning column view over the buffer.</returns>
    internal static ArrayColumn<T> OverBuffer(string name, string typeName, T[] buffer, int length)
        => new(name, typeName, buffer, offset: 0, length, pooled: false);

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => length;

    /// <inheritdoc/>
    public ReadOnlySpan<T> Values => new ReadOnlySpan<T>(values, offset, length);

    /// <inheritdoc/>
    ReadOnlySpan<T> ISpanColumn<T>.Span => Values;

    /// <inheritdoc/>
    // Index through the logical span, not the backing array: a pooled array can be longer than the row count,
    // so a direct values[offset + row] would return a stale slot for an out-of-range row instead of throwing.
    public T this[int row] => Values[row];

    /// <inheritdoc/>
    public object GetValue(int row) => Values[row];

    /// <inheritdoc/>
    public void Dispose()
    {
        if (pooled && values.Length != 0)
        {
            // Clear reference-bearing element types so a returned array does not pin what it last held.
            ArrayPool<T>.Shared.Return(values, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
        }

        values = Array.Empty<T>();
    }

    /// <summary>
    /// Reads a converted fixed-width column: bulk-reads <paramref name="byteCount"/> raw little-endian bytes into
    /// a pooled scratch buffer, converts them into a pooled destination array via <paramref name="fill"/>, and
    /// returns a column that owns and pools that array. The scratch buffer is always returned; the destination is
    /// returned too if the read or conversion throws, so no rented buffer leaks on failure.
    /// </summary>
    /// <param name="reader">The reader positioned at the column body.</param>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="rowCount">The number of values to read.</param>
    /// <param name="byteCount">The column body's total byte length.</param>
    /// <param name="fill">Converts the raw bytes into the decoded values.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded, pooled column.</returns>
    public static async ValueTask<IColumn> ReadAsync(
        ClickHouseBinaryReader reader,
        string name,
        string typeName,
        int rowCount,
        int byteCount,
        ColumnValueFill<T> fill,
        CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            return new ArrayColumn<T>(name, typeName, Array.Empty<T>(), offset: 0, length: 0, pooled: false);
        }

        T[] destination = ArrayPool<T>.Shared.Rent(rowCount);
        byte[] scratch = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            await reader.ReadBytesAsync(scratch.AsMemory(0, byteCount), cancellationToken).ConfigureAwait(false);
            fill(scratch.AsSpan(0, byteCount), destination.AsSpan(0, rowCount));
        }
        catch
        {
            ArrayPool<T>.Shared.Return(destination, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
            throw;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }

        return new ArrayColumn<T>(name, typeName, destination, offset: 0, rowCount, pooled: true);
    }
}
