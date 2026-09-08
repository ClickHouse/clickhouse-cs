using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A <c>Time</c> column that surfaces the raw wire seconds. ClickHouse writes a little-endian <c>Int32</c> second
/// count per row (a signed time-of-day/duration, not tied to a date; range [-999:59:59, 999:59:59]); those bytes
/// are kept verbatim as the column's storage and exposed — with no copy — as the <see cref="int"/> <see cref="Values"/>.
///
/// <para>
/// A caller that wants a <see cref="TimeSpan"/> can call <see cref="GetTimeSpan"/> or <see cref="ToTimeSpans"/>;
/// that projection is exact (a <see cref="TimeSpan"/> represents whole-second durations without loss).
/// </para>
///
/// <para>
/// The backing buffer is rented from <see cref="ArrayPool{T}"/> and returned on <see cref="Dispose"/>; like every
/// column, the bytes and the span returned by <see cref="Values"/> are borrowed for the block's lifetime. Copy
/// out to retain.
/// </para>
/// </summary>
internal sealed class TimeColumn : IColumn<int>
{
    private readonly int length;
    private readonly bool pooled;
    private byte[] buffer;

    /// <summary>Initializes a column over the raw little-endian second bytes.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="buffer">The little-endian column bytes (may be longer than <paramref name="length"/>).</param>
    /// <param name="length">The logical byte length; must be a whole multiple of <c>sizeof(int)</c>.</param>
    /// <param name="pooled">Whether <paramref name="buffer"/> was rented and should be returned on dispose.</param>
    public TimeColumn(string name, string typeName, byte[] buffer, int length, bool pooled)
    {
        Name = name;
        TypeName = typeName;
        this.buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
        this.length = length;
        this.pooled = pooled;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => length / sizeof(int);

    /// <summary>The raw signed second counts, as a zero-copy view. Use <see cref="ToTimeSpans"/> for a
    /// <see cref="TimeSpan"/> view.</summary>
    public ReadOnlySpan<int> Values => MemoryMarshal.Cast<byte, int>(buffer.AsSpan(0, length));

    /// <inheritdoc/>
    public int this[int row] => Values[row];

    /// <inheritdoc/>
    public object GetValue(int row) => Values[row];

    /// <summary>Returns a row as a <see cref="TimeSpan"/> (exact — <c>Time</c> is a whole-second duration).</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The duration.</returns>
    public TimeSpan GetTimeSpan(int row) => TimeSpan.FromSeconds(Values[row]);

    /// <summary>
    /// Projects every row to a <see cref="TimeSpan"/>, as a freshly allocated array the caller owns (it outlives
    /// the block, unlike <see cref="Values"/>).
    /// </summary>
    /// <returns>One <see cref="TimeSpan"/> per row, in row order.</returns>
    public TimeSpan[] ToTimeSpans()
    {
        ReadOnlySpan<int> seconds = Values;
        var result = new TimeSpan[seconds.Length];
        for (int i = 0; i < seconds.Length; i++)
        {
            result[i] = TimeSpan.FromSeconds(seconds[i]);
        }

        return result;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (pooled && buffer.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        buffer = Array.Empty<byte>();
    }

    /// <summary>
    /// Reads a <c>Time</c> column: bulk-reads the raw second bytes into a pooled buffer that becomes the column's
    /// storage. The buffer is returned to the pool if the read throws, so no rent leaks on failure.
    /// </summary>
    /// <param name="reader">The reader positioned at the column body.</param>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="rowCount">The number of values to read.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded, pooled column.</returns>
    public static async ValueTask<IColumn> ReadAsync(
        ClickHouseBinaryReader reader,
        string name,
        string typeName,
        int rowCount,
        CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            return new TimeColumn(name, typeName, Array.Empty<byte>(), length: 0, pooled: false);
        }

        int byteCount = checked(rowCount * sizeof(int));
        byte[] rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            await reader.ReadBytesAsync(rented.AsMemory(0, byteCount), cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(rented);
            throw;
        }

        return new TimeColumn(name, typeName, rented, byteCount, pooled: true);
    }
}
