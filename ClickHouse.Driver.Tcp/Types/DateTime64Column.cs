using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A <c>DateTime64(scale[, tz])</c> column that surfaces the raw wire counts. ClickHouse writes a little-endian
/// <c>Int64</c> tick count at <c>10^-scale</c> seconds since the Unix epoch per row; those bytes are kept
/// verbatim as the column's storage and exposed — with no copy — as the <see cref="long"/> <see cref="Values"/>,
/// the exact wire value at any scale (including scales 8 and 9, which are finer than a .NET tick). The scale and
/// timezone — the same for every row — live once on the column rather than on each value.
///
/// <para>
/// The backing buffer is rented from <see cref="ArrayPool{T}"/> and returned on <see cref="Dispose"/>; like every
/// column, the bytes and the span returned by <see cref="Values"/> are borrowed for the block's lifetime. Copy
/// out to retain.
/// </para>
/// </summary>
internal sealed class DateTime64Column : IColumn<long>, IStoredValuesColumn
{
    private readonly int scale;
    private readonly TimeZoneInfo timeZone;
    private readonly int length;
    private readonly bool pooled;
    private byte[] buffer;

    /// <summary>Initializes a column over the raw little-endian count bytes.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="scale">The fractional-second scale (0–9).</param>
    /// <param name="timeZone">The timezone values are presented in.</param>
    /// <param name="buffer">The little-endian column bytes (may be longer than <paramref name="length"/>).</param>
    /// <param name="length">The logical byte length; must be a whole multiple of <c>sizeof(long)</c>.</param>
    /// <param name="pooled">Whether <paramref name="buffer"/> was rented and should be returned on dispose.</param>
    public DateTime64Column(string name, string typeName, int scale, TimeZoneInfo timeZone, byte[] buffer, int length, bool pooled)
    {
        Name = name;
        TypeName = typeName;
        this.scale = scale;
        this.timeZone = timeZone;
        this.buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
        this.length = length;
        this.pooled = pooled;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => length / sizeof(long);

    /// <summary>The fractional-second scale (0–9) shared by every value in the column.</summary>
    public int Scale => scale;

    /// <summary>The timezone the counts are presented in, shared by every value in the column. Combine with
    /// <see cref="Scale"/> to interpret the raw <see cref="Values"/> counts.</summary>
    public TimeZoneInfo TimeZone => timeZone;

    /// <summary>
    /// The raw signed tick counts (at <c>10^-Scale</c> seconds since the epoch), as a zero-copy view. This is the
    /// exact wire value at every scale; use <see cref="ToDateTimeOffsets"/> for a (lossy) calendar view.
    /// </summary>
    public ReadOnlySpan<long> Values => MemoryMarshal.Cast<byte, long>(buffer.AsSpan(0, length));

    /// <inheritdoc/>
    public long this[int row] => Values[row];

    /// <inheritdoc/>
    public object GetValue(int row) => Values[row];

    /// <summary>Returns a row as a <see cref="DateTimeOffset"/> at 100 ns resolution, presented in the column's timezone.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The instant. Lossy for scales finer than 7 (sub-100 ns digits are truncated); read
    /// <see cref="Values"/> for the exact count.</returns>
    public DateTimeOffset GetDateTimeOffset(int row) => ToDateTimeOffset(Values[row]);

    /// <summary>
    /// Projects every row to a <see cref="DateTimeOffset"/> presented in the column's timezone, as a freshly
    /// allocated array the caller owns (it outlives the block, unlike <see cref="Values"/>).
    /// </summary>
    /// <returns>One <see cref="DateTimeOffset"/> per row, in row order.</returns>
    /// <remarks><see cref="DateTimeOffset"/> holds only 100 ns ticks, so this view is lossy for scales finer than 7
    /// (sub-100 ns digits are truncated toward zero); read <see cref="Values"/> for the exact counts.</remarks>
    public DateTimeOffset[] ToDateTimeOffsets()
    {
        ReadOnlySpan<long> counts = Values;
        var result = new DateTimeOffset[counts.Length];
        for (int i = 0; i < counts.Length; i++)
        {
            result[i] = ToDateTimeOffset(counts[i]);
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
    /// Reads a <c>DateTime64</c> column: bulk-reads the raw count bytes into a pooled buffer that becomes the
    /// column's storage. The buffer is returned to the pool if the read throws, so no rent leaks on failure.
    /// </summary>
    /// <param name="reader">The reader positioned at the column body.</param>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="scale">The fractional-second scale (0–9).</param>
    /// <param name="timeZone">The timezone values are presented in.</param>
    /// <param name="rowCount">The number of values to read.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded, pooled column.</returns>
    public static async ValueTask<IColumn> ReadAsync(
        ClickHouseBinaryReader reader,
        string name,
        string typeName,
        int scale,
        TimeZoneInfo timeZone,
        int rowCount,
        CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            return new DateTime64Column(name, typeName, scale, timeZone, Array.Empty<byte>(), length: 0, pooled: false);
        }

        int byteCount = checked(rowCount * sizeof(long));
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

        return new DateTime64Column(name, typeName, scale, timeZone, rented, byteCount, pooled: true);
    }

    // Projects a raw count onto the .NET calendar and presents it in the column's timezone. Sub-100 ns digits at
    // scale 8/9 are truncated toward zero here; the exact value stays in Values. The offset is resolved from the
    // instant so both daylight-saving transitions and historical base-offset changes are honored.
    private DateTimeOffset ToDateTimeOffset(long count) => ColumnValueProjections.DateTime64ToOffset(count, scale, timeZone);
}
