using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A <c>DateTime</c> / <c>DateTime('tz')</c> column that surfaces the raw wire seconds. ClickHouse writes a
/// little-endian <c>UInt32</c> of seconds since the Unix epoch per row; those bytes are kept verbatim as the
/// column's storage and exposed — with no copy — as the <see cref="uint"/> <see cref="Values"/>. The timezone —
/// the same for every row — lives once on the column rather than on each value.
///
/// <para>
/// A caller that wants presented instants can call <see cref="GetDateTimeOffset"/> or <see cref="ToDateTimeOffsets"/>
/// to project the seconds to <see cref="DateTimeOffset"/> in the column's timezone; that projection is exact
/// (a <see cref="DateTimeOffset"/> represents second-resolution instants without loss).
/// </para>
///
/// <para>
/// The backing buffer is rented from <see cref="ArrayPool{T}"/> and returned on <see cref="Dispose"/>; like every
/// column, the bytes and the span returned by <see cref="Values"/> are borrowed for the block's lifetime. Copy
/// out to retain.
/// </para>
/// </summary>
internal sealed class DateTimeColumn : IColumn<uint>, IDateTimeColumn, IStoredValuesColumn
{
    private readonly ResolvedTimeZone timeZone;
    private readonly int length;
    private readonly bool pooled;
    private byte[] buffer;

    /// <summary>Initializes a column over the raw little-endian second bytes.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="timeZone">The timezone values are presented in.</param>
    /// <param name="buffer">The little-endian column bytes (may be longer than <paramref name="length"/>).</param>
    /// <param name="length">The logical byte length; must be a whole multiple of <c>sizeof(uint)</c>.</param>
    /// <param name="pooled">Whether <paramref name="buffer"/> was rented and should be returned on dispose.</param>
    public DateTimeColumn(string name, string typeName, ResolvedTimeZone timeZone, byte[] buffer, int length, bool pooled)
    {
        Name = name;
        TypeName = typeName;
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
    public int RowCount => length / sizeof(uint);

    /// <summary>The timezone the seconds are presented in, shared by every value in the column. Use it to
    /// interpret the raw <see cref="Values"/> seconds.</summary>
    /// <exception cref="FormatException">The header named a timezone this platform cannot represent. The
    /// <see cref="Values"/> seconds are unaffected.</exception>
    public TimeZoneInfo TimeZone => timeZone.Value;

    /// <summary>Zero: <c>DateTime</c> counts whole seconds. <c>DateTime64(scale)</c> is where this varies.</summary>
    public int Scale => 0;

    /// <summary>The raw epoch-second counts, as a zero-copy view. Use <see cref="ToDateTimeOffsets"/> for a
    /// calendar view presented in the column's timezone.</summary>
    public ReadOnlySpan<uint> Values => MemoryMarshal.Cast<byte, uint>(buffer.AsSpan(0, length));

    /// <inheritdoc/>
    public uint this[int row] => Values[row];

    /// <inheritdoc/>
    public object GetValue(int row) => Values[row];

    /// <summary>Returns a row as a <see cref="DateTimeOffset"/> presented in the column's timezone.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The instant (exact — a <see cref="DateTimeOffset"/> holds second-resolution values without loss).</returns>
    public DateTimeOffset GetDateTimeOffset(int row) => ToDateTimeOffset(Values[row]);

    /// <summary>
    /// Projects every row to a <see cref="DateTimeOffset"/> presented in the column's timezone, as a freshly
    /// allocated array the caller owns (it outlives the block, unlike <see cref="Values"/>).
    /// </summary>
    /// <returns>One <see cref="DateTimeOffset"/> per row, in row order.</returns>
    public DateTimeOffset[] ToDateTimeOffsets()
    {
        ReadOnlySpan<uint> seconds = Values;
        var result = new DateTimeOffset[seconds.Length];
        for (int i = 0; i < seconds.Length; i++)
        {
            result[i] = ToDateTimeOffset(seconds[i]);
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
    /// Reads a <c>DateTime</c> column: bulk-reads the raw second bytes into a pooled buffer that becomes the
    /// column's storage. The buffer is returned to the pool if the read throws, so no rent leaks on failure.
    /// </summary>
    /// <param name="reader">The reader positioned at the column body.</param>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="timeZone">The timezone values are presented in.</param>
    /// <param name="rowCount">The number of values to read.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded, pooled column.</returns>
    public static async ValueTask<IColumn> ReadAsync(
        ClickHouseBinaryReader reader,
        string name,
        string typeName,
        ResolvedTimeZone timeZone,
        int rowCount,
        CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            return new DateTimeColumn(name, typeName, timeZone, Array.Empty<byte>(), length: 0, pooled: false);
        }

        int byteCount = checked(rowCount * sizeof(uint));
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

        return new DateTimeColumn(name, typeName, timeZone, rented, byteCount, pooled: true);
    }

    private DateTimeOffset ToDateTimeOffset(uint seconds) => ColumnValueProjections.DateTimeToOffset(seconds, timeZone.Value);
}
