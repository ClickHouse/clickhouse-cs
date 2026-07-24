using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A <c>DateTime</c> / <c>DateTime('tz')</c> column that keeps the raw wire seconds and offers them under two
/// types. ClickHouse writes a little-endian <c>UInt32</c> of seconds since the Unix epoch per row; those bytes
/// are kept verbatim as the column's storage (reinterpreted as <see cref="uint"/> with no copy), and the
/// timezone — the same for every row — lives once on the column. A caller reads each row either as the raw
/// epoch seconds (<see cref="Seconds"/> / <see cref="GetUnixTimeSeconds"/>, zero-copy) or — the default
/// <see cref="IColumn{T}"/> view — as a <see cref="DateTimeOffset"/> presented in the column's timezone
/// (<see cref="DateTimeOffset"/> represents second-resolution instants exactly, so no precision is lost).
///
/// <para>
/// The backing buffer is rented from <see cref="ArrayPool{T}"/> and returned on <see cref="Dispose"/>; like every
/// column, the bytes and the span returned by <see cref="Seconds"/> are borrowed for the block's lifetime. Copy
/// out to retain.
/// </para>
/// </summary>
internal sealed class DateTimeColumn : IColumn<DateTimeOffset>
{
    private readonly TimeZoneInfo timeZone;
    private readonly bool fixedOffset;
    private readonly TimeSpan constantOffset;
    private readonly int length;
    private readonly bool pooled;
    private byte[] buffer;
    private DateTimeOffset[] cache;

    /// <summary>Initializes a column over the raw little-endian second bytes.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="timeZone">The timezone values are presented in.</param>
    /// <param name="buffer">The little-endian column bytes (may be longer than <paramref name="length"/>).</param>
    /// <param name="length">The logical byte length; must be a whole multiple of <c>sizeof(uint)</c>.</param>
    /// <param name="pooled">Whether <paramref name="buffer"/> was rented and should be returned on dispose.</param>
    public DateTimeColumn(string name, string typeName, TimeZoneInfo timeZone, byte[] buffer, int length, bool pooled)
    {
        Name = name;
        TypeName = typeName;
        this.timeZone = timeZone;

        // Zones without daylight saving present a single offset for every instant, so resolve it once here
        // rather than walking the zone's rules per row; only a DST zone needs the per-instant lookup in At.
        fixedOffset = !timeZone.SupportsDaylightSavingTime;
        constantOffset = timeZone.BaseUtcOffset;

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

    /// <summary>The raw epoch-second counts, as a zero-copy view.</summary>
    public ReadOnlySpan<uint> Seconds => MemoryMarshal.Cast<byte, uint>(buffer.AsSpan(0, length));

    /// <summary>
    /// The rows as <see cref="DateTimeOffset"/>, materialized once and cached. Prefer <see cref="Seconds"/> or
    /// <see cref="GetUnixTimeSeconds"/> to avoid building this array when only the raw seconds are needed.
    /// </summary>
    public ReadOnlySpan<DateTimeOffset> Values
    {
        get
        {
            if (cache is null)
            {
                // Rent rather than allocate: this is a convenience view consumers copy out of, so it only needs
                // to live until Dispose returns it to the pool. Single-consumer per connection, so the lazy fill
                // needs no synchronization. The rented buffer may be longer than RowCount; Values slices to it.
                ReadOnlySpan<uint> seconds = Seconds;
                DateTimeOffset[] decoded = ArrayPool<DateTimeOffset>.Shared.Rent(seconds.Length);
                for (int i = 0; i < seconds.Length; i++)
                {
                    decoded[i] = At(seconds[i]);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, RowCount);
        }
    }

    /// <inheritdoc/>
    // The cache is rented and may be longer than RowCount, so slice before indexing to keep an out-of-range row
    // failing fast rather than returning a stale slot; the uncached path indexes the length-bounded Seconds span.
    public DateTimeOffset this[int row] => cache is not null ? cache.AsSpan(0, RowCount)[row] : At(Seconds[row]);

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>Returns the raw epoch seconds for a row.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The seconds since the Unix epoch.</returns>
    public long GetUnixTimeSeconds(int row) => Seconds[row];

    /// <inheritdoc/>
    public void Dispose()
    {
        if (pooled && buffer.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        buffer = Array.Empty<byte>();

        if (cache is not null)
        {
            // DateTimeOffset holds no references, so no clear is needed.
            ArrayPool<DateTimeOffset>.Shared.Return(cache);
            cache = null;
        }
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
        TimeZoneInfo timeZone,
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

    // The wire value is a UTC instant; the timezone only decides the offset it is presented with. A zone with no
    // daylight saving presents one offset for every instant (resolved once in the constructor); a DST zone is
    // resolved from the instant so its transitions are honored.
    private DateTimeOffset At(uint seconds)
    {
        DateTimeOffset utc = DateTimeOffset.FromUnixTimeSeconds(seconds);
        return fixedOffset ? utc.ToOffset(constantOffset) : TimeZoneInfo.ConvertTime(utc, timeZone);
    }
}
