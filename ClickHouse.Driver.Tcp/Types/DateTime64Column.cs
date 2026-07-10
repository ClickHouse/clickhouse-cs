using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A <c>DateTime64(scale[, tz])</c> column that keeps the raw wire counts and offers them under several types.
/// ClickHouse writes a little-endian <c>Int64</c> tick count at <c>10^-scale</c> seconds since the Unix epoch
/// per row; those bytes are kept verbatim as the column's storage (reinterpreted as <see cref="long"/> with no
/// copy), and the scale and timezone — which are the same for every row — live once on the column rather than
/// on each value. A caller chooses how to read each row: the raw count (<see cref="GetCount"/> /
/// <see cref="Counts"/>, zero-copy), the presented instant (<see cref="GetDateTimeOffset"/>), or — the default
/// <see cref="IColumn{T}"/> view — a full-precision <see cref="ClickHouseDateTime64"/> that also carries the
/// scale and resolved offset.
///
/// <para>
/// The backing buffer is rented from <see cref="ArrayPool{T}"/> and returned on <see cref="Dispose"/>; like every
/// column, the bytes and the span returned by <see cref="Counts"/> are borrowed for the block's lifetime. Copy
/// out to retain.
/// </para>
/// </summary>
internal sealed class DateTime64Column : IColumn<ClickHouseDateTime64>
{
    private const int DotNetTickScale = 7; // .NET tick = 100 ns = 10^-7 s.
    private static readonly long UnixEpochTicks = DateTime.UnixEpoch.Ticks;

    private readonly int scale;
    private readonly TimeZoneInfo timeZone;
    private readonly bool fixedOffset;
    private readonly TimeSpan constantOffset;
    private readonly int length;
    private readonly bool pooled;
    private byte[] buffer;
    private ClickHouseDateTime64[] cache;

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
    public int RowCount => length / sizeof(long);

    /// <summary>The fractional-second scale (0–9) shared by every value in the column.</summary>
    public int Scale => scale;

    /// <summary>The raw signed tick counts (at <c>10^-Scale</c> seconds since the epoch), as a zero-copy view.</summary>
    public ReadOnlySpan<long> Counts => MemoryMarshal.Cast<byte, long>(buffer.AsSpan(0, length));

    /// <summary>
    /// The rows as <see cref="ClickHouseDateTime64"/>, materialized once and cached. Prefer <see cref="Counts"/>,
    /// <see cref="GetCount"/>, or <see cref="GetDateTimeOffset"/> to avoid building this array when only the raw
    /// count or the offset view is needed.
    /// </summary>
    public ReadOnlySpan<ClickHouseDateTime64> Values
    {
        get
        {
            if (cache is null)
            {
                // Rent rather than allocate: this is a convenience view consumers copy out of, so it only needs
                // to live until Dispose returns it to the pool. Single-consumer per connection, so the lazy fill
                // needs no synchronization. The rented buffer may be longer than RowCount; Values slices to it.
                ReadOnlySpan<long> counts = Counts;
                ClickHouseDateTime64[] decoded = ArrayPool<ClickHouseDateTime64>.Shared.Rent(counts.Length);
                for (int i = 0; i < counts.Length; i++)
                {
                    decoded[i] = At(counts[i]);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, RowCount);
        }
    }

    /// <inheritdoc/>
    // The cache is rented and may be longer than RowCount, so slice before indexing to keep an out-of-range row
    // failing fast rather than returning a stale slot; the uncached path indexes the length-bounded Counts span.
    public ClickHouseDateTime64 this[int row] => cache is not null ? cache.AsSpan(0, RowCount)[row] : At(Counts[row]);

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>Returns the raw signed tick count for a row (at <c>10^-Scale</c> seconds since the epoch).</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The raw count.</returns>
    public long GetCount(int row) => Counts[row];

    /// <summary>Returns a row as a <see cref="DateTimeOffset"/> at 100 ns resolution, presented in the column's timezone.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The instant. Lossy for scales finer than 7 (sub-100 ns digits are truncated); use
    /// <see cref="GetCount"/> or the <see cref="ClickHouseDateTime64"/> view for the exact value.</returns>
    public DateTimeOffset GetDateTimeOffset(int row) => this[row].ToDateTimeOffset();

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
            // ClickHouseDateTime64 holds no references, so no clear is needed.
            ArrayPool<ClickHouseDateTime64>.Shared.Return(cache);
            cache = null;
        }
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

    // Builds the self-contained value for a raw count: the timezone only decides the presented offset. A zone
    // with no daylight saving presents one offset for every instant (resolved once in the constructor); a DST
    // zone is resolved from the instant (a 100 ns-truncated instant is precise enough to pick the offset for any
    // transition, which only ever land on a whole second).
    private ClickHouseDateTime64 At(long count)
    {
        if (fixedOffset)
        {
            return new ClickHouseDateTime64(count, scale, constantOffset);
        }

        long dotNetTicks = FixedPointScaling.ShiftDecimalPlaces(count, DotNetTickScale - scale);
        var utc = new DateTimeOffset(UnixEpochTicks + dotNetTicks, TimeSpan.Zero);
        return new ClickHouseDateTime64(count, scale, timeZone.GetUtcOffset(utc));
    }
}
