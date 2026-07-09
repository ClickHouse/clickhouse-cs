using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A minimal codec for the ClickHouse <c>DateTime</c> column: a little-endian <c>UInt32</c> of seconds since
/// the Unix epoch per row, surfaced as a UTC <see cref="DateTime"/>. The type's timezone argument (part of the
/// type string, not the body) affects only display, not the instant, so it is ignored here. This exists so the
/// server's always-present ProfileEvents block, which carries a <c>DateTime</c> column, can be consumed; full
/// timezone-aware DateTime handling is a later TODO.
/// </summary>
internal sealed class DateTimeColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly DateTimeColumnCodec Instance = new();

    private DateTimeColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "DateTime";

    /// <inheritdoc/>
    public int? FixedRowByteSize => sizeof(uint);

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        var values = new DateTime[rowCount];
        if (rowCount == 0)
        {
            return new ArrayColumn<DateTime>(columnName, columnType, values);
        }

        // Read the whole fixed-size column body in one transfer, reinterpret it as the epoch-second UInt32s,
        // then convert. The scratch buffer is pooled since it's discarded once the DateTimes are built.
        int byteCount = checked(rowCount * sizeof(uint));
        byte[] rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            await reader.ReadBytesAsync(rented.AsMemory(0, byteCount), cancellationToken).ConfigureAwait(false);
            ReadOnlySpan<uint> seconds = MemoryMarshal.Cast<byte, uint>(rented.AsSpan(0, byteCount));
            for (int i = 0; i < rowCount; i++)
            {
                values[i] = DateTime.UnixEpoch.AddSeconds(seconds[i]);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }

        return new ArrayColumn<DateTime>(columnName, columnType, values);
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<DateTime> or IColumn<DateTimeOffset>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // Both a UTC instant (DateTimeOffset) and a DateTime map to the same epoch-second column body; a
        // DateTime is normalized to UTC first (its offset, if any, is resolved by ToUniversalTime).
        switch (column)
        {
            case IColumn<DateTime> dateTimes:
                foreach (DateTime value in dateTimes.Values.Slice(start, length))
                {
                    writer.WriteUInt32(ToUnixSeconds(value.ToUniversalTime()));
                }

                break;

            case IColumn<DateTimeOffset> offsets:
                foreach (DateTimeOffset value in offsets.Values.Slice(start, length))
                {
                    writer.WriteUInt32(ToUnixSeconds(value.UtcDateTime));
                }

                break;

            default:
                throw new ArgumentException($"A DateTime column must hold DateTime or DateTimeOffset values, not {column.GetType()}.", nameof(column));
        }
    }

    private static uint ToUnixSeconds(DateTime utc)
    {
        // Seconds from ticks (not TotalSeconds, which is a double), then range-check: ClickHouse DateTime is a
        // UInt32 second count, so anything before the epoch or past 2106-02-07 06:28:15 UTC cannot be
        // represented and must fail loudly rather than silently wrap.
        long seconds = (utc - DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerSecond;
        if (seconds < 0 || seconds > uint.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(utc),
                utc,
                "DateTime is outside the range ClickHouse DateTime can hold (1970-01-01 to 2106-02-07 06:28:15 UTC).");
        }

        return (uint)seconds;
    }
}
