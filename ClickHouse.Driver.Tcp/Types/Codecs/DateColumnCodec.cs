using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Date</c> column: a little-endian <c>UInt16</c> day count since the Unix epoch
/// (1970-01-01), surfaced as a <see cref="DateOnly"/>. The representable range is 1970-01-01 to 2149-06-06.
/// </summary>
internal sealed class DateColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly DateColumnCodec Instance = new();

    private DateColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "Date";

    /// <inheritdoc/>
    public int? FixedRowByteSize => sizeof(ushort);

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        return ArrayColumn<DateOnly>.ReadAsync(reader, columnName, columnType, rowCount, checked(rowCount * sizeof(ushort)), Fill, cancellationToken);

        static void Fill(ReadOnlySpan<byte> source, Span<DateOnly> destination)
        {
            ReadOnlySpan<ushort> days = MemoryMarshal.Cast<byte, ushort>(source);
            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = DateOnly.FromDayNumber(DateColumnCodecShared.UnixEpochDayNumber + days[i]);
            }
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<DateOnly>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        foreach (DateOnly value in ((IColumn<DateOnly>)column).Values.Slice(start, length))
        {
            int days = value.DayNumber - DateColumnCodecShared.UnixEpochDayNumber;
            if (days is < 0 or > ushort.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(column), value, "Date is outside the range ClickHouse Date can hold (1970-01-01 to 2149-06-06).");
            }

            writer.WriteUInt16((ushort)days);
        }
    }
}

/// <summary>
/// A codec for the ClickHouse <c>Date32</c> column: a little-endian <c>Int32</c> day count since the Unix epoch
/// (may be negative), surfaced as a <see cref="DateOnly"/>. The representable range is 1900-01-01 to 2299-12-31.
/// </summary>
internal sealed class Date32ColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly Date32ColumnCodec Instance = new();

    // ClickHouse Date32 supported day range relative to the Unix epoch: 1900-01-01 to 2299-12-31.
    private static readonly int MinDays = new DateOnly(1900, 1, 1).DayNumber - DateColumnCodecShared.UnixEpochDayNumber;
    private static readonly int MaxDays = new DateOnly(2299, 12, 31).DayNumber - DateColumnCodecShared.UnixEpochDayNumber;

    private Date32ColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "Date32";

    /// <inheritdoc/>
    public int? FixedRowByteSize => sizeof(int);

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        return ArrayColumn<DateOnly>.ReadAsync(reader, columnName, columnType, rowCount, checked(rowCount * sizeof(int)), Fill, cancellationToken);

        static void Fill(ReadOnlySpan<byte> source, Span<DateOnly> destination)
        {
            ReadOnlySpan<int> days = MemoryMarshal.Cast<byte, int>(source);
            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = DateOnly.FromDayNumber(DateColumnCodecShared.UnixEpochDayNumber + days[i]);
            }
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<DateOnly>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        foreach (DateOnly value in ((IColumn<DateOnly>)column).Values.Slice(start, length))
        {
            int days = value.DayNumber - DateColumnCodecShared.UnixEpochDayNumber;
            if (days < MinDays || days > MaxDays)
            {
                throw new ArgumentOutOfRangeException(nameof(column), value, "Date32 is outside the range ClickHouse Date32 can hold (1900-01-01 to 2299-12-31).");
            }

            writer.WriteInt32(days);
        }
    }
}

/// <summary>Shared helpers for the day-count date codecs.</summary>
internal static class DateColumnCodecShared
{
    /// <summary>The <see cref="DateOnly.DayNumber"/> of the Unix epoch (1970-01-01), the wire's day-zero.</summary>
    public static readonly int UnixEpochDayNumber = new DateOnly(1970, 1, 1).DayNumber;
}
