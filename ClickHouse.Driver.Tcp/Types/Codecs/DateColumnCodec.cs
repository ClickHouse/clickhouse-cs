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
    public Type ElementType => typeof(DateOnly);

    /// <inheritdoc/>
    public object NullPlaceholder => DateColumnCodecShared.Epoch;

    /// <inheritdoc/>
    // A DateOnly compares by its day number, which is what is encoded.
    public object LowCardinalityKeyWriter(Type writeType)
        => writeType == typeof(DateOnly) ? LowCardinalityKeys.Identity<DateOnly>() : null;

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
    // The column that a query of the type reads.
    public bool WritesFromStorage(IColumn column) => column is ArrayColumn<DateOnly>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // The decoded column holds the dates: they are converted to their day counts a chunk at a time.
        if (column is ArrayColumn<DateOnly> stored)
        {
            Span<ushort> chunk = stackalloc ushort[DateColumnCodecShared.ChunkValues];
            ReadOnlySpan<DateOnly> values = stored.Values.Slice(start, length);
            while (!values.IsEmpty)
            {
                int count = Math.Min(chunk.Length, values.Length);
                for (int i = 0; i < count; i++)
                {
                    chunk[i] = ToDays(values[i]);
                }

                writer.WriteBytes(MemoryMarshal.AsBytes(chunk.Slice(0, count)));
                values = values.Slice(count);
            }

            return;
        }

        var typed = (IColumn<DateOnly>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteUInt16(ToDays(typed[start + i]));
        }
    }

    // The parameter name is the one of WriteColumn.
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
    private static ushort ToDays(DateOnly value)
    {
        int days = value.DayNumber - DateColumnCodecShared.UnixEpochDayNumber;
        if (days is < 0 or > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException("column", value, "Date is outside the range ClickHouse Date can hold (1970-01-01 to 2149-06-06).");
        }

        return (ushort)days;
    }
#pragma warning restore CA2208
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
    public Type ElementType => typeof(DateOnly);

    /// <inheritdoc/>
    public object NullPlaceholder => DateColumnCodecShared.Epoch;

    /// <inheritdoc/>
    // A DateOnly compares by its day number, which is what is encoded.
    public object LowCardinalityKeyWriter(Type writeType)
        => writeType == typeof(DateOnly) ? LowCardinalityKeys.Identity<DateOnly>() : null;

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
    // The column that a query of the type reads.
    public bool WritesFromStorage(IColumn column) => column is ArrayColumn<DateOnly>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var typed = (IColumn<DateOnly>)column;
        for (int i = 0; i < length; i++)
        {
            DateOnly value = typed[start + i];
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
    /// <summary>The number of values that a write converts on the stack before it gives them to the writer in one copy.</summary>
    public const int ChunkValues = 2048;

    /// <summary>The <see cref="DateOnly.DayNumber"/> of the Unix epoch (1970-01-01), the wire's day-zero.</summary>
    public static readonly int UnixEpochDayNumber = new DateOnly(1970, 1, 1).DayNumber;

    /// <summary>The Unix epoch (1970-01-01) — day-zero, in range for both <c>Date</c> and <c>Date32</c>, used as the null placeholder.</summary>
    public static readonly DateOnly Epoch = new(1970, 1, 1);
}
