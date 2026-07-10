using System;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Time</c> column: a little-endian <c>Int32</c> second count (a signed
/// time-of-day/duration, not tied to a date), surfaced as a <see cref="TimeSpan"/>. The representable range is
/// [-999:59:59, 999:59:59].
/// </summary>
internal sealed class TimeColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly TimeColumnCodec Instance = new();

    // ClickHouse Time range: ±999 hours 59 minutes 59 seconds.
    private const int MaxSeconds = (999 * 3600) + (59 * 60) + 59;
    private const int MinSeconds = -MaxSeconds;

    private TimeColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "Time";

    /// <inheritdoc/>
    public int? FixedRowByteSize => sizeof(int);

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        return ArrayColumn<TimeSpan>.ReadAsync(reader, columnName, columnType, rowCount, checked(rowCount * sizeof(int)), Fill, cancellationToken);

        static void Fill(ReadOnlySpan<byte> source, Span<TimeSpan> destination)
        {
            ReadOnlySpan<int> seconds = MemoryMarshal.Cast<byte, int>(source);
            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = TimeSpan.FromSeconds(seconds[i]);
            }
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<TimeSpan>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        foreach (TimeSpan value in ((IColumn<TimeSpan>)column).Values.Slice(start, length))
        {
            // Time stores whole seconds; any sub-second component is truncated toward zero (the caller owns
            // the precision trade-off, and Time64 is available when sub-second precision matters).
            long seconds = value.Ticks / TimeSpan.TicksPerSecond;
            if (seconds is < MinSeconds or > MaxSeconds)
            {
                throw new ArgumentOutOfRangeException(nameof(column), value, "Time is outside the range ClickHouse Time can hold ([-999:59:59, 999:59:59]).");
            }

            writer.WriteInt32((int)seconds);
        }
    }
}

/// <summary>
/// A codec for the ClickHouse <c>Time64(scale)</c> column: a little-endian <c>Int64</c> tick count at
/// 10^-<c>scale</c> seconds (a signed time-of-day/duration), surfaced as a <see cref="TimeSpan"/>. .NET's tick
/// is 100 ns (scale 7), so precision finer than the .NET tick or the column scale is truncated toward zero on
/// read and write respectively.
/// </summary>
internal sealed class Time64ColumnCodec : IColumnCodec
{
    private const int DotNetTickScale = 7; // .NET tick = 100 ns = 10^-7 s.

    // ClickHouse Time64 range: ±999 hours 59 minutes 59 seconds (plus sub-second digits within that bound).
    private const long MaxSeconds = (999 * 3600) + (59 * 60) + 59;
    private const long MinSeconds = -MaxSeconds;

    private readonly int scale;

    private Time64ColumnCodec(string typeName, int scale)
    {
        TypeName = typeName;
        this.scale = scale;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int? FixedRowByteSize => sizeof(long);

    /// <summary>Builds a <c>Time64</c> codec from its scale argument.</summary>
    /// <param name="node">The parsed <c>Time64</c> type node.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The scale argument is missing, malformed, or out of the range 0..9.</exception>
    public static Time64ColumnCodec Create(TypeNode node)
    {
        if (node.Arguments.Count == 0 || !int.TryParse(node.Arguments[0].Name.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out int scale))
        {
            throw new FormatException($"Time64 type '{node}' must specify a numeric scale, e.g. Time64(3).");
        }

        if (scale is < 0 or > 9)
        {
            throw new FormatException($"Time64 scale {scale} is out of the supported range 0..9.");
        }

        return new Time64ColumnCodec(node.ToString(), scale);
    }

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        int shift = DotNetTickScale - scale;
        return ArrayColumn<TimeSpan>.ReadAsync(reader, columnName, columnType, rowCount, checked(rowCount * sizeof(long)), Fill, cancellationToken);

        void Fill(ReadOnlySpan<byte> source, Span<TimeSpan> destination)
        {
            ReadOnlySpan<long> counts = MemoryMarshal.Cast<byte, long>(source);
            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = TimeSpan.FromTicks(FixedPointScaling.ShiftDecimalPlaces(counts[i], shift));
            }
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<TimeSpan>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        int shift = scale - DotNetTickScale;
        foreach (TimeSpan value in ((IColumn<TimeSpan>)column).Values.Slice(start, length))
        {
            // Reject durations outside ClickHouse's range up front (mirrors Time), rather than emitting a count
            // the server rejects or wraps. Precision finer than the column scale is truncated toward zero.
            long seconds = value.Ticks / TimeSpan.TicksPerSecond;
            if (seconds is < MinSeconds or > MaxSeconds)
            {
                throw new ArgumentOutOfRangeException(nameof(column), value, "Time64 is outside the range ClickHouse Time64 can hold ([-999:59:59, 999:59:59]).");
            }

            long count = FixedPointScaling.ShiftDecimalPlaces(value.Ticks, shift);
            writer.WriteInt64(count);
        }
    }
}
