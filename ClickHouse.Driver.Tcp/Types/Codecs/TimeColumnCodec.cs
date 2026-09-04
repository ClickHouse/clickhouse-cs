using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Time</c> column: a little-endian <c>Int32</c> second count (a signed
/// time-of-day/duration, not tied to a date), surfaced as the raw <see cref="int"/> second count. The
/// representable range is [-999:59:59, 999:59:59]. A <see cref="TimeSpan"/> or a <see cref="TimeOnly"/> can also
/// be written for convenience.
/// <para>
/// Reading as a <see cref="TimeOnly"/> is a narrowing: a column value may be negative or past 24 hours, which no
/// time of day is, and such a row is refused rather than reduced modulo a day.
/// </para>
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
    public Type ElementType => typeof(int);

    /// <inheritdoc/>
    public IReadOnlyList<Type> WritableElementTypes { get; } = new[] { typeof(int), typeof(TimeSpan), typeof(TimeOnly) };

    /// <inheritdoc/>
    public IReadOnlyList<Type> ReadableElementTypes { get; } = new[] { typeof(int), typeof(TimeSpan), typeof(TimeOnly) };

    /// <inheritdoc/>
    public object NullPlaceholder => 0;

    /// <inheritdoc/>
    public Type CanonicalWriteElementType => typeof(int);

    /// <inheritdoc/>
    public object CanonicalWritePlaceholder => 0;

    /// <inheritdoc/>
    public object NullPlaceholderAs(Type writeType)
    {
        if (writeType == typeof(int))
        {
            return NullPlaceholder;
        }

        if (writeType == typeof(TimeSpan))
        {
            return TimeSpan.Zero;
        }

        if (writeType == typeof(TimeOnly))
        {
            return TimeOnly.MinValue;
        }

        throw new NotSupportedException($"The '{TypeName}' codec has no null placeholder for {writeType}.");
    }

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => TimeColumn.ReadAsync(reader, columnName, columnType, rowCount, cancellationToken);

    /// <inheritdoc/>
    public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
    {
        ColumnValueProjections.RequireSourceType(value, typeof(int), TypeName);

        if (targetType == typeof(int))
        {
            projected = value;
            return true;
        }

        if (targetType == typeof(TimeSpan))
        {
            projected = ColumnValueProjections.Call(nameof(ColumnValueProjections.TimeToTimeSpan), value);
            return true;
        }

        if (targetType == typeof(TimeOnly))
        {
            projected = ColumnValueProjections.Call(nameof(ColumnValueProjections.TimeToTimeOnly), value);
            return true;
        }

        projected = null;
        return false;
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<int> or IColumn<TimeSpan> or IColumn<TimeOnly>;

    /// <inheritdoc/>
    public IColumn ToCanonicalWriteColumn(IColumn column)
    {
        if (column is IColumn<int>)
        {
            return column;
        }

        if (column is IColumn<TimeSpan> spans)
        {
            return new ProjectedColumn<TimeSpan, int>(TypeName, spans, ToSeconds);
        }

        if (column is IColumn<TimeOnly> times)
        {
            return new ProjectedColumn<TimeOnly, int>(TypeName, times, ToSeconds);
        }

        throw new ArgumentException($"A Time column must hold int, TimeSpan or TimeOnly values, not {column.GetType()}.", nameof(column));
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        switch (column)
        {
            case IColumn<int> seconds:
                WriteCanonicalColumn(writer, seconds, start, length);
                break;
            case IColumn<TimeSpan> spans:
                for (int i = 0; i < length; i++)
                {
                    writer.WriteInt32(ToSeconds(spans[start + i]));
                }

                break;
            case IColumn<TimeOnly> times:
                for (int i = 0; i < length; i++)
                {
                    writer.WriteInt32(ToSeconds(times[start + i]));
                }

                break;
            default:
                throw new ArgumentException($"A Time column must hold int, TimeSpan or TimeOnly values, not {column.GetType()}.", nameof(column));
        }
    }

    /// <inheritdoc/>
    public void WriteCanonicalColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var seconds = (IColumn<int>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteInt32(seconds[start + i]);
        }
    }

    // A time of day is always inside the column's range, so this needs no bound of its own.
    private static int ToSeconds(TimeOnly value) => (int)(value.Ticks / TimeSpan.TicksPerSecond);

    private static int ToSeconds(TimeSpan value)
    {
        long seconds = value.Ticks / TimeSpan.TicksPerSecond;
        if (seconds is < MinSeconds or > MaxSeconds)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "Time is outside the range ClickHouse Time can hold ([-999:59:59, 999:59:59]).");
        }

        return (int)seconds;
    }
}
