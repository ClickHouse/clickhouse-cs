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
/// representable range is [-999:59:59, 999:59:59]. A <see cref="TimeSpan"/> can also be written for convenience.
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
    public IReadOnlyList<Type> WritableElementTypes { get; } = new[] { typeof(int), typeof(TimeSpan) };

    /// <inheritdoc/>
    public IReadOnlyList<Type> ReadableElementTypes { get; } = new[] { typeof(int), typeof(TimeSpan) };

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

        projected = null;
        return false;
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<int> or IColumn<TimeSpan>;

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

        throw new ArgumentException($"A Time column must hold int or TimeSpan values, not {column.GetType()}.", nameof(column));
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
            default:
                throw new ArgumentException($"A Time column must hold int or TimeSpan values, not {column.GetType()}.", nameof(column));
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
