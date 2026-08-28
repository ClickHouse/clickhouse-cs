using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Time64(scale)</c> column: a little-endian <c>Int64</c> tick count at
/// 10^-<c>scale</c> seconds (a signed time-of-day/duration), surfaced as the raw <see cref="long"/> count that
/// retains the exact wire value at any scale (including scales 8 and 9, which are finer than a .NET tick). A
/// <see cref="TimeSpan"/> can also be written for convenience, truncated toward zero to the column scale.
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
    public Type ElementType => typeof(long);

    /// <inheritdoc/>
    public IReadOnlyList<Type> WritableElementTypes { get; } = new[] { typeof(long), typeof(TimeSpan) };

    /// <inheritdoc/>
    public IReadOnlyList<Type> ReadableElementTypes { get; } = new[] { typeof(long), typeof(TimeSpan) };

    /// <inheritdoc/>
    public object NullPlaceholder => 0L;

    /// <inheritdoc/>
    public Type CanonicalWriteElementType => typeof(long);

    /// <inheritdoc/>
    public object CanonicalWritePlaceholder => 0L;

    /// <inheritdoc/>
    public object NullPlaceholderAs(Type writeType)
    {
        if (writeType == typeof(long))
        {
            return NullPlaceholder;
        }

        if (writeType == typeof(TimeSpan))
        {
            return TimeSpan.Zero;
        }

        throw new NotSupportedException($"The '{TypeName}' codec has no null placeholder for {writeType}.");
    }

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
        => Time64Column.ReadAsync(reader, columnName, columnType, scale, rowCount, cancellationToken);

    /// <inheritdoc/>
    public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
    {
        ColumnValueProjections.RequireSourceType(value, typeof(long), TypeName);

        if (targetType == typeof(long))
        {
            projected = value;
            return true;
        }

        if (targetType == typeof(TimeSpan))
        {
            projected = ColumnValueProjections.Call(nameof(ColumnValueProjections.Time64ToTimeSpan), value, scale);
            return true;
        }

        projected = null;
        return false;
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<long> or IColumn<TimeSpan>;

    /// <inheritdoc/>
    public IColumn ToCanonicalWriteColumn(IColumn column)
    {
        if (column is IColumn<long>)
        {
            return column;
        }

        if (column is IColumn<TimeSpan> spans)
        {
            return new ProjectedColumn<TimeSpan, long>(TypeName, spans, ToCount);
        }

        throw new ArgumentException($"A Time64 column must hold long or TimeSpan values, not {column.GetType()}.", nameof(column));
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        switch (column)
        {
            case IColumn<long> counts:
                WriteCanonicalColumn(writer, counts, start, length);
                break;
            case IColumn<TimeSpan> spans:
                for (int i = 0; i < length; i++)
                {
                    writer.WriteInt64(ToCount(spans[start + i]));
                }

                break;
            default:
                throw new ArgumentException($"A Time64 column must hold long or TimeSpan values, not {column.GetType()}.", nameof(column));
        }
    }

    /// <inheritdoc/>
    public void WriteCanonicalColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var counts = (IColumn<long>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteInt64(counts[start + i]);
        }
    }

    private long ToCount(TimeSpan value)
    {
        long seconds = value.Ticks / TimeSpan.TicksPerSecond;
        if (seconds is < MinSeconds or > MaxSeconds)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "Time64 is outside the range ClickHouse Time64 can hold ([-999:59:59, 999:59:59]).");
        }

        return FixedPointScaling.ShiftDecimalPlaces(value.Ticks, scale - DotNetTickScale);
    }
}
