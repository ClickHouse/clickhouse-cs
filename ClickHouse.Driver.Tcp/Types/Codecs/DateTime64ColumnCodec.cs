using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Encodes ClickHouse <c>DateTime64</c> as signed counts at its declared scale. Reads preserve the raw
/// <see cref="long"/> value; the explicit or session timezone controls <see cref="DateTimeOffset"/> projections and
/// how unspecified <see cref="DateTime"/> values are interpreted.
/// </summary>
internal sealed class DateTime64ColumnCodec : IColumnCodec
{
    private const int MaxScale = 9; // Nanoseconds — the finest scale ClickHouse DateTime64 supports.
    private const int DotNetTickScale = 7; // .NET tick = 100 ns = 10^-7 s.
    private static readonly long UnixEpochTicks = DateTime.UnixEpoch.Ticks;

    private readonly int scale;
    private readonly TimeZoneInfo timeZone;

    private DateTime64ColumnCodec(string typeName, int scale, TimeZoneInfo timeZone)
    {
        TypeName = typeName;
        this.scale = scale;
        this.timeZone = timeZone;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(long);

    /// <inheritdoc/>
    public IReadOnlyList<Type> WritableElementTypes { get; } = new[] { typeof(long), typeof(DateTimeOffset), typeof(DateTime) };

    /// <inheritdoc/>
    public IReadOnlyList<Type> ReadableElementTypes { get; } = new[] { typeof(long), typeof(DateTimeOffset), typeof(DateTime) };

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

        if (writeType == typeof(DateTimeOffset))
        {
            return DateTimeOffset.UnixEpoch;
        }

        if (writeType == typeof(DateTime))
        {
            return DateTime.UnixEpoch;
        }

        throw new NotSupportedException($"The '{TypeName}' codec has no null placeholder for {writeType}.");
    }

    /// <summary>Builds a <c>DateTime64</c> codec from its scale and optional timezone arguments.</summary>
    /// <param name="node">The parsed <c>DateTime64</c> type node.</param>
    /// <param name="serverTimezone">The session timezone, used when the type string carries none.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The scale argument is missing, malformed, or out of the range 0..9.</exception>
    public static DateTime64ColumnCodec Create(TypeNode node, string serverTimezone)
    {
        if (node.Arguments.Count == 0 || !int.TryParse(node.Arguments[0].Name.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out int scale))
        {
            throw new FormatException($"DateTime64 type '{node}' must specify a numeric scale, e.g. DateTime64(3).");
        }

        if (scale is < 0 or > MaxScale)
        {
            throw new FormatException($"DateTime64 scale {scale} is out of the supported range 0..{MaxScale}.");
        }

        string explicitTz = node.Arguments.Count > 1 ? DateTimeZones.UnquoteTimezone(node.Arguments[1]) : null;
        TimeZoneInfo tz = DateTimeZones.Resolve(explicitTz, serverTimezone);
        return new DateTime64ColumnCodec(node.ToString(), scale, tz);
    }

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => DateTime64Column.ReadAsync(reader, columnName, columnType, scale, timeZone, rowCount, cancellationToken);

    /// <inheritdoc/>
    public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
    {
        ColumnValueProjections.RequireSourceType(value, typeof(long), TypeName);

        if (targetType == typeof(long))
        {
            projected = value;
            return true;
        }

        if (targetType == typeof(DateTimeOffset))
        {
            projected = ColumnValueProjections.Call(nameof(ColumnValueProjections.DateTime64ToOffset), value, scale, timeZone);
            return true;
        }

        if (targetType == typeof(DateTime))
        {
            projected = ColumnValueProjections.Call(nameof(ColumnValueProjections.DateTime64ToDateTime), value, scale, timeZone);
            return true;
        }

        projected = null;
        return false;
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<long> or IColumn<DateTimeOffset> or IColumn<DateTime>;

    /// <inheritdoc/>
    public IColumn ToCanonicalWriteColumn(IColumn column)
    {
        if (column is IColumn<long>)
        {
            return column;
        }

        if (column is IColumn<DateTimeOffset> offsets)
        {
            return new ProjectedColumn<DateTimeOffset, long>(TypeName, offsets, CountFromDateTimeOffset);
        }

        if (column is IColumn<DateTime> dateTimes)
        {
            return new ProjectedColumn<DateTime, long>(TypeName, dateTimes, CountFromDateTime);
        }

        throw new ArgumentException(
            $"A DateTime64 column must hold long, DateTimeOffset, or DateTime values, not {column.GetType()}.",
            nameof(column));
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        switch (column)
        {
            case IColumn<long> counts:
                WriteCanonicalColumn(writer, counts, start, length);
                break;
            case IColumn<DateTimeOffset> offsets:
                for (int i = 0; i < length; i++)
                {
                    writer.WriteInt64(CountFromDateTimeOffset(offsets[start + i]));
                }

                break;
            case IColumn<DateTime> dateTimes:
                for (int i = 0; i < length; i++)
                {
                    writer.WriteInt64(CountFromDateTime(dateTimes[start + i]));
                }

                break;
            default:
                throw new ArgumentException(
                    $"A DateTime64 column must hold long, DateTimeOffset, or DateTime values, not {column.GetType()}.",
                    nameof(column));
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

    // Converts an instant to the wire count at this column's scale. A DateTimeOffset holds only 100 ns ticks, so
    // scales 7 and finer are always exact; a coarser scale must divide the .NET tick count evenly, since dropping
    // non-zero sub-scale digits would silently lose precision.
    private long CountFromDateTimeOffset(DateTimeOffset value)
    {
        long dotNetTicksSinceEpoch = value.UtcDateTime.Ticks - UnixEpochTicks;
        int places = scale - DotNetTickScale;
        if (places >= 0)
        {
            return FixedPointScaling.ShiftDecimalPlaces(dotNetTicksSinceEpoch, places);
        }

        long factor = FixedPointScaling.Pow10(-places);
        if (dotNetTicksSinceEpoch % factor != 0)
        {
            throw new ArgumentException($"{value:o} cannot be written to {TypeName} (scale {scale}) without losing precision.", nameof(value));
        }

        return dotNetTicksSinceEpoch / factor;
    }

    private long CountFromDateTime(DateTime value)
        => CountFromDateTimeOffset(new DateTimeOffset(DateTimeColumnCodec.ToUtc(value, timeZone)));
}
