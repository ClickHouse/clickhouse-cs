using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Encodes ClickHouse <c>DateTime</c> as Unix seconds. The explicit or session timezone controls
/// <see cref="DateTimeOffset"/> projections and how unspecified <see cref="DateTime"/> values are interpreted.
/// </summary>
internal sealed class DateTimeColumnCodec : IColumnCodec
{
    private readonly TimeZoneInfo timeZone;

    private DateTimeColumnCodec(string typeName, TimeZoneInfo timeZone)
    {
        TypeName = typeName;
        this.timeZone = timeZone;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(uint);

    /// <inheritdoc/>
    public IReadOnlyList<Type> WritableElementTypes { get; } = new[] { typeof(uint), typeof(DateTimeOffset), typeof(DateTime) };

    /// <inheritdoc/>
    public IReadOnlyList<Type> ReadableElementTypes { get; } = new[] { typeof(uint), typeof(DateTimeOffset), typeof(DateTime) };

    /// <inheritdoc/>
    public object NullPlaceholder => 0u;

    /// <inheritdoc/>
    public object NullPlaceholderAs(Type writeType)
    {
        if (writeType == typeof(uint))
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

    /// <summary>Builds a <c>DateTime</c> codec, resolving its timezone from the type string or the session.</summary>
    /// <param name="node">The parsed <c>DateTime</c> type node (its optional argument is the timezone).</param>
    /// <param name="serverTimezone">The session timezone, used when the type string carries none.</param>
    /// <returns>The codec.</returns>
    public static DateTimeColumnCodec Create(TypeNode node, string serverTimezone)
    {
        string explicitTz = node.Arguments.Count > 0 ? DateTimeZones.UnquoteTimezone(node.Arguments[0]) : null;
        TimeZoneInfo tz = DateTimeZones.Resolve(explicitTz, serverTimezone);
        return new DateTimeColumnCodec(node.ToString(), tz);
    }

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => DateTimeColumn.ReadAsync(reader, columnName, columnType, timeZone, rowCount, cancellationToken);

    /// <inheritdoc/>
    public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
    {
        ColumnValueProjections.RequireSourceType(value, typeof(uint), TypeName);

        if (targetType == typeof(uint))
        {
            projected = value;
            return true;
        }

        if (targetType == typeof(DateTimeOffset))
        {
            projected = ColumnValueProjections.Call(nameof(ColumnValueProjections.DateTimeToOffset), value, timeZone);
            return true;
        }

        if (targetType == typeof(DateTime))
        {
            projected = ColumnValueProjections.Call(nameof(ColumnValueProjections.DateTimeToDateTime), value, timeZone);
            return true;
        }

        projected = null;
        return false;
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<uint> or IColumn<DateTimeOffset> or IColumn<DateTime>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // Interpret unspecified DateTime values as wall clocks in the resolved timezone.
        switch (column)
        {
            case IColumn<uint> seconds:
                // Raw epoch seconds are the wire representation, so they are written verbatim.
                for (int i = 0; i < length; i++)
                {
                    writer.WriteUInt32(seconds[start + i]);
                }

                break;

            case IColumn<DateTimeOffset> offsets:
                for (int i = 0; i < length; i++)
                {
                    writer.WriteUInt32(ToUnixSeconds(offsets[start + i].UtcDateTime));
                }

                break;

            case IColumn<DateTime> dateTimes:
                for (int i = 0; i < length; i++)
                {
                    writer.WriteUInt32(ToUnixSeconds(ToUtc(dateTimes[start + i], timeZone)));
                }

                break;

            default:
                throw new ArgumentException($"A DateTime column must hold DateTimeOffset or DateTime values, not {column.GetType()}.", nameof(column));
        }
    }

    // Reduces a DateTime to the UTC instant to encode. Utc and Local already denote an instant. An Unspecified
    // value has no offset, so its wall-clock is read in the column's timezone.
    internal static DateTime ToUtc(DateTime value, TimeZoneInfo timeZone)
    {
        if (value.Kind != DateTimeKind.Unspecified)
        {
            return value.ToUniversalTime();
        }

        // A skipped wall-clock names no instant, so it is rejected instead of guessed. Deriving the pre-gap offset
        // from TimeZoneInfo is not reliable: GetUtcOffset answers with the zone's base offset, which differs from
        // the pre-gap one wherever the historical standard offset did (America/Juneau in the 1970s, and 27 other
        // zones as late as 2023).
        if (timeZone.IsInvalidTime(value))
        {
            throw new ArgumentException(
                $"{value:yyyy-MM-dd HH:mm:ss} does not exist in '{timeZone.Id}': a daylight-saving change skips it. " +
                "Pass a DateTimeOffset, or a DateTime with Kind=Utc, to name the instant you mean.",
                nameof(value));
        }

        // An hour that occurs twice takes the earlier occurrence: the larger offset, since it subtracts to an
        // earlier instant. A fall-back always lowers the offset, so this holds in every zone.
        if (timeZone.IsAmbiguousTime(value))
        {
            TimeSpan[] offsets = timeZone.GetAmbiguousTimeOffsets(value);
            TimeSpan earliest = offsets[0];
            for (int i = 1; i < offsets.Length; i++)
            {
                if (offsets[i] > earliest)
                {
                    earliest = offsets[i];
                }
            }

            return DateTime.SpecifyKind(value - earliest, DateTimeKind.Utc);
        }

        return TimeZoneInfo.ConvertTimeToUtc(value, timeZone);
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
