using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>DateTime64(scale[, 'tz'])</c> column: a little-endian <c>Int64</c> tick count
/// at 10^-<c>scale</c> seconds since the Unix epoch (may be negative), surfaced as a
/// <see cref="ClickHouseDateTime64"/> that retains the exact wire value at any scale (including scales 8 and 9,
/// which are finer than a .NET tick). The timezone (explicit or the session's) sets the offset each value is
/// presented with, resolved per instant so daylight-saving transitions are honored.
/// </summary>
internal sealed class DateTime64ColumnCodec : IColumnCodec
{
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
    public int? FixedRowByteSize => sizeof(long);

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

        if (scale is < 0 or > ClickHouseDateTime64.MaxScale)
        {
            throw new FormatException($"DateTime64 scale {scale} is out of the supported range 0..{ClickHouseDateTime64.MaxScale}.");
        }

        string explicitTz = node.Arguments.Count > 1 ? DateTimeZones.UnquoteTimezone(node.Arguments[1]) : null;
        TimeZoneInfo tz = DateTimeZones.Resolve(explicitTz, serverTimezone);
        return new DateTime64ColumnCodec(node.ToString(), scale, tz);
    }

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => DateTime64Column.ReadAsync(reader, columnName, columnType, scale, timeZone, rowCount, cancellationToken);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<ClickHouseDateTime64> or IColumn<DateTimeOffset> or IColumn<DateTime>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        switch (column)
        {
            case IColumn<ClickHouseDateTime64> native:
                foreach (ClickHouseDateTime64 value in native.Values.Slice(start, length))
                {
                    writer.WriteInt64(RescaleCount(value.Count, value.Scale, scale, TypeName));
                }

                break;

            case IColumn<DateTimeOffset> offsets:
                foreach (DateTimeOffset value in offsets.Values.Slice(start, length))
                {
                    writer.WriteInt64(ClickHouseDateTime64.FromDateTimeOffset(value, scale).Count);
                }

                break;

            case IColumn<DateTime> dateTimes:
                foreach (DateTime value in dateTimes.Values.Slice(start, length))
                {
                    writer.WriteInt64(ClickHouseDateTime64.FromDateTimeOffset(new DateTimeOffset(DateTimeColumnCodec.ToUtc(value, timeZone)), scale).Count);
                }

                break;

            default:
                throw new ArgumentException(
                    $"A DateTime64 column must hold ClickHouseDateTime64, DateTimeOffset, or DateTime values, not {column.GetType()}.",
                    nameof(column));
        }
    }

    // Rescales a raw count from its own scale to the column's: a widening (more digits) multiplies exactly; a
    // narrowing (fewer digits) must divide evenly, since dropping non-zero digits would silently lose precision.
    private static long RescaleCount(long count, int fromScale, int toScale, string typeName)
    {
        int shift = toScale - fromScale;
        if (shift == 0)
        {
            return count;
        }

        long factor = Pow10(Math.Abs(shift));
        if (shift > 0)
        {
            return checked(count * factor);
        }

        (long quotient, long remainder) = (count / factor, count % factor);
        if (remainder != 0)
        {
            throw new ArgumentException($"A DateTime64 value at scale {fromScale} cannot be written to {typeName} (scale {toScale}) without losing precision.", "column");
        }

        return quotient;
    }

    private static long Pow10(int exponent)
    {
        long result = 1;
        for (int i = 0; i < exponent; i++)
        {
            result *= 10;
        }

        return result;
    }
}
