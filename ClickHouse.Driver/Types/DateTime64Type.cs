using System;
using System.Globalization;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;
using ClickHouse.Driver.Utility;
using NodaTime;

namespace ClickHouse.Driver.Types;

internal class DateTime64Type : AbstractDateTimeType
{
    public int Scale { get; set; }

    public override string Name => "DateTime64";

    public override string ToString() => TimeZone == null ? $"DateTime64({Scale})" : $"DateTime64({Scale}, '{TimeZone.Id}')";

    public DateTime FromClickHouseTicks(long clickHouseTicks)
    {
        // Convert ClickHouse variable precision ticks into "standard" .NET 100ns ones
        var ticks = MathUtils.ShiftDecimalPlaces(clickHouseTicks, 7 - Scale);
        return ToDateTime(Instant.FromUnixTimeTicks(ticks));
    }

    public long ToClickHouseTicks(Instant instant) => MathUtils.ShiftDecimalPlaces(instant.ToUnixTimeTicks(), Scale - 7);

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        var scale = int.Parse(node.ChildNodes[0].Value, CultureInfo.InvariantCulture);

        DateTimeZone timeZone = null;
        if (node.ChildNodes.Count > 1)
        {
            var timeZoneName = node.ChildNodes[1].Value.Trim('\'');
            timeZone = ResolveTimezone(timeZoneName);
        }

        return new DateTime64Type
        {
            TimeZone = timeZone,
            Scale = scale,
        };
    }

    protected override DateTime ReadDateTime(ExtendedBinaryReader reader) => ToDateTime(ReadInstant(reader));

    protected override DateTimeOffset ReadDateTimeOffset(ExtendedBinaryReader reader) => ToDateTimeOffset(ReadInstant(reader));

    // Same conversion as FromClickHouseTicks, exposing the intermediate instant so both the DateTime and the
    // DateTimeOffset read share one wire read.
    private Instant ReadInstant(ExtendedBinaryReader reader)
        => Instant.FromUnixTimeTicks(MathUtils.ShiftDecimalPlaces(reader.ReadInt64(), 7 - Scale));

    // Scales finer than a .NET tick reach the Int64 limit inside the DateTime range (DateTime64(9): 1677 to 2262),
    // so check before scaling up: an unchecked multiply would wrap and write a different instant.
    protected override void WriteChecked<T>(ExtendedBinaryWriter writer, DateTimeOffset dto, T value)
    {
        var instant = Instant.FromDateTimeOffset(dto);
        if (Scale <= 7)
        {
            writer.Write(ToClickHouseTicks(instant));
            return;
        }

        var factor = MathUtils.ToPower(10, Scale - 7);
        var ticks = instant.ToUnixTimeTicks();
        if (ticks > long.MaxValue / factor || ticks < long.MinValue / factor)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                value,
                $"Value is outside the range that ClickHouse {this} can store as an Int64 count of 10^-{Scale} s since the epoch ({DescribeInt64Range(factor)}).");
        }

        writer.Write(ticks * factor);
    }

    // The lower limit of DateTime64(8) is before DateTime.MinValue, so only its upper limit can be reached.
    private static string DescribeInt64Range(long factor)
    {
        var minTicks = long.MinValue / factor;
        var max = FormatUnixTicks(long.MaxValue / factor);
        return minTicks < DateTime.MinValue.Ticks - DateTime.UnixEpoch.Ticks ? $"up to {max}" : $"{FormatUnixTicks(minTicks)} to {max}";
    }

    private static string FormatUnixTicks(long unixTicks)
        => DateTime.UnixEpoch.AddTicks(unixTicks).ToString("yyyy-MM-dd HH:mm:ss.fffffff 'UTC'", CultureInfo.InvariantCulture);
}
