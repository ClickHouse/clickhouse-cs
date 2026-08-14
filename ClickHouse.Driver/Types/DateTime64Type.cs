using System;
using System.Globalization;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;
using ClickHouse.Driver.Utility;
using NodaTime;

namespace ClickHouse.Driver.Types;

internal class DateTime64Type : AbstractDateTimeType, IInstantReader
{
    public int Scale { get; set; }

    public override string Name => "DateTime64";

    public override string ToString() => TimeZone == null ? $"DateTime64({Scale})" : $"DateTime64({Scale}, '{TimeZone.Id}')";

    public DateTime FromClickHouseTicks(long clickHouseTicks) => ToDateTime(ToInstant(clickHouseTicks));

    // Converts ClickHouse variable precision ticks into "standard" .NET 100ns ones
    public Instant ToInstant(long clickHouseTicks) => Instant.FromUnixTimeTicks(MathUtils.ShiftDecimalPlaces(clickHouseTicks, 7 - Scale));

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

    // Same conversion as FromClickHouseTicks, exposing the intermediate instant so every read of this type
    // shares one wire read.
    private Instant ReadInstant(ExtendedBinaryReader reader) => ToInstant(reader.ReadInt64());

    bool IInstantReader.ReportsInstant => true;

    object IInstantReader.ReadWithInstant(ExtendedBinaryReader reader, out Instant? instant)
    {
        var decoded = ReadInstant(reader);
        instant = decoded;
        return ToDateTime(decoded);
    }

    // No range check: any coerced instant is representable, so 'original' is unused.
    protected override void WriteChecked<T>(ExtendedBinaryWriter writer, DateTimeOffset dto, T original)
        => writer.Write(ToClickHouseTicks(Instant.FromDateTimeOffset(dto)));
}
