﻿using System;
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

    public override string ToString() => TimeZone == null ? $"DateTime64({Scale})" : $"DateTime64({Scale}, {TimeZone.Id})";

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
            timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName);
        }
        timeZone ??= DateTimeZoneProviders.Tzdb.GetZoneOrNull(settings.timezone);

        return new DateTime64Type
        {
            TimeZone = timeZone,
            Scale = scale,
        };
    }

    public override object Read(ExtendedBinaryReader reader) => FromClickHouseTicks(reader.ReadInt64());

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        writer.Write(ToClickHouseTicks(Instant.FromDateTimeOffset(CoerceToDateTimeOffset(value))));
    }
}
