﻿using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;
using NodaTime;

namespace ClickHouse.Driver.Types;

internal class DateTimeType : AbstractDateTimeType
{
    public override string Name => "DateTime";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        DateTimeZone timeZone = null;
        if (node.ChildNodes.Count > 0)
        {
            var timeZoneName = node.ChildNodes[0].Value.Trim('\'');
            timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(timeZoneName);
        }
        timeZone ??= DateTimeZoneProviders.Tzdb.GetZoneOrNull(settings.timezone);

        return new DateTimeType { TimeZone = timeZone };
    }

    public override object Read(ExtendedBinaryReader reader) => ToDateTime(Instant.FromUnixTimeSeconds(reader.ReadUInt32()));

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        writer.Write((int)CoerceToDateTimeOffset(value).ToUnixTimeSeconds());
    }
}
