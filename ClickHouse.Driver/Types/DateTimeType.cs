using System;
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
            timeZone = ResolveTimezone(timeZoneName);
        }

        return new DateTimeType { TimeZone = timeZone };
    }

    public override object Read(ExtendedBinaryReader reader) => ToDateTime(Instant.FromUnixTimeSeconds(reader.ReadUInt32()));

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var seconds = CoerceToDateTimeOffset(value).ToUnixTimeSeconds();
        if (seconds < 0L || seconds > uint.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                value,
                $"Value is outside the supported range for ClickHouse {Name} (1970-01-01 00:00:00 UTC to 2106-02-07 06:28:15 UTC).");
        }

        writer.Write((uint)seconds);
    }
}
