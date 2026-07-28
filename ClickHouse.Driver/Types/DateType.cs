using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class DateType : AbstractDateTimeType
{
    public override string Name { get; }

    public override string ToString() => "Date";

    public override object Read(ExtendedBinaryReader reader) => DateTimeConversions.FromUnixTimeDays(reader.ReadUInt16());

    public override ParameterizedType Parse(SyntaxTreeNode typeName, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings) => throw new NotImplementedException();

    protected override void WriteChecked<T>(ExtendedBinaryWriter writer, DateTimeOffset dto, T value)
    {
        var days = dto.ToUnixTimeDays();
        if (days < 0 || days > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                value,
                "Value is outside the supported range for ClickHouse Date (1970-01-01 to 2149-06-06).");
        }

        writer.Write((ushort)days);
    }
}
