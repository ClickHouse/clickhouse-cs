using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class Date32Type : DateType
{
    public override string Name { get; }

    public override string ToString() => "Date32";

    public override object Read(ExtendedBinaryReader reader) => DateTimeConversions.FromUnixTimeDays(reader.ReadInt32());

    public override ParameterizedType Parse(SyntaxTreeNode typeName, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings) => throw new NotImplementedException();

    // ClickHouse Date32 days-from-epoch range: [1900-01-01, 2299-12-31].
    private const int Date32MinUnixDays = -25567;
    private const int Date32MaxUnixDays = 120529;

    protected override void WriteChecked<T>(ExtendedBinaryWriter writer, DateTimeOffset dto, T value)
    {
        var days = dto.ToUnixTimeDays();
        if (days < Date32MinUnixDays || days > Date32MaxUnixDays)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                value,
                "Value is outside the supported range for ClickHouse Date32 (1900-01-01 to 2299-12-31).");
        }

        writer.Write(days);
    }
}
