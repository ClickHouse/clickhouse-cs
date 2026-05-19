using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class BooleanType : ClickHouseType
{
    public override Type FrameworkType => typeof(bool);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadBoolean();

    public override string ToString() => "Bool";

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        if (value is bool bValue)
        {
            writer.Write(bValue);
        }
        else
        {
            writer.Write((bool)(object)value);
        }
    }
}
