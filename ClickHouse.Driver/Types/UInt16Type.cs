using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class UInt16Type : IntegerType
{
    public override Type FrameworkType => typeof(ushort);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadUInt16();

    public override string ToString() => "UInt16";

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        if (value is not ushort ushortValue)
        {
            ushortValue = Convert.ToUInt16(value, CultureInfo.InvariantCulture);
        }

        writer.Write(ushortValue);
    }
}
