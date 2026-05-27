using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class UInt32Type : IntegerType
{
    public override Type FrameworkType => typeof(uint);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadUInt32();

    public override string ToString() => "UInt32";

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        if (value is not uint uintValue)
        {
            uintValue = Convert.ToUInt32(value, CultureInfo.InvariantCulture);
        }

        writer.Write(uintValue);
    }
}
