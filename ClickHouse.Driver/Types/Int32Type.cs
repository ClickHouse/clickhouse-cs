using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class Int32Type : IntegerType
{
    public override Type FrameworkType => typeof(int);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadInt32();

    public override string ToString() => "Int32";

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        if (value is not int intValue)
        {
            intValue = Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        writer.Write(intValue);
    }
}
