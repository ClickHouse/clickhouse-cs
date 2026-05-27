using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class UInt8Type : IntegerType
{
    public override Type FrameworkType => typeof(byte);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadByte();

    public override string ToString() => "UInt8";

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        if (value is not byte byteValue)
        {
            byteValue = Convert.ToByte(value, CultureInfo.InvariantCulture);
        }

        writer.Write(byteValue);
    }
}
