using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class UInt64Type : IntegerType
{
    public override Type FrameworkType => typeof(ulong);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadUInt64();

    public override string ToString() => "UInt64";

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        if (value is not ulong ulongValue)
        {
            ulongValue = Convert.ToUInt64(value, CultureInfo.InvariantCulture);
        }

        writer.Write(ulongValue);
    }
}
