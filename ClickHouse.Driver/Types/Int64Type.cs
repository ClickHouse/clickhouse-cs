using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class Int64Type : IntegerType
{
    public override Type FrameworkType => typeof(long);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadInt64();

    public override string ToString() => "Int64";

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        if (value is not long longValue)
        {
            longValue = Convert.ToInt64(value, CultureInfo.InvariantCulture);
        }

        writer.Write(longValue);
    }
}
