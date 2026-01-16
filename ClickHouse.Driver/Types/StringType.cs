using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class StringType : ClickHouseType
{
    public override Type FrameworkType => typeof(string);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadString();

    public override string ToString() => "String";

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is string s)
        {
            writer.Write(s);
        }
        else if (value is byte[] b)
        {
            writer.Write7BitEncodedInt(b.Length);
            writer.Write(b);
        }
        else
        {
            throw new ArgumentException($"String requires string or byte[], got {value?.GetType().Name ?? "null"}");
        }
    }
}
