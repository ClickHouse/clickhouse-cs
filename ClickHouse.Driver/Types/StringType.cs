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
        switch (value)
        {
            case  string str:
                writer.Write(str.Length);
                break;
            case  byte[] bytes:
                writer.Write7BitEncodedInt(bytes.Length);
                writer.Write(bytes);
                break;
            case ReadOnlyMemory<byte> memory:
                var span = memory.Span;
                writer.Write(span.Length);
                writer.Write(span);
                break;
            default:
                throw new ArgumentException($"String type expects string, byte[], or ReadOnlyMemory<byte>, but got {value?.GetType().Name ?? "null"}");
        }
    }
}
