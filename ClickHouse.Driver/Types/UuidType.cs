using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class UuidType : ClickHouseType
{
    public override Type FrameworkType => typeof(Guid);

    public override object Read(ExtendedBinaryReader reader)
    {
        // Byte manipulation because of ClickHouse's weird GUID/UUID implementation
        var bytes = new byte[16];
        reader.Read(bytes, 6, 2);
        reader.Read(bytes, 4, 2);
        reader.Read(bytes, 0, 4);
        reader.Read(bytes, 8, 8);
        Array.Reverse(bytes, 8, 8);
        return new Guid(bytes);
    }

    public override string ToString() => "UUID";

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        if (value is not Guid guid)
        {
            guid = new Guid((string)(object)value);
        }

        var bytes = guid.ToByteArray();
        Array.Reverse(bytes, 8, 8);
        writer.Write(bytes, 6, 2);
        writer.Write(bytes, 4, 2);
        writer.Write(bytes, 0, 4);
        writer.Write(bytes, 8, 8);
    }
}
