using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class UuidType : ClickHouseType, ITypedWriter<Guid>, ITypedReader<Guid>
{
    public override Type FrameworkType => typeof(Guid);

    public override object Read(ExtendedBinaryReader reader) => ReadValue(reader);

    public Guid ReadValue(ExtendedBinaryReader reader)
    {
        // Byte manipulation because of ClickHouse's weird GUID/UUID implementation
        Span<byte> bytes = stackalloc byte[16];
        reader.ReadBytes(bytes.Slice(6, 2));
        reader.ReadBytes(bytes.Slice(4, 2));
        reader.ReadBytes(bytes.Slice(0, 4));
        reader.ReadBytes(bytes.Slice(8, 8));
        bytes.Slice(8, 8).Reverse();
        return new Guid(bytes);
    }

    public override string ToString() => "UUID";

    public override void Write(ExtendedBinaryWriter writer, object value) => WriteValue(writer, ExtractGuid(value));

    public void WriteValue(ExtendedBinaryWriter writer, Guid value)
    {
        // TryWriteBytes lays the Guid out exactly as ToByteArray does, into a stack buffer instead of a
        // fresh array. It only fails on a destination shorter than 16 bytes, which this one never is.
        Span<byte> bytes = stackalloc byte[16];
        value.TryWriteBytes(bytes);
        bytes.Slice(8, 8).Reverse();
        writer.Write(bytes.Slice(6, 2));
        writer.Write(bytes.Slice(4, 2));
        writer.Write(bytes.Slice(0, 4));
        writer.Write(bytes.Slice(8, 8));
    }

    private static Guid ExtractGuid(object data) => data is Guid g ? g : new Guid((string)data);
}
