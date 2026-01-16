using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class DynamicType : ClickHouseType
{
    public override Type FrameworkType => typeof(object);

    public TypeSettings TypeSettings { get; init; }

    public override string ToString() => "Dynamic";

    public override object Read(ExtendedBinaryReader reader) =>
        BinaryTypeDecoder.
            FromByteCode(reader, TypeSettings).
            Read(reader);

    public override void Write(ExtendedBinaryWriter writer, object value) =>
        throw new NotImplementedException();
}
