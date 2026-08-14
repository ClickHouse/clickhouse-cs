using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class BooleanType : ClickHouseType, ITypedReader<bool>
{
    public override Type FrameworkType => typeof(bool);

    public override object Read(ExtendedBinaryReader reader) => ReadValue(reader);

    public bool ReadValue(ExtendedBinaryReader reader) => reader.ReadBoolean();

    public override string ToString() => "Bool";

    public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write((bool)value);
}
