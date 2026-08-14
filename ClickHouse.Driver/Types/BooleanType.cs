using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class BooleanType : ClickHouseType, ITypedReader<bool>, ITypedWriter<bool>
{
    public override Type FrameworkType => typeof(bool);

    public override object Read(ExtendedBinaryReader reader) => ReadValue(reader);

    public bool ReadValue(ExtendedBinaryReader reader) => reader.ReadBoolean();

    public override string ToString() => "Bool";

    public override void Write(ExtendedBinaryWriter writer, object value) => WriteValue(writer, (bool)value);

    public void WriteValue(ExtendedBinaryWriter writer, bool value) => writer.Write(value);
}
