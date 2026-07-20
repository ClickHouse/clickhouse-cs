using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class UInt16Type : IntegerType, ITypedReader<ushort>
{
    public override Type FrameworkType => typeof(ushort);

    public override object Read(ExtendedBinaryReader reader) => ReadValue(reader);

    public ushort ReadValue(ExtendedBinaryReader reader) => reader.ReadUInt16();

    public override string ToString() => "UInt16";

    public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(Convert.ToUInt16(value, CultureInfo.InvariantCulture));
}
