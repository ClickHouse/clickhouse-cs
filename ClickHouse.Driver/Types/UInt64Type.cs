using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class UInt64Type : IntegerType, ITypedReader<ulong>, ITypedWriter<ulong>
{
    public override Type FrameworkType => typeof(ulong);

    public override object Read(ExtendedBinaryReader reader) => ReadValue(reader);

    public ulong ReadValue(ExtendedBinaryReader reader) => reader.ReadUInt64();

    public override string ToString() => "UInt64";

    public override void Write(ExtendedBinaryWriter writer, object value) => WriteValue(writer, Convert.ToUInt64(value, CultureInfo.InvariantCulture));

    public void WriteValue(ExtendedBinaryWriter writer, ulong value) => writer.Write(value);
}
