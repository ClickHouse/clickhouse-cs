using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class Int64Type : IntegerType, ITypedReader<long>, ITypedWriter<long>
{
    public override Type FrameworkType => typeof(long);

    public override object Read(ExtendedBinaryReader reader) => ReadValue(reader);

    public long ReadValue(ExtendedBinaryReader reader) => reader.ReadInt64();

    public override string ToString() => "Int64";

    public override void Write(ExtendedBinaryWriter writer, object value) => WriteValue(writer, Convert.ToInt64(value, CultureInfo.InvariantCulture));

    public void WriteValue(ExtendedBinaryWriter writer, long value) => writer.Write(value);
}
