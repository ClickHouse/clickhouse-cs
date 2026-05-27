using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class NothingType : ClickHouseType
{
    public override Type FrameworkType => typeof(DBNull);

    public override object Read(ExtendedBinaryReader reader) => DBNull.Value;

    public override string ToString() => "Nothing";

    public override void Write<T>(ExtendedBinaryWriter writer, T value) { }
}
