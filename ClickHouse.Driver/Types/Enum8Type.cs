using System;
using System.Collections.Generic;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class Enum8Type : EnumType, ITypedReader<string>, ITypedReader<int>
{
    public Enum8Type() { }

    public Enum8Type(Dictionary<string, int> values)
        : base(values) { }

    public override string Name => "Enum8";

    // The typed read offers the raw numeric value as well as the label. It skips the label lookup by design:
    // the stored value stands on its own, so unlike the label read it cannot fail on a value the column's
    // enum definition omits.
    public override object Read(ExtendedBinaryReader reader) => ReadLabel(reader);

    string ITypedReader<string>.ReadValue(ExtendedBinaryReader reader) => ReadLabel(reader);

    int ITypedReader<int>.ReadValue(ExtendedBinaryReader reader) => reader.ReadSByte();

    private string ReadLabel(ExtendedBinaryReader reader) => Lookup(reader.ReadSByte());

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var enumIndex = value is string enumStr ? (sbyte)Lookup(enumStr) : Convert.ToSByte(value, CultureInfo.InvariantCulture);
        writer.Write(enumIndex);
    }
}
