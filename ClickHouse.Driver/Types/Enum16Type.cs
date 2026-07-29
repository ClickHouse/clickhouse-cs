using System;
using System.Collections.Generic;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class Enum16Type : EnumType, ITypedReader<string>, ITypedReader<int>
{
    public Enum16Type() { }

    public Enum16Type(Dictionary<string, int> values)
        : base(values) { }

    public override string Name => "Enum16";

    // See Enum8Type for why the label and numeric reads are explicit impls, and why the numeric read skips
    // the label lookup.
    public override object Read(ExtendedBinaryReader reader) => ReadLabel(reader);

    string ITypedReader<string>.ReadValue(ExtendedBinaryReader reader) => ReadLabel(reader);

    int ITypedReader<int>.ReadValue(ExtendedBinaryReader reader) => reader.ReadInt16();

    private string ReadLabel(ExtendedBinaryReader reader) => Lookup(reader.ReadInt16());

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var enumIndex = value is string enumStr ? (short)Lookup(enumStr) : Convert.ToInt16(value, CultureInfo.InvariantCulture);
        writer.Write(enumIndex);
    }
}
