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

    // The boxed read returns the canonical label. The typed read fast path can additionally produce the raw
    // numeric value from the same wire byte (explicit impls because they differ only by return type), so both
    // are byte-identical to the boxed path by construction. The numeric read deliberately skips the label
    // lookup — it is the stored value itself, so unlike the label read it cannot fail on a value that is
    // absent from the column's enum definition.
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
