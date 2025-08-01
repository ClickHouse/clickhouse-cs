﻿using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class Enum16Type : EnumType
{
    public override string Name => "Enum16";

    public override string ToString() => "Enum16";

    public override object Read(ExtendedBinaryReader reader) => Lookup(reader.ReadInt16());

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var enumIndex = value is string enumStr ? (short)Lookup(enumStr) : Convert.ToInt16(value, CultureInfo.InvariantCulture);
        writer.Write(enumIndex);
    }
}
