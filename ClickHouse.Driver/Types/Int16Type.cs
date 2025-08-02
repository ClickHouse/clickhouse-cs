﻿using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class Int16Type : IntegerType
{
    public override Type FrameworkType => typeof(short);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadInt16();

    public override string ToString() => "Int16";

    public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(Convert.ToInt16(value, CultureInfo.InvariantCulture));
}
