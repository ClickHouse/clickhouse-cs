﻿using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class UInt64Type : IntegerType
{
    public override Type FrameworkType => typeof(ulong);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadUInt64();

    public override string ToString() => "UInt64";

    public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(Convert.ToUInt64(value, CultureInfo.InvariantCulture));
}
