﻿using System;
using System.Globalization;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class Float64Type : FloatType
{
    public override Type FrameworkType => typeof(double);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadDouble();

    public override string ToString() => "Float64";

    public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(Convert.ToDouble(value, CultureInfo.InvariantCulture));
}
