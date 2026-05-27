using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class PointType : TupleType
{
    public PointType()
    {
        UnderlyingTypes = new[] { new Float64Type(), new Float64Type() };
    }

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        if (value is System.Drawing.Point p)
        {
            var tuple = Tuple.Create(p.X, p.Y);
            base.Write(writer, tuple);
        }
        else
        {
            base.Write(writer, value);
        }
    }

    public override string ToString() => "Point";
}
