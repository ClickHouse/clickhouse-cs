namespace ClickHouse.Driver.Types;

internal class LineStringType : ArrayType
{
    public LineStringType()
    {
        UnderlyingType = new PointType();
    }

    public override string ToString() => "LineString";
}
