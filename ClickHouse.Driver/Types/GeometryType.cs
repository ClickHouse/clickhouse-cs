namespace ClickHouse.Driver.Types;

internal class GeometryType : VariantType
{
    public GeometryType()
    {
        // Alphabetical order (as used by ClickHouse)
        UnderlyingTypes = new ClickHouseType[]
        {
            new LineStringType(),      // 0
            new MultiLineStringType(), // 1
            new MultiPolygonType(),    // 2
            new PointType(),           // 3
            new PolygonType(),         // 4
            new RingType(),            // 5
        };
    }

    public override string Name => "Geometry";

    public override string ToString() => "Geometry";
}
