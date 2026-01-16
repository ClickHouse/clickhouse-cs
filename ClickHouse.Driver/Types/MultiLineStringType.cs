namespace ClickHouse.Driver.Types;

internal class MultiLineStringType : ArrayType
{
    public MultiLineStringType()
    {
        UnderlyingType = new LineStringType();
    }

    public override string ToString() => "MultiLineString";
}
