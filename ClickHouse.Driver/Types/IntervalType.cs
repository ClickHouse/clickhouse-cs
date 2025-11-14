namespace ClickHouse.Driver.Types;

internal abstract class IntervalType : ClickHouseType
{
    public virtual bool Signed => true;
}
