using NodaTime;

namespace ClickHouse.Driver;

internal record struct TypeSettings(bool useBigDecimal)
{
    public static TypeSettings Default => new TypeSettings(true);
}
