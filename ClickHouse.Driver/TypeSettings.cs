using ClickHouse.Driver.Json;

namespace ClickHouse.Driver;

internal record struct TypeSettings(bool useBigDecimal, JsonTypeRegistry jsonTypeRegistry)
{
    public static TypeSettings Default => new TypeSettings(true, null);
}
