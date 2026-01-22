using ClickHouse.Driver.Json;

namespace ClickHouse.Driver;

internal record struct TypeSettings(bool useBigDecimal, JsonTypeRegistry jsonTypeRegistry, JsonReadMode jsonReadMode, JsonWriteMode jsonWriteMode)
{
    public static TypeSettings Default => new TypeSettings(true, null, JsonReadMode.Binary, JsonWriteMode.String);
}
