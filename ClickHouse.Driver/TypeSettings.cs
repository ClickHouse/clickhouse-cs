using ClickHouse.Driver.Json;

namespace ClickHouse.Driver;

internal record struct TypeSettings(bool useBigDecimal, bool readStringsAsByteArrays, JsonTypeRegistry jsonTypeRegistry, JsonReadMode jsonReadMode, JsonWriteMode jsonWriteMode, MapReadMode mapReadMode = MapReadMode.Dictionary, bool allowDuplicateJsonKeys = false, JsonNullPathMode jsonNullPathMode = JsonNullPathMode.Include)
{
    public static TypeSettings Default => new TypeSettings(true, false, null, JsonReadMode.Binary, JsonWriteMode.String, MapReadMode.Dictionary, false, JsonNullPathMode.Include);
}
