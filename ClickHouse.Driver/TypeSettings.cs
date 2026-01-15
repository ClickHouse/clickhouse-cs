namespace ClickHouse.Driver;

internal record struct TypeSettings(bool useBigDecimal, JsonReadMode jsonReadMode, JsonWriteMode jsonWriteMode)
{
    public static TypeSettings Default => new TypeSettings(true, JsonReadMode.JsonNode, JsonWriteMode.JsonNode);
}
