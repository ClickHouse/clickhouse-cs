namespace ClickHouse.Driver;

internal record struct TypeSettings(bool useBigDecimal, bool readStringsAsByteArrays)
{
    public static TypeSettings Default => new TypeSettings(true, false);
}
