using NodaTime;

namespace ClickHouse.Driver;

internal record struct TypeSettings(bool useBigDecimal, string timezone, bool mapAsListOfTuples)
{
    public static string DefaultTimezone = DateTimeZoneProviders.Tzdb.GetSystemDefault().Id;

    public static TypeSettings Default => new TypeSettings(true, DefaultTimezone, false);
}
