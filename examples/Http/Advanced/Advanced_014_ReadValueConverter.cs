using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates using IReadValueConverter to transform values returned by the data reader.
/// This is useful when you want to customize values after deserialization but before they
/// reach your code — for example, setting DateTime.Kind, converting units, or normalizing strings.
/// </summary>
public static class ReadValueConverter
{
    public static async Task Run()
    {
        // Basic path: register per-CLR-type transforms with DictionaryReadValueConverter
        await DictionaryConverterExample();

        // Advanced: implement IReadValueConverter directly for full control
        await CustomConverterExample();

        // Override the converter for a specific query via QueryOptions
        await PerQueryConverterExample();

        // Works through the ADO.NET ClickHouseConnection path too
        await AdoNetExample();
    }

    /// <summary>
    /// The recommended way to use IReadValueConverter: register per-CLR-type transforms
    /// with DictionaryReadValueConverter. Values whose runtime type is not registered
    /// pass through unchanged, so columns you don't care about pay nothing.
    /// </summary>
    private static async Task DictionaryConverterExample()
    {
        Console.WriteLine("1. DictionaryReadValueConverter - register transforms per CLR type:");

        var converter = new DictionaryReadValueConverter()
            .For<DateTime>(dt => DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            .For<string>(s => s.Trim());

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ReadValueConverter = converter,
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-06-15 12:30:45') as dt, '  hello  ' as s, toInt32(42) as n");

        while (reader.Read())
        {
            var dt = reader.GetFieldValue<DateTime>(0);
            Console.WriteLine($"   DateTime (Kind set to Utc): {dt:yyyy-MM-dd HH:mm:ss}, Kind={dt.Kind}");
            Console.WriteLine($"   String (trimmed): '{reader.GetFieldValue<string>(1)}'");
            Console.WriteLine($"   Int32 (unregistered, unchanged): {reader.GetFieldValue<int>(2)}");
        }
    }

    /// <summary>
    /// For cases that need more than CLR-type-keyed dispatch — for example, distinguishing
    /// <c>DateTime</c> (no timezone) from <c>DateTime('UTC')</c> using the ClickHouse-side type
    /// name — implement IReadValueConverter directly.
    /// </summary>
    private static async Task CustomConverterExample()
    {
        Console.WriteLine("\n2. Custom IReadValueConverter - dispatch on ClickHouse-side type:");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ReadValueConverter = new UtcOnlyForNoTzDateTimeConverter(),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-06-15 12:30:45') as no_tz, toDateTime('2025-06-15 12:30:45', 'Europe/Berlin') as berlin");

        while (reader.Read())
        {
            var noTz = reader.GetFieldValue<DateTime>(0);
            var berlin = reader.GetFieldValue<DateTime>(1);
            Console.WriteLine($"   no-tz column   -> Kind={noTz.Kind} (forced to Utc)");
            Console.WriteLine($"   Europe/Berlin -> Kind={berlin.Kind} (left as-is)");
        }
    }

    /// <summary>
    /// QueryOptions.ReadValueConverter overrides the client-level converter for a single query.
    /// </summary>
    private static async Task PerQueryConverterExample()
    {
        Console.WriteLine("\n3. Per-query converter override via QueryOptions:");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ReadValueConverter = new DictionaryReadValueConverter()
                .For<DateTime>(dt => DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
        };
        using var client = new ClickHouseClient(settings);

        using (var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-06-15 12:30:45') as dt"))
        {
            while (reader.Read())
            {
                var dt = reader.GetFieldValue<DateTime>(0);
                Console.WriteLine($"   Client converter (Utc): Kind={dt.Kind}");
            }
        }

        var options = new QueryOptions
        {
            ReadValueConverter = new DictionaryReadValueConverter()
                .For<DateTime>(dt => DateTime.SpecifyKind(dt, DateTimeKind.Local)),
        };

        using (var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-06-15 12:30:45') as dt", options: options))
        {
            while (reader.Read())
            {
                var dt = reader.GetFieldValue<DateTime>(0);
                Console.WriteLine($"   Query converter (Local): Kind={dt.Kind}");
            }
        }
    }

    /// <summary>
    /// The converter works through the ADO.NET ClickHouseConnection path too.
    /// </summary>
    private static async Task AdoNetExample()
    {
        Console.WriteLine("\n4. Works with ClickHouseConnection (ADO.NET):");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ReadValueConverter = new DictionaryReadValueConverter()
                .For<DateTime>(dt => DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
        };
        using var connection = new ClickHouseConnection(settings);

        using var reader = await connection.ExecuteReaderAsync(
            "SELECT toDateTime('2025-06-15 12:30:45') as dt");
        while (reader.Read())
        {
            var dt = (DateTime)reader.GetValue(0);
            Console.WriteLine($"   DateTime via ADO.NET: Kind={dt.Kind}");
        }
    }

    /// <summary>
    /// Example custom converter: forces Kind=Utc only for DateTime columns that have no
    /// timezone in the ClickHouse type, leaving zoned values untouched.
    /// </summary>
    private class UtcOnlyForNoTzDateTimeConverter : IReadValueConverter
    {
        public object ConvertValue(object value, string columnName, string clickHouseType)
        {
            if (value is DateTime dt && clickHouseType == "DateTime")
                return DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            return value;
        }

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
        {
            if (typeof(T) == typeof(DateTime) && value is DateTime dt && clickHouseType == "DateTime")
                return (T)(object)DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            return value;
        }
    }
}
