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
        // Set DateTime.Kind to Utc for all DateTime columns
        await DateTimeKindExample();

        // Transform multiple types with a single converter
        await MultiTypeConverterExample();

        // Override the converter for a specific query via QueryOptions
        await PerQueryConverterExample();

        // Works through the ADO.NET ClickHouseConnection path too
        await AdoNetExample();
    }

    /// <summary>
    /// The most common use case: ClickHouse DateTime columns without an explicit timezone
    /// return DateTime with Kind=Unspecified. Use a converter to set Kind=Utc globally.
    /// </summary>
    private static async Task DateTimeKindExample()
    {
        Console.WriteLine("1. DateTimeKindConverter - Set Kind=Utc on all DateTime values:");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        // DateTime column without timezone returns Kind=Unspecified by default.
        // With the converter, it returns Kind=Utc.
        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-06-15 12:30:45') as dt, toInt32(42) as num");

        while (reader.Read())
        {
            var dt = reader.GetFieldValue<DateTime>(0);
            Console.WriteLine($"   DateTime: {dt:yyyy-MM-dd HH:mm:ss}, Kind: {dt.Kind}");
            Console.WriteLine($"   Int32 (unaffected): {reader.GetFieldValue<int>(1)}");
        }
    }

    /// <summary>
    /// A converter can transform multiple types. Here we trim + uppercase strings,
    /// double integers, and leave other types alone (contrived, but demonstrates the pattern).
    /// </summary>
    private static async Task MultiTypeConverterExample()
    {
        Console.WriteLine("\n2. MultiTypeConverter - Transform multiple types:");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ReadValueConverter = new MultiTypeConverter(),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT '  hello world  ' as raw_string, toInt32(21) as n, toFloat64(3.14) as pi");

        while (reader.Read())
        {
            Console.WriteLine($"   String (trim + upper): '{reader.GetFieldValue<string>(0)}'");
            Console.WriteLine($"   Int32 (doubled):        {reader.GetFieldValue<int>(1)}");
            Console.WriteLine($"   Float64 (unaffected):   {reader.GetFieldValue<double>(2)}");
        }
    }

    /// <summary>
    /// QueryOptions.ReadValueConverter overrides the client-level converter for a single query.
    /// </summary>
    private static async Task PerQueryConverterExample()
    {
        Console.WriteLine("\n3. Per-query converter override via QueryOptions:");

        // Client-level: Kind=Utc
        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        // Without query options: uses client converter (Kind=Utc)
        using (var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-06-15 12:30:45') as dt"))
        {
            while (reader.Read())
            {
                var dt = reader.GetFieldValue<DateTime>(0);
                Console.WriteLine($"   Client converter (Utc): Kind={dt.Kind}");
            }
        }

        // With query options: overrides to Kind=Local
        var options = new QueryOptions
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Local),
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
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
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
    /// A converter that sets DateTime.Kind for all DateTime values.
    /// This is the most common real-world use case.
    /// </summary>
    private class DateTimeKindConverter : IReadValueConverter
    {
        private readonly DateTimeKind kind;

        public DateTimeKindConverter(DateTimeKind kind) => this.kind = kind;

        public object ConvertValue(object value, string columnName, string clickHouseType)
        {
            if (value is DateTime dt)
                return DateTime.SpecifyKind(dt, kind);
            return value;
        }

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
        {
            if (typeof(T) == typeof(DateTime) && value is DateTime dt)
                return (T)(object)DateTime.SpecifyKind(dt, kind);
            return value;
        }
    }

    /// <summary>
    /// A converter that handles multiple types in a single implementation:
    /// trims and uppercases strings, doubles integers, passes everything else through.
    /// </summary>
    private class MultiTypeConverter : IReadValueConverter
    {
        public object ConvertValue(object value, string columnName, string clickHouseType)
        {
            if (value is string s)
                return s.Trim().ToUpperInvariant();
            if (value is int i)
                return i * 2;
            return value;
        }

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
        {
            if (typeof(T) == typeof(string) && value is string s)
                return (T)(object)s.Trim().ToUpperInvariant();
            if (typeof(T) == typeof(int) && value is int i)
                return (T)(object)(i * 2);
            return value;
        }
    }
}
