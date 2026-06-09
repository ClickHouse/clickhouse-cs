using System.Collections;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates the column types supported by the Native (TCP) protocol MVP: fixed-width scalars
/// (integers, floats, Date/DateTime), String, Nullable(T), and Array(T). The values are produced
/// with explicit casts in a single SELECT so no table setup is required.
/// </summary>
public static class NativeDataTypes
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost;Protocol=native");

        const string sql = @"
            SELECT
                toInt32(-42)                          AS i32,
                toUInt64(123456789)                   AS u64,
                toFloat64(3.14159)                    AS f64,
                'hello native'                        AS str,
                toDate('2024-01-15')                  AS d,
                toDateTime('2024-01-15 12:30:00')     AS dt,
                CAST(NULL AS Nullable(Int32))         AS nullable_null,
                CAST(7 AS Nullable(Int32))            AS nullable_value,
                [1, 2, 3]                             AS int_array,
                ['a', 'b', 'c']                       AS string_array";

        using var reader = await client.ExecuteReaderAsync(sql);

        if (!await reader.ReadAsync())
        {
            Console.WriteLine("No rows returned.");
            return;
        }

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            var typeName = reader.GetDataTypeName(i);
            var value = Format(reader.GetValue(i));
            Console.WriteLine($"{name,-15} {typeName,-20} = {value}");
        }

        Console.WriteLine("\nAll values above were decoded from the Native columnar format.");
    }

    private static string Format(object? value) => value switch
    {
        null or DBNull => "NULL",
        string s => $"\"{s}\"",
        // Arrays (including byte[]) come back as CLR arrays; render them as [a, b, c].
        IEnumerable e and not string => "[" + string.Join(", ", e.Cast<object>().Select(Format)) + "]",
        _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "NULL",
    };
}
