using System.Globalization;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates using IParameterFormatter to control how parameter values are
/// serialized for HTTP transport. This is the value-formatting counterpart to
/// IParameterTypeResolver — the resolver decides which ClickHouse type a value
/// is sent as, and the formatter decides how the value's bytes look on the wire.
///
/// Use this when the built-in formatting (DateTime precision, decimal culture,
/// string escaping, number representation) does not match what your schema or
/// downstream consumers expect.
/// </summary>
public static class ParameterFormatter
{
    public static async Task Run()
    {
        // A DictionaryParameterFormatter keyed on CLR type
        await DictionaryFormatterExample();

        // The formatter also runs on every element of composite values (arrays, tuples, maps)
        await CompositeElementExample();

        // Custom IParameterFormatter for more complex logic
        await CustomFormatterExample();

        // Per-query override via QueryOptions
        await PerQueryFormatterExample();
    }

    /// <summary>
    /// The built-in DictionaryParameterFormatter maps CLR types to a format function.
    /// Types not in the dictionary fall through to default formatting.
    /// </summary>
    private static async Task DictionaryFormatterExample()
    {
        Console.WriteLine("1. DictionaryParameterFormatter - custom DateTime serialization:");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ParameterFormatter = new DictionaryParameterFormatter(new Dictionary<Type, Func<object, string>>
            {
                // Force a fixed ISO-8601 format with microsecond precision, regardless of DateTime.Kind
                [typeof(DateTime)] = v => ((DateTime)v).ToString("yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture),
            }),
        };
        using var client = new ClickHouseClient(settings);

        var dt = new DateTime(2025, 6, 15, 12, 30, 45, 123, DateTimeKind.Unspecified);
        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "dt",
            Value = dt,
            ClickHouseType = "DateTime64(6)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @dt as res", parameters);
        while (reader.Read())
        {
            Console.WriteLine($"   Sent:     {dt:yyyy-MM-ddTHH:mm:ss.ffffff}");
            Console.WriteLine($"   Received: {reader.GetDateTime(0):yyyy-MM-ddTHH:mm:ss.ffffff}");
        }
    }

    /// <summary>
    /// The formatter runs on every element inside composite values (arrays, tuples,
    /// maps, nullables, low-cardinality, variants). Here we double every int
    /// element inside an array.
    /// </summary>
    private static async Task CompositeElementExample()
    {
        Console.WriteLine("\n2. Formatter runs on array elements too:");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ParameterFormatter = new DictionaryParameterFormatter(new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v * 2).ToString(CultureInfo.InvariantCulture),
            }),
        };
        using var client = new ClickHouseClient(settings);

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "arr",
            Value = new[] { 1, 2, 3 },
            ClickHouseType = "Array(Int32)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @arr as res", parameters);
        while (reader.Read())
        {
            var arr = (int[])reader.GetValue(0);
            Console.WriteLine($"   Sent:     [1, 2, 3]");
            Console.WriteLine($"   Received: [{string.Join(", ", arr)}]");
        }
    }

    /// <summary>
    /// For advanced scenarios, implement IParameterFormatter directly.
    /// </summary>
    private static async Task CustomFormatterExample()
    {
        Console.WriteLine("\n3. Custom IParameterFormatter:");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ParameterFormatter = new DoublingDecimalFormatter(),
        };
        using var client = new ClickHouseClient(settings);

        var price = 19.9m;
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("price", price);

        using var reader = await client.ExecuteReaderAsync("SELECT @price as value", parameters);
        while (reader.Read())
        {
            Console.WriteLine($"   Parameter value:     {price.ToString(CultureInfo.InvariantCulture)}");
            Console.WriteLine($"   Received: {reader.GetValue(0)}");
        }
    }

    /// <summary>
    /// QueryOptions.ParameterFormatter overrides the client-level formatter for a single query.
    /// </summary>
    private static async Task PerQueryFormatterExample()
    {
        Console.WriteLine("\n4. Per-query formatter via QueryOptions:");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ParameterFormatter = new DictionaryParameterFormatter(new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v + 100).ToString(CultureInfo.InvariantCulture),
            }),
        };
        using var client = new ClickHouseClient(settings);

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 1);

        using (var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters))
        {
            while (reader.Read())
                Console.WriteLine($"   Client formatter: {reader.GetInt32(0)}");  // 101
        }

        var options = new QueryOptions
        {
            ParameterFormatter = new DictionaryParameterFormatter(new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v + 1000).ToString(CultureInfo.InvariantCulture),
            }),
        };

        using (var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters, options))
        {
            while (reader.Read())
                Console.WriteLine($"   Query formatter:  {reader.GetInt32(0)}");  // 1001
        }
    }

    /// <summary>
    /// Doubles every decimal value before it is serialized on the wire.
    /// Returns null for non-decimal values so they fall through to default formatting.
    /// </summary>
    private class DoublingDecimalFormatter : IParameterFormatter
    {
        public string Format(object value, string typeName, string parameterName)
        {
            if (value is decimal d)
                return (d * 2).ToString(CultureInfo.InvariantCulture);
            return null;
        }
    }
}
