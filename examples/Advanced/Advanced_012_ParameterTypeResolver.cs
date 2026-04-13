using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates using IParameterTypeResolver to control the default ClickHouse type mapping
/// for @-style parameterized queries. This is useful when you want all DateTime parameters
/// to map to DateTime64(3) instead of DateTime, or all decimals to use a specific precision,
/// without setting ClickHouseType on every individual parameter.
/// </summary>
public static class ParameterTypeResolver
{
    public static async Task Run()
    {
        // Map DateTime to DateTime64(3) and decimal to Decimal64(4) using a dictionary
        await DictionaryResolverExample();

        // Implement IParameterTypeResolver to pick decimal precision based on the actual value
        await CustomResolverExample();

        // Explicit ClickHouseType on a parameter always overrides the resolver
        await ExplicitTypeOverrideExample();

        // The resolver works through the ADO.NET ClickHouseConnection path too
        await AdoNetExample();

        // Override the resolver for a specific query via QueryOptions
        await PerQueryResolverExample();
    }

    /// <summary>
    /// The built-in DictionaryParameterTypeResolver maps .NET types to
    /// ClickHouse types via a simple dictionary. Here, we configure DateTime
    /// to map to DateTime64(3) instead of the default DateTime.
    /// </summary>
    private static async Task DictionaryResolverExample()
    {
        Console.WriteLine("1. DictionaryParameterTypeResolver - DateTime mapped to DateTime64(3):");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ParameterTypeResolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
            {
                [typeof(DateTime)] = "DateTime64(3)",
                [typeof(decimal)] = "Decimal64(4)",
            }),
        };
        using var client = new ClickHouseClient(settings);

        var now = new DateTime(2025, 6, 15, 12, 30, 45, 123, DateTimeKind.Unspecified);
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("dt", now);
        parameters.AddParameter("amount", 99.1234m);

        // @dt is automatically resolved to DateTime64(3), preserving milliseconds.
        // Without the resolver, DateTime maps to DateTime (second precision only).
        using var reader = await client.ExecuteReaderAsync(
            "SELECT @dt as dt_value, toTypeName(@dt) as dt_type, @amount as amount_value, toTypeName(@amount) as amount_type",
            parameters);

        while (reader.Read())
        {
            Console.WriteLine($"   DateTime value: {reader.GetDateTime(0):yyyy-MM-dd HH:mm:ss.fff}");
            Console.WriteLine($"   DateTime type:  {reader.GetString(1)}");
            Console.WriteLine($"   Decimal value:  {reader.GetValue(2)}");
            Console.WriteLine($"   Decimal type:   {reader.GetString(3)}");
        }
    }

    /// <summary>
    /// For advanced scenarios, implement IParameterTypeResolver directly.
    /// The resolver receives the .NET type, the actual value, and the parameter name,
    /// so you can make decisions based on any of these.
    /// </summary>
    private static async Task CustomResolverExample()
    {
        Console.WriteLine("\n2. Custom IParameterTypeResolver - value-aware resolution:");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ParameterTypeResolver = new SmartDecimalResolver(),
        };
        using var client = new ClickHouseClient(settings);

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("price", 19.99m);       // scale=2 → Decimal64(2)
        parameters.AddParameter("rate", 0.123456789m);   // scale=9 → Decimal128(9)

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toTypeName(@price) as price_type, toTypeName(@rate) as rate_type",
            parameters);

        while (reader.Read())
        {
            Console.WriteLine($"   price type: {reader.GetString(0)}");
            Console.WriteLine($"   rate type:  {reader.GetString(1)}");
        }
    }

    /// <summary>
    /// Setting ClickHouseType on a parameter always overrides the resolver.
    /// </summary>
    private static async Task ExplicitTypeOverrideExample()
    {
        Console.WriteLine("\n3. Explicit ClickHouseType overrides the resolver:");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ParameterTypeResolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
            {
                [typeof(int)] = "Int64",  // resolver says Int64
            }),
        };
        using var client = new ClickHouseClient(settings);

        var parameters = new ClickHouseParameterCollection();
        // This parameter has an explicit ClickHouseType, which wins over the resolver
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "val",
            Value = 42,
            ClickHouseType = "UInt8",
        });

        using var reader = await client.ExecuteReaderAsync(
            "SELECT @val as value, toTypeName(@val) as type_name",
            parameters);

        while (reader.Read())
        {
            Console.WriteLine($"   Value: {reader.GetValue(0)}, Type: {reader.GetString(1)}");
        }
    }

    /// <summary>
    /// The resolver works through the ADO.NET ClickHouseConnection path too.
    /// </summary>
    private static async Task AdoNetExample()
    {
        Console.WriteLine("\n4. Works with ClickHouseConnection (ADO.NET):");

        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ParameterTypeResolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
            {
                [typeof(DateTime)] = "DateTime64(6)",
            }),
        };
        using var connection = new ClickHouseConnection(settings);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT toTypeName(@dt) as type_name";
        command.AddParameter("dt", DateTime.UtcNow);

        using var reader = await command.ExecuteReaderAsync();
        while (reader.Read())
        {
            Console.WriteLine($"   DateTime type via ADO.NET: {reader.GetString(0)}");
        }
    }

    /// <summary>
    /// QueryOptions.ParameterTypeResolver overrides the client-level resolver for a single query.
    /// </summary>
    private static async Task PerQueryResolverExample()
    {
        Console.WriteLine("\n5. Per-query resolver via QueryOptions:");

        // Client-level: int → Int64
        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ParameterTypeResolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
            {
                [typeof(int)] = "Int64",
            }),
        };
        using var client = new ClickHouseClient(settings);

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        // Without query options: uses client resolver (Int64)
        using (var reader = await client.ExecuteReaderAsync(
            "SELECT toTypeName(@val) as type_name", parameters))
        {
            while (reader.Read())
                Console.WriteLine($"   Client resolver:  {reader.GetString(0)}");
        }

        // With query options: overrides to UInt16
        var options = new QueryOptions
        {
            ParameterTypeResolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
            {
                [typeof(int)] = "UInt16",
            }),
        };

        using (var reader = await client.ExecuteReaderAsync(
            "SELECT toTypeName(@val) as type_name", parameters, options))
        {
            while (reader.Read())
                Console.WriteLine($"   Query resolver:   {reader.GetString(0)}");
        }
    }

    /// <summary>
    /// A custom resolver that picks the ClickHouse decimal type based on the actual
    /// scale of the decimal value. Small scales use Decimal64, large scales use Decimal128.
    /// </summary>
    private class SmartDecimalResolver : IParameterTypeResolver
    {
        public string ResolveType(Type clrType, object value, string parameterName)
        {
            if (clrType != typeof(decimal))
                return null; // Let other types use default inference

            var scale = (decimal.GetBits((decimal)value)[3] >> 16) & 0x7F;
            // Use Decimal64 for small scales (fits in 64 bits), Decimal128 for larger
            return scale <= 4 ? $"Decimal64({scale})" : $"Decimal128({scale})";
        }
    }
}
