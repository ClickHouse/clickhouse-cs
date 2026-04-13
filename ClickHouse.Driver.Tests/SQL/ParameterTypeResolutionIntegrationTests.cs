using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

/// <summary>
/// Integration tests for IParameterTypeResolver exercising both HTTP transport paths:
/// - Query parameters (UseFormDataParameters=false, default)
/// - Form data (UseFormDataParameters=true)
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
public class ParameterTypeResolutionIntegrationTests
{
    private readonly bool useFormDataParameters;

    public ParameterTypeResolutionIntegrationTests(bool useFormDataParameters)
    {
        this.useFormDataParameters = useFormDataParameters;
    }

    private ClickHouseClient CreateClientWithResolver(IParameterTypeResolver resolver)
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: useFormDataParameters);
        settings = new ClickHouseClientSettings(settings) { ParameterTypeResolver = resolver };
        return new ClickHouseClient(settings);
    }

    private ClickHouseClient CreateClientWithMappings(Dictionary<Type, string> mappings)
        => CreateClientWithResolver(new DictionaryParameterTypeResolver(mappings));

    [Test]
    public async Task ExecuteReaderAsync_DictionaryResolverMapsDateTimeToDateTime64_PreservesMilliseconds()
    {
        using var client = CreateClientWithMappings(new Dictionary<Type, string>
        {
            [typeof(DateTime)] = "DateTime64(3)",
        });

        var now = new DateTime(2025, 6, 15, 12, 30, 45, 123, DateTimeKind.Unspecified);
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("dt", now);

        using var reader = await client.ExecuteReaderAsync("SELECT @dt as res", parameters);
        Assert.That(reader.Read(), Is.True);

        var result = reader.GetValue(0);
        Assert.That(result, Is.InstanceOf<DateTime>());
        Assert.That(((DateTime)result).Millisecond, Is.EqualTo(123));
    }

    [Test]
    public async Task ExecuteReaderAsync_DictionaryResolverMapsDecimalToDecimal64_ReportsCorrectType()
    {
        using var client = CreateClientWithMappings(new Dictionary<Type, string>
        {
            [typeof(decimal)] = "Decimal64(6)",
        });

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 123.4567m);

        using var reader = await client.ExecuteReaderAsync("SELECT toTypeName(@val) as type_name", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Decimal(18, 6)"));
    }


    [Test]
    public async Task ExecuteReaderAsync_ExplicitClickHouseType_OverridesResolver()
    {
        using var client = CreateClientWithMappings(new Dictionary<Type, string>
        {
            [typeof(int)] = "Int64",
        });

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "val",
            Value = 42,
            ClickHouseType = "UInt8",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT toTypeName(@val) as type_name", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("UInt8"));
    }

    [Test]
    public async Task ExecuteReaderAsync_SqlTypeHint_OverridesResolver()
    {
        using var client = CreateClientWithMappings(new Dictionary<Type, string>
        {
            [typeof(int)] = "Int64",
        });

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync("SELECT toTypeName({val:UInt16}) as type_name", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("UInt16"));
    }

    [Test]
    public async Task ExecuteReaderAsync_ResolverReturnsNull_FallsToDefaultInference()
    {
        using var client = CreateClientWithMappings(new Dictionary<Type, string>
        {
            [typeof(DateTime)] = "DateTime64(3)", // only maps DateTime, not int
        });

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync("SELECT toTypeName(@val) as type_name", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Int32"));
    }

    [Test]
    public async Task ExecuteReaderAsync_NoResolver_UsesDefaultInference()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: useFormDataParameters);
        using var client = new ClickHouseClient(settings);

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync("SELECT toTypeName(@val) as type_name", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Int32"));
    }


    [Test]
    public async Task ExecuteReaderAsync_CustomResolver_AppliesValueBasedResolution()
    {
        using var client = CreateClientWithResolver(new AlwaysInt64Resolver());

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync("SELECT toTypeName(@val) as type_name", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Int64"));
    }


    [Test]
    public async Task ExecuteReaderAsync_ADO_DictionaryResolver_PreservesMilliseconds()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: useFormDataParameters);
        settings = new ClickHouseClientSettings(settings)
        {
            ParameterTypeResolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
            {
                [typeof(DateTime)] = "DateTime64(3)",
            }),
        };
        using var connection = new ClickHouseConnection(settings);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT @dt as res";
        command.AddParameter("dt", new DateTime(2025, 6, 15, 12, 30, 45, 456, DateTimeKind.Unspecified));

        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        var result = reader.GetDateTime(0);
        Assert.That(result.Millisecond, Is.EqualTo(456));
    }

    [Test]
    public async Task ExecuteReaderAsync_ADO_MultipleParameters_EachResolvedByType()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: useFormDataParameters);
        settings = new ClickHouseClientSettings(settings)
        {
            ParameterTypeResolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
            {
                [typeof(int)] = "Int64",
                [typeof(string)] = "FixedString(10)",
            }),
        };
        using var connection = new ClickHouseConnection(settings);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT toTypeName(@a) as t1, toTypeName(@b) as t2";
        command.AddParameter("a", 42);
        command.AddParameter("b", "hello");

        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Int64"));
        Assert.That(reader.GetString(1), Is.EqualTo("FixedString(10)"));
    }

    [Test]
    public async Task ExecuteReaderAsync_CountingResolver_CalledExactlyOncePerParameter()
    {
        var resolver = new CountingResolver();
        using var client = CreateClientWithResolver(resolver);

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("a", 42);
        parameters.AddParameter("b", 123);

        using var reader = await client.ExecuteReaderAsync("SELECT @a, @b", parameters);
        Assert.That(reader.Read(), Is.True);

        // Resolver should be called exactly once per parameter, not twice
        // (once for SQL placeholder, once for HTTP formatting would be a bug)
        Assert.That(resolver.CallCount, Is.EqualTo(2));
    }

    private class AlwaysInt64Resolver : IParameterTypeResolver
    {
        public string ResolveType(Type clrType, object value, string parameterName)
        {
            if (clrType == typeof(int))
                return "Int64";
            return null;
        }
    }

    [Test]
    public async Task ExecuteReaderAsync_QueryOptionsResolver_OverridesClientResolver()
    {
        // Client maps int → Int64
        using var client = CreateClientWithMappings(new Dictionary<Type, string>
        {
            [typeof(int)] = "Int64",
        });

        // Query options maps int → UInt32, should win
        var options = new QueryOptions
        {
            ParameterTypeResolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
            {
                [typeof(int)] = "UInt32",
            }),
        };

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync("SELECT toTypeName(@val) as type_name", parameters, options);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("UInt32"));
    }

    [Test]
    public async Task ExecuteReaderAsync_QueryOptionsResolverNull_FallsToClientResolver()
    {
        // Client maps int → Int64
        using var client = CreateClientWithMappings(new Dictionary<Type, string>
        {
            [typeof(int)] = "Int64",
        });

        // Query options has no resolver — client resolver should apply
        var options = new QueryOptions();

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync("SELECT toTypeName(@val) as type_name", parameters, options);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Int64"));
    }

    [Test]
    public async Task ExecuteReaderAsync_QueryOptionsResolverWithNoClientResolver_Works()
    {
        // No client-level resolver
        var settings = TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: useFormDataParameters);
        using var client = new ClickHouseClient(settings);

        // Query options provides the resolver
        var options = new QueryOptions
        {
            ParameterTypeResolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
            {
                [typeof(int)] = "Int64",
            }),
        };

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync("SELECT toTypeName(@val) as type_name", parameters, options);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Int64"));
    }

    [Test]
    public async Task ExecuteReaderAsync_ExplicitClickHouseType_WinsOverQueryOptionsResolver()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: useFormDataParameters);
        using var client = new ClickHouseClient(settings);

        var options = new QueryOptions
        {
            ParameterTypeResolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
            {
                [typeof(int)] = "Int64",
            }),
        };

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "val",
            Value = 42,
            ClickHouseType = "UInt8", // explicit type wins over everything
        });

        using var reader = await client.ExecuteReaderAsync("SELECT toTypeName(@val) as type_name", parameters, options);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("UInt8"));
    }

    private class CountingResolver : IParameterTypeResolver
    {
        public int CallCount { get; private set; }

        public string ResolveType(Type clrType, object value, string parameterName)
        {
            CallCount++;
            return clrType == typeof(int) ? "Int64" : null;
        }
    }
}
