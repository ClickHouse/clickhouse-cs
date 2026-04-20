using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

/// <summary>
/// Integration tests for IParameterFormatter exercising both HTTP transport paths:
/// - Query parameters (UseFormDataParameters=false, default)
/// - Form data (UseFormDataParameters=true)
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
public class ParameterFormatterIntegrationTests
{
    private readonly bool useFormDataParameters;

    public ParameterFormatterIntegrationTests(bool useFormDataParameters)
    {
        this.useFormDataParameters = useFormDataParameters;
    }

    private ClickHouseClient CreateClientWithFormatter(IParameterFormatter formatter)
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: useFormDataParameters);
        settings = new ClickHouseClientSettings(settings) { ParameterFormatter = formatter };
        return new ClickHouseClient(settings);
    }

    [Test]
    public async Task ExecuteReaderAsync_DictionaryFormatter_TransformsScalarValue_RoundTrips()
    {
        // Formatter doubles the int before it goes on the wire — round-trip value proves it ran.
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v * 2).ToString(CultureInfo.InvariantCulture),
            }));

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 21);

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(42));
    }

    [Test]
    public async Task ExecuteReaderAsync_FormatterReturnsNull_FallsThroughToDefault()
    {
        // Formatter only handles DateTime; for other types returns null -> default formatter used.
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(DateTime)] = v => "not-used",
            }));

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(42));
    }

    [Test]
    public async Task ExecuteReaderAsync_NullValue_FormatterIsNotInvoked()
    {
        // If the formatter were invoked on a null value, it would throw.
        using var client = CreateClientWithFormatter(new ThrowingFormatter());

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "val",
            Value = DBNull.Value,
            ClickHouseType = "Nullable(Int32)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.IsDBNull(0), Is.True);
    }

    [Test]
    public async Task ExecuteReaderAsync_NoFormatter_UsesDefaultFormatting()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: useFormDataParameters);
        using var client = new ClickHouseClient(settings);

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(42));
    }

    [Test]
    public async Task ExecuteReaderAsync_FormatterReceivesResolvedTypeName_WhenTypeHintGiven()
    {
        // Verify the formatter is called with the SQL-hint-resolved type name, not the default-inferred one.
        var capturing = new CapturingFormatter();
        using var client = CreateClientWithFormatter(capturing);

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT {val:UInt64} as res", parameters);
        Assert.That(reader.Read(), Is.True);

        Assert.That(capturing.Captured, Has.Count.EqualTo(1));
        Assert.That(capturing.Captured[0].TypeName, Is.EqualTo("UInt64"));
        Assert.That(capturing.Captured[0].ParameterName, Is.EqualTo("val"));
    }

    [Test]
    public async Task ExecuteReaderAsync_FormatterReceivesExplicitClickHouseType_AsTypeName()
    {
        var capturing = new CapturingFormatter();
        using var client = CreateClientWithFormatter(capturing);

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "val",
            Value = 42,
            ClickHouseType = "UInt8",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters);
        Assert.That(reader.Read(), Is.True);

        Assert.That(capturing.Captured, Has.Count.EqualTo(1));
        Assert.That(capturing.Captured[0].TypeName, Is.EqualTo("UInt8"));
    }

    [Test]
    public async Task ExecuteReaderAsync_CountingFormatter_CalledExactlyOncePerParameter()
    {
        var counting = new CountingFormatter();
        using var client = CreateClientWithFormatter(counting);

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("a", 42);
        parameters.AddParameter("b", 123);

        using var reader = await client.ExecuteReaderAsync("SELECT @a, @b", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(counting.CallCount, Is.EqualTo(2));
    }

    [Test]
    public async Task ExecuteReaderAsync_QueryOptionsFormatter_OverridesClientFormatter()
    {
        // Client-level formatter that we don't want to see used
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => throw new InvalidOperationException("client formatter should be overridden"),
            }));

        // Per-query formatter that wins
        var options = new QueryOptions
        {
            ParameterFormatter = new DictionaryParameterFormatter(new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v + 1).ToString(CultureInfo.InvariantCulture),
            }),
        };

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 42);

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters, options);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(43));
    }

    [Test]
    public async Task ExecuteReaderAsync_QueryOptionsFormatterNull_FallsToClientFormatter()
    {
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v + 100).ToString(CultureInfo.InvariantCulture),
            }));

        var options = new QueryOptions();

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 1);

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters, options);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(101));
    }

    [Test]
    public async Task ExecuteReaderAsync_QueryOptionsFormatterWithNoClientFormatter_Works()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: useFormDataParameters);
        using var client = new ClickHouseClient(settings);

        var options = new QueryOptions
        {
            ParameterFormatter = new DictionaryParameterFormatter(new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v * 2).ToString(CultureInfo.InvariantCulture),
            }),
        };

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("val", 21);

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters, options);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(42));
    }

    [Test]
    public async Task ExecuteReaderAsync_ADO_DictionaryFormatter_AppliesToCommand()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: useFormDataParameters);
        settings = new ClickHouseClientSettings(settings)
        {
            ParameterFormatter = new DictionaryParameterFormatter(new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v - 1).ToString(CultureInfo.InvariantCulture),
            }),
        };
        using var connection = new ClickHouseConnection(settings);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT @val as res";
        command.AddParameter("val", 100);

        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(99));
    }

    [Test]
    public async Task ExecuteReaderAsync_FormatterAppliedToArrayElements_RoundTripsCustomFormat()
    {
        // Format each int element as the value plus 1000 — verifies the formatter runs per element.
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v + 1000).ToString(CultureInfo.InvariantCulture),
            }));

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "arr",
            Value = new[] { 1, 2, 3 },
            ClickHouseType = "Array(Int32)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @arr as res", parameters);
        Assert.That(reader.Read(), Is.True);
        var arr = (int[])reader.GetValue(0);
        Assert.That(arr, Is.EqualTo(new[] { 1001, 1002, 1003 }));
    }

    [Test]
    public async Task ExecuteReaderAsync_FormatterAppliedToTupleElements_RoundTripsCustomFormat()
    {
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v * 10).ToString(CultureInfo.InvariantCulture),
            }));

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "tup",
            Value = Tuple.Create(1, 2),
            ClickHouseType = "Tuple(Int32, Int32)",
        });

        using var reader = await client.ExecuteReaderAsync(
            "SELECT tupleElement(@tup, 1) as a, tupleElement(@tup, 2) as b", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(10));
        Assert.That(reader.GetInt32(1), Is.EqualTo(20));
    }

    [Test]
    public async Task ExecuteReaderAsync_FormatterAppliedToMapKeysAndValues_RoundTripsCustomFormat()
    {
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v + 100).ToString(CultureInfo.InvariantCulture),
            }));

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "m",
            Value = new Dictionary<int, int> { [1] = 10, [2] = 20 },
            ClickHouseType = "Map(Int32, Int32)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @m as res", parameters);
        Assert.That(reader.Read(), Is.True);
        var dict = (Dictionary<int, int>)reader.GetValue(0);
        Assert.That(dict, Is.EquivalentTo(new Dictionary<int, int> { [101] = 110, [102] = 120 }));
    }

    [Test]
    public async Task ExecuteReaderAsync_FormatterAppliedToNullableInnerValue_RoundTripsCustomFormat()
    {
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(int)] = v => ((int)v * 2).ToString(CultureInfo.InvariantCulture),
            }));

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "val",
            Value = 21,
            ClickHouseType = "Nullable(Int32)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(42));
    }

    [Test]
    public async Task ExecuteReaderAsync_FormatterAppliedToLowCardinalityInnerValue_InArrayContext_WrappedAsString()
    {
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(string)] = v => ((string)v).ToUpperInvariant(),
            }));

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "arr",
            Value = new[] { "hello", "world" },
            ClickHouseType = "Array(LowCardinality(String))",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @arr as res", parameters);
        Assert.That(reader.Read(), Is.True);
        var arr = (string[])reader.GetValue(0);
        Assert.That(arr, Is.EqualTo(new[] { "HELLO", "WORLD" }));
    }

    [Test]
    public async Task ExecuteReaderAsync_NullableParameter_FormatterInvokedOnceWithInnerTypeName()
    {
        // Nullable is a transparent wrapper — the formatter sees Int32 exactly once,
        // not Nullable(Int32) then Int32.
        var counting = new CountingFormatter();
        using var client = CreateClientWithFormatter(counting);

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "val",
            Value = 42,
            ClickHouseType = "Nullable(Int32)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters);
        Assert.That(reader.Read(), Is.True);

        Assert.That(counting.CallCount, Is.EqualTo(1));
        Assert.That(counting.LastTypeName, Is.EqualTo("Int32"));
    }

    [Test]
    public async Task ExecuteReaderAsync_LowCardinalityParameter_FormatterInvokedOnceWithInnerTypeName()
    {
        var counting = new CountingFormatter();
        using var client = CreateClientWithFormatter(counting);

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "val",
            Value = "hello",
            ClickHouseType = "LowCardinality(String)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters);
        Assert.That(reader.Read(), Is.True);

        Assert.That(counting.CallCount, Is.EqualTo(1));
        Assert.That(counting.LastTypeName, Is.EqualTo("String"));
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    [FromVersion(25, 4)]
    public async Task ExecuteReaderAsync_VariantParameter_FormatterInvokedOnceWithMatchedInnerTypeName()
    {
        var counting = new CountingFormatter();
        using var client = CreateClientWithFormatter(counting);

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "val",
            Value = "hello",
            ClickHouseType = "Variant(Int32, String)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @val as res", parameters);
        Assert.That(reader.Read(), Is.True);

        Assert.That(counting.CallCount, Is.EqualTo(1));
        Assert.That(counting.LastTypeName, Is.EqualTo("String"));
    }

    [Test]
    public async Task ExecuteReaderAsync_FormatterReturnsString_InArrayContext_IsWrappedInSingleQuotes()
    {
        // When formatting a string-like type inside a composite, the driver wraps the
        // formatter output in single quotes without escaping.
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(string)] = v => ((string)v).ToUpperInvariant(),
            }));

        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "arr",
            Value = new[] { "hello", "world" },
            ClickHouseType = "Array(String)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @arr as res", parameters);
        Assert.That(reader.Read(), Is.True);
        var arr = (string[])reader.GetValue(0);
        Assert.That(arr, Is.EqualTo(new[] { "HELLO", "WORLD" }));
    }

    [Test]
    public async Task ExecuteReaderAsync_DictionaryFormatter_ArrayOfDateTime_RoundTrips()
    {
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(DateTime)] = v => ((DateTime)v).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            }));

        var expected = new[]
        {
            new DateTime(2025, 6, 15, 12, 30, 45, DateTimeKind.Unspecified),
            new DateTime(2025, 6, 15, 12, 31, 45, DateTimeKind.Unspecified),
        };
        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "arr",
            Value = expected,
            ClickHouseType = "Array(DateTime)",
        });

        using var reader = await client.ExecuteReaderAsync("SELECT @arr as res", parameters);
        Assert.That(reader.Read(), Is.True);
        var actual = (DateTime[])reader.GetValue(0);
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    [FromVersion(25, 4)]
    public async Task ExecuteReaderAsync_DictionaryFormatter_ArrayOfVariantStringDateTimeUtc_RoundTrips()
    {
        using var client = CreateClientWithFormatter(new DictionaryParameterFormatter(
            new Dictionary<Type, Func<object, string>>
            {
                [typeof(DateTime)] = v => ((DateTime)v).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            }));

        var expected = new object[]
        {
            "hello",
            new DateTime(2025, 6, 15, 12, 30, 45, DateTimeKind.Utc),
        };
        var parameters = new ClickHouseParameterCollection();
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "arr",
            Value = expected,
            ClickHouseType = "Array(Variant(String, DateTime('UTC')))",
        });

        using var reader = await client.ExecuteReaderAsync(
            "SELECT @arr as res, arrayMap(x -> variantType(x), @arr) as kinds",
            parameters);
        Assert.That(reader.Read(), Is.True);

        var actual = (object[])reader.GetValue(0);
        Assert.That(actual[0], Is.EqualTo(expected[0]));
        Assert.That(actual[1], Is.EqualTo(expected[1]));
        Assert.That(((DateTime)actual[1]).Kind, Is.EqualTo(DateTimeKind.Utc));

        var kinds = (string[])reader.GetValue(1);
        Assert.That(kinds, Is.EqualTo(new[] { "String", "DateTime('UTC')" }));
    }

    private class ThrowingFormatter : IParameterFormatter
    {
        public string Format(object value, string typeName, string parameterName)
            => throw new InvalidOperationException("formatter should not have been invoked");
    }

    private class CountingFormatter : IParameterFormatter
    {
        public int CallCount { get; private set; }

        public string LastTypeName { get; private set; }

        public string Format(object value, string typeName, string parameterName)
        {
            CallCount++;
            LastTypeName = typeName;
            return null;
        }
    }

    private class CapturingFormatter : IParameterFormatter
    {
        public List<(object Value, string TypeName, string ParameterName)> Captured { get; } = new();

        public string Format(object value, string typeName, string parameterName)
        {
            Captured.Add((value, typeName, parameterName));
            return null;
        }
    }
}
