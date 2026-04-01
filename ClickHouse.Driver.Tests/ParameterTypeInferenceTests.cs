using System;
using System.Collections.Generic;
using ClickHouse.Driver.ADO.Parameters;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

public class ParameterTypeInferenceTests
{
    [Test]
    public void ResolveTypeName_ExplicitClickHouseType_WinsOverEverything()
    {
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "p",
            Value = DateTime.UtcNow,
            ClickHouseType = "DateTime",
        };
        var resolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
        {
            [typeof(DateTime)] = "DateTime64(6)",
        });

        var result = ParameterTypeInference.ResolveTypeName(parameter, "DateTime64(3)", resolver);

        Assert.That(result, Is.EqualTo("DateTime"));
    }

    [Test]
    public void ResolveTypeName_SqlTypeHint_WinsOverResolverAndDefault()
    {
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "p",
            Value = DateTime.UtcNow,
        };
        var resolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
        {
            [typeof(DateTime)] = "DateTime64(6)",
        });

        var result = ParameterTypeInference.ResolveTypeName(parameter, "DateTime64(3)", resolver);

        Assert.That(result, Is.EqualTo("DateTime64(3)"));
    }

    [Test]
    public void ResolveTypeName_Resolver_WinsOverDefault()
    {
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "p",
            Value = DateTime.UtcNow,
        };
        var resolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
        {
            [typeof(DateTime)] = "DateTime64(6)",
        });

        var result = ParameterTypeInference.ResolveTypeName(parameter, null, resolver);

        Assert.That(result, Is.EqualTo("DateTime64(6)"));
    }

    [Test]
    public void ResolveTypeName_ResolverReturnsNull_FallsThrough()
    {
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "p",
            Value = 42,
        };
        // Resolver maps DateTime but not int
        var resolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
        {
            [typeof(DateTime)] = "DateTime64(6)",
        });

        var result = ParameterTypeInference.ResolveTypeName(parameter, null, resolver);

        Assert.That(result, Is.EqualTo("Int32"));
    }

    [Test]
    public void ResolveTypeName_NullResolver_FallsToDefault()
    {
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "p",
            Value = 42,
        };

        var result = ParameterTypeInference.ResolveTypeName(parameter, null, null);

        Assert.That(result, Is.EqualTo("Int32"));
    }

    [Test]
    public void ResolveTypeName_DecimalSpecialCase_PreservesScale()
    {
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "p",
            Value = 123.456m, // scale = 3
        };

        var result = ParameterTypeInference.ResolveTypeName(parameter, null, null);

        Assert.That(result, Is.EqualTo("Decimal128(3)"));
    }

    [Test]
    public void ResolveTypeName_Resolver_WinsOverDecimalSpecialCase()
    {
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "p",
            Value = 123.456m,
        };
        var resolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
        {
            [typeof(decimal)] = "Decimal64(4)",
        });

        var result = ParameterTypeInference.ResolveTypeName(parameter, null, resolver);

        Assert.That(result, Is.EqualTo("Decimal64(4)"));
    }

    [Test]
    public void ResolveTypeName_NullValue_ReturnsNullableNothing()
    {
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "p",
            Value = null,
        };

        var result = ParameterTypeInference.ResolveTypeName(parameter, null, null);

        Assert.That(result, Is.EqualTo("Nullable(Nothing)"));
    }

    [Test]
    public void ResolveTypeName_DbNullValue_SkipsResolver()
    {
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "p",
            Value = DBNull.Value,
        };
        var resolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
        {
            [typeof(DBNull)] = "String",
        });

        // Resolver should be skipped for DBNull values
        var result = ParameterTypeInference.ResolveTypeName(parameter, null, resolver);

        Assert.That(result, Is.EqualTo("Nullable(Nothing)"));
    }

    [Test]
    public void QueryForm_WithoutResolver_UsesDefaultInference()
    {
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "id",
            Value = 42,
        };

        Assert.That(parameter.QueryForm, Is.EqualTo("{id:Int32}"));
    }

    [Test]
    public void CustomResolver_ValueBasedResolution_Works()
    {
        var resolver = new ScaleAwareDecimalResolver();
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "amount",
            Value = 123.45678m, // scale = 5
        };

        var result = ParameterTypeInference.ResolveTypeName(parameter, null, resolver);

        Assert.That(result, Is.EqualTo("Decimal128(5)"));
    }

    [Test]
    public void CustomResolver_NameBasedResolution_Works()
    {
        var resolver = new NameBasedResolver();
        var parameter = new ClickHouseDbParameter
        {
            ParameterName = "created_at",
            Value = DateTime.UtcNow,
        };

        var result = ParameterTypeInference.ResolveTypeName(parameter, null, resolver);

        Assert.That(result, Is.EqualTo("DateTime64(3)"));
    }

    private class ScaleAwareDecimalResolver : IParameterTypeResolver
    {
        public string ResolveType(Type clrType, object value, string parameterName)
        {
            if (clrType != typeof(decimal))
                return null;

            var scale = (decimal.GetBits((decimal)value)[3] >> 16) & 0x7F;
            return $"Decimal128({scale})";
        }
    }

    private class NameBasedResolver : IParameterTypeResolver
    {
        public string ResolveType(Type clrType, object value, string parameterName)
        {
            if (clrType == typeof(DateTime) && parameterName.Contains("_at"))
                return "DateTime64(3)";
            return null;
        }
    }

    [Test]
    public void ReplacePlaceholders_WithPreResolvedTypes_UsesResolvedTypes()
    {
        var collection = new ClickHouseParameterCollection();
        collection.Add(new ClickHouseDbParameter { ParameterName = "dt", Value = DateTime.UtcNow });
        collection.Add(new ClickHouseDbParameter { ParameterName = "id", Value = 42 });

        var resolvedTypes = new Dictionary<string, string>
        {
            ["dt"] = "DateTime64(3)",
            ["id"] = "Int32",
        };

        var result = collection.ReplacePlaceholders("SELECT @dt, @id", resolvedTypes);

        Assert.That(result, Does.Contain("{dt:DateTime64(3)}"));
        Assert.That(result, Does.Contain("{id:Int32}"));
    }

    [Test]
    public void ReplacePlaceholders_WithDefaultResolvedTypes_MatchesDefaultInference()
    {
        var collection = new ClickHouseParameterCollection();
        collection.Add(new ClickHouseDbParameter { ParameterName = "id", Value = 42 });
        collection.Add(new ClickHouseDbParameter { ParameterName = "name", Value = "test" });

        // Resolve with no resolver (default inference)
        var resolvedTypes = new Dictionary<string, string>();
        foreach (ClickHouseDbParameter p in collection)
            resolvedTypes[p.ParameterName] = ParameterTypeInference.ResolveTypeName(p, null, null);

        var result = collection.ReplacePlaceholders("SELECT @id, @name", resolvedTypes);

        Assert.That(result, Is.EqualTo("SELECT {id:Int32}, {name:String}"));
    }

    [Test]
    public void ResolveTypeName_ResolveOnceFlow_ResolverCalledExactlyOncePerParameter()
    {
        var resolver = new CountingResolver("DateTime64(3)");

        var dtParam = new ClickHouseDbParameter { ParameterName = "dt", Value = DateTime.UtcNow };
        var idParam = new ClickHouseDbParameter { ParameterName = "id", Value = 42 };

        // Resolve once per parameter (simulating ClickHouseClient.ResolveParameterTypes)
        var dtType = ParameterTypeInference.ResolveTypeName(dtParam, null, resolver);
        var idType = ParameterTypeInference.ResolveTypeName(idParam, null, resolver);

        // Resolver was called exactly once per parameter
        Assert.That(resolver.CallCount, Is.EqualTo(2));

        // Reusing the resolved types for placeholder replacement does NOT call the resolver again
        var collection = new ClickHouseParameterCollection { dtParam, idParam };
        var resolvedTypes = new Dictionary<string, string> { ["dt"] = dtType, ["id"] = idType };
        collection.ReplacePlaceholders("SELECT @dt, @id", resolvedTypes);

        Assert.That(resolver.CallCount, Is.EqualTo(2), "Resolver should not be called again during ReplacePlaceholders");
    }

    private class CountingResolver : IParameterTypeResolver
    {
        private readonly string returnValue;
        public int CallCount { get; private set; }

        public CountingResolver(string returnValue) => this.returnValue = returnValue;

        public string ResolveType(Type clrType, object value, string parameterName)
        {
            CallCount++;
            return clrType == typeof(DateTime) ? returnValue : null;
        }
    }

}
