using System;
using System.Collections.Generic;
using ClickHouse.Driver.ADO.Parameters;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

public class ParameterTypeResolutionTests
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

        var result = ParameterTypeResolution.ResolveTypeName(parameter, "DateTime64(3)", resolver);

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

        var result = ParameterTypeResolution.ResolveTypeName(parameter, "DateTime64(3)", resolver);

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

        var result = ParameterTypeResolution.ResolveTypeName(parameter, null, resolver);

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

        var result = ParameterTypeResolution.ResolveTypeName(parameter, null, resolver);

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

        var result = ParameterTypeResolution.ResolveTypeName(parameter, null, null);

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

        var result = ParameterTypeResolution.ResolveTypeName(parameter, null, null);

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

        var result = ParameterTypeResolution.ResolveTypeName(parameter, null, resolver);

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

        var result = ParameterTypeResolution.ResolveTypeName(parameter, null, null);

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
        var result = ParameterTypeResolution.ResolveTypeName(parameter, null, resolver);

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

        var result = ParameterTypeResolution.ResolveTypeName(parameter, null, resolver);

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

        var result = ParameterTypeResolution.ResolveTypeName(parameter, null, resolver);

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


}
