using System;
using System.Collections.Generic;
using ClickHouse.Driver.ADO.Parameters;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

public class DictionaryParameterTypeResolverTests
{
    [Test]
    public void Constructor_NullMappings_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new DictionaryParameterTypeResolver(null));
    }

    [Test]
    public void Constructor_ValidMappings_DoesNotThrow()
    {
        Assert.DoesNotThrow(() => new DictionaryParameterTypeResolver(
            new Dictionary<Type, string>
            {
                [typeof(DateTime)] = "DateTime64(3)",
                [typeof(decimal)] = "Decimal64(4)",
                [typeof(string)] = "FixedString(100)",
            }));
    }

    [Test]
    public void ResolveType_MappedType_ReturnsMappedClickHouseType()
    {
        var resolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
        {
            [typeof(DateTime)] = "DateTime64(3)",
        });

        var result = resolver.ResolveType(typeof(DateTime), DateTime.UtcNow, "dt");

        Assert.That(result, Is.EqualTo("DateTime64(3)"));
    }

    [Test]
    public void ResolveType_UnmappedType_ReturnsNull()
    {
        var resolver = new DictionaryParameterTypeResolver(new Dictionary<Type, string>
        {
            [typeof(DateTime)] = "DateTime64(3)",
        });

        var result = resolver.ResolveType(typeof(int), 42, "id");

        Assert.That(result, Is.Null);
    }

    [Test]
    public void ResolveType_MutatedOriginalDictionary_ReturnsOriginalMapping()
    {
        var dict = new Dictionary<Type, string>
        {
            [typeof(DateTime)] = "DateTime64(3)",
        };
        var resolver = new DictionaryParameterTypeResolver(dict);

        dict[typeof(DateTime)] = "DateTime64(6)";

        Assert.That(resolver.ResolveType(typeof(DateTime), DateTime.UtcNow, "dt"), Is.EqualTo("DateTime64(3)"));
    }
}
