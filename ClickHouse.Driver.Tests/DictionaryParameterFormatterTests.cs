using System;
using System.Collections.Generic;
using ClickHouse.Driver.ADO.Parameters;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

public class DictionaryParameterFormatterTests
{
    [Test]
    public void Constructor_NullMappings_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new DictionaryParameterFormatter(null));
    }

    [Test]
    public void Constructor_NullFormatterFunction_ThrowsArgumentException()
    {
        var mappings = new Dictionary<Type, Func<object, string>>
        {
            [typeof(DateTime)] = null,
        };
        Assert.Throws<ArgumentException>(() => new DictionaryParameterFormatter(mappings));
    }

    [Test]
    public void Format_MappedType_ReturnsFunctionOutput()
    {
        var formatter = new DictionaryParameterFormatter(new Dictionary<Type, Func<object, string>>
        {
            [typeof(int)] = v => $"custom-{v}",
        });

        var result = formatter.Format(42, "Int32", "id");

        Assert.That(result, Is.EqualTo("custom-42"));
    }

    [Test]
    public void Format_UnmappedType_ReturnsNull()
    {
        var formatter = new DictionaryParameterFormatter(new Dictionary<Type, Func<object, string>>
        {
            [typeof(DateTime)] = v => "never-used",
        });

        var result = formatter.Format(42, "Int32", "id");

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Format_MutatedOriginalDictionary_ReturnsOriginalMapping()
    {
        var dict = new Dictionary<Type, Func<object, string>>
        {
            [typeof(int)] = v => "first",
        };
        var formatter = new DictionaryParameterFormatter(dict);

        dict[typeof(int)] = v => "second";

        Assert.That(formatter.Format(42, "Int32", "id"), Is.EqualTo("first"));
    }
}
