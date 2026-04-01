using System.Collections.Generic;
using ClickHouse.Driver.ADO.Parameters;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

public class ParameterCollectionTests
{
    [Test]
    public void TestParameterCollectionOperations()
    {
        var param1 = new ClickHouseDbParameter() { ParameterName = "param1", ClickHouseType = "Int32", Value = 1 };
        var param2 = new ClickHouseDbParameter() { ParameterName = "param2", ClickHouseType = "Int32", Value = 2 };
        var param3 = new ClickHouseDbParameter() { ParameterName = "param3", ClickHouseType = "String", Value = "ASD" };
        var param4 = new ClickHouseDbParameter() { ParameterName = "param4", ClickHouseType = "Nothing", Value = null };

        var collection = new ClickHouseParameterCollection
        {
            param1
        };
        collection.AddRange(new[] { param2, param3 });

        Assert.That(collection, Is.All.Not.Null);
        Assert.That(collection, Is.Unique);
        Assert.That(collection.Count, Is.EqualTo(3));
        ClassicAssert.IsTrue(collection.Contains(param2));
        ClassicAssert.IsTrue(collection.Contains("param3"));
        collection.CopyTo(new object[collection.Count], 0);
        Assert.Multiple(() =>
        {
            Assert.That(collection.IndexOf(param1), Is.EqualTo(0));
            Assert.That(collection.IndexOf("param3"), Is.EqualTo(2));
        });

        collection["param4"] = param4;
        collection.Insert(3, param2);
        Assert.Multiple(() =>
        {
            Assert.That(collection[3], Is.EqualTo(param2));
            Assert.That(collection["param4"], Is.EqualTo(param4));
        });

        var sql = "SELECT @param1, @param2, @param3, @param4";
        var resolvedTypes = new Dictionary<string, string>();
        foreach (ClickHouseDbParameter p in collection)
            resolvedTypes[p.ParameterName] = ParameterTypeInference.ResolveTypeName(p, null, null);
        Assert.That(collection.ReplacePlaceholders(sql, resolvedTypes), Is.EqualTo("SELECT {param1:Int32}, {param2:Int32}, {param3:String}, {param4:Nothing}"));

        collection.RemoveAt("param4");
        collection.RemoveAt(3);
        collection.Remove(param2);
        Assert.That(collection.Count, Is.EqualTo(2));
        collection.Clear();
        Assert.That(collection.Count, Is.EqualTo(0));
    }
}
