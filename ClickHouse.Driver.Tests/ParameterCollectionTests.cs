using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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
        var resolvedTypes = collection.ResolveTypeNames(sql, null);
        Assert.That(collection.ReplacePlaceholders(sql, resolvedTypes), Is.EqualTo("SELECT {param1:Int32}, {param2:Int32}, {param3:String}, {param4:Nothing}"));

        collection.RemoveAt("param4");
        collection.RemoveAt(3);
        collection.Remove(param2);
        Assert.That(collection.Count, Is.EqualTo(2));
        collection.Clear();
        Assert.That(collection.Count, Is.EqualTo(0));
    }

    [Test]
    public void NamedLookup_LargeCollection_TracksOrderRenamesAndReplacement()
    {
        var collection = CreateLargeCollection();
        var renamed = new ClickHouseDbParameter { ParameterName = "Original" };
        var duplicate = new ClickHouseDbParameter { ParameterName = "Duplicate" };
        collection.Insert(0, renamed);
        collection.Insert(1, duplicate);

        Assert.That(collection.Contains("Original"), Is.True);
        Assert.That(collection.Contains("original"), Is.False);
        renamed.ParameterName = "Duplicate";

        Assert.That(collection.Contains("Original"), Is.False);
        Assert.That(collection.IndexOf("Duplicate"), Is.EqualTo(0));
        Assert.That(collection["Duplicate"], Is.SameAs(renamed));

        var replacement = new ClickHouseDbParameter { ParameterName = "Replacement" };
        collection["Duplicate"] = replacement;
        Assert.That(collection["Duplicate"], Is.SameAs(duplicate));
        Assert.That(collection["Replacement"], Is.SameAs(replacement));

        collection.Insert(0, duplicate);
        Assert.That(collection.IndexOf("Duplicate"), Is.EqualTo(0));
        collection.Remove(duplicate);
        Assert.That(collection.IndexOf("Duplicate"), Is.EqualTo(1));
        var unnamed = new ClickHouseDbParameter();
        collection.AddRange(new[] { duplicate, unnamed });
        Assert.That(collection.IndexOf((string)null), Is.EqualTo(collection.Count - 1));
        Assert.That(collection.Contains("NowNamed"), Is.False);
        unnamed.ParameterName = "NowNamed";
        Assert.That(collection.IndexOf((string)null), Is.EqualTo(-1));
        Assert.That(collection["NowNamed"], Is.SameAs(unnamed));
        collection.RemoveAt("Duplicate");
        Assert.That(collection.Contains("Duplicate"), Is.False);
        collection.RemoveAt(0);
        Assert.That(collection.Contains("Replacement"), Is.False);
        collection.Clear();
        Assert.That(collection.IndexOf("p63"), Is.EqualTo(-1));
    }

    [Test]
    public void NamedLookup_SharedParameter_ObservesRenamesInBothCollections()
    {
        var first = CreateLargeCollection();
        var second = CreateLargeCollection();
        var shared = new ClickHouseDbParameter { ParameterName = "Original" };
        Assert.That(first.Contains("p63"), Is.True);
        Assert.That(second.Contains("p63"), Is.True);
        first.Add(shared);
        second.Add(shared);

        shared.ParameterName = "New";
        Assert.That(first.Contains("Original"), Is.False);
        Assert.That(second.Contains("Original"), Is.False);
        Assert.That(first["New"], Is.SameAs(shared));
        Assert.That(second["New"], Is.SameAs(shared));

        first.Clear();
        shared.ParameterName = "AfterClear";
        Assert.That(second.IndexOf("New"), Is.EqualTo(-1));
        Assert.That(second["AfterClear"], Is.SameAs(shared));
    }

    [Test]
    public void NamedLookup_OverriddenName_UsesTheCurrentGetterValue()
    {
        var collection = CreateLargeCollection();
        Assert.That(collection.Contains("p63"), Is.True);
        var parameter = new DynamicNameParameter { CurrentName = "Old" };
        collection[0] = parameter;
        Assert.That(collection["Old"], Is.SameAs(parameter));

        parameter.CurrentName = "p63";
        Assert.That(collection.Contains("Old"), Is.False);
        Assert.That(collection.IndexOf("p63"), Is.EqualTo(0));
        Assert.That(collection["p63"], Is.SameAs(parameter));
    }

    [Test]
    public async Task NamedLookup_ConcurrentFirstReaders_FindTheExistingParameter()
    {
        const int count = 16_384;
        var collection = CreateLargeCollection(count);
        var expected = collection[count - 1];
        var name = expected.ParameterName;

        using var start = new Barrier(3);
        var contains = ReadTogether(() => collection.Contains(name));
        var position = ReadTogether(() => collection.IndexOf(name) == count - 1);
        var parameter = ReadTogether(() => ReferenceEquals(collection[name], expected));

        Assert.That(await Task.WhenAll(contains, position, parameter), Is.All.True);

        Task<bool> ReadTogether(Func<bool> lookup) => Task.Factory.StartNew(
            () =>
            {
                start.SignalAndWait();
                return lookup();
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    private static ClickHouseParameterCollection CreateLargeCollection(int count = 64)
    {
        var collection = new ClickHouseParameterCollection();
        for (var i = 0; i < count; i++)
            collection.Add(new ClickHouseDbParameter { ParameterName = $"p{i}" });
        return collection;
    }

    private sealed class DynamicNameParameter : ClickHouseDbParameter
    {
        internal string CurrentName { get; set; }

        public override string ParameterName
        {
            get => CurrentName;
            set => CurrentName = value;
        }
    }

    [Test]
    [TestCase("SELECT 1 AS `x{y`, {dt:DateTime64(3, 'UTC')}")]
    [TestCase("SELECT $$x{y$$, {dt:DateTime64(3, 'UTC')}")]
    public void ResolveTypeNames_QueryContainsBraceThatIsNotATypeHint_UsesHint(string sql)
    {
        var collection = new ClickHouseParameterCollection
        {
            new ClickHouseDbParameter { ParameterName = "dt", Value = new DateTime(2020, 1, 2, 3, 4, 5, 123, DateTimeKind.Utc) },
        };

        var resolvedTypes = collection.ResolveTypeNames(sql, null);

        Assert.That(resolvedTypes["dt"], Is.EqualTo("DateTime64(3, 'UTC')"));
    }
}
