using System.Collections.Generic;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.Utility;

[TestFixture]
public class DictionaryExtensionsTests
{
    [Test]
    public void EntriesEqual_BothNull_ReturnsTrue()
    {
        IReadOnlyDictionary<string, string> a = null;
        IReadOnlyDictionary<string, string> b = null;

        Assert.That(a.EntriesEqual(b), Is.True);
    }

    [Test]
    public void EntriesEqual_NullAndEmpty_ReturnsTrueBothDirections()
    {
        IReadOnlyDictionary<string, string> a = null;
        IReadOnlyDictionary<string, string> b = new Dictionary<string, string>();

        Assert.That(a.EntriesEqual(b), Is.True);
        Assert.That(b.EntriesEqual(a), Is.True);
    }

    [Test]
    public void EntriesEqual_NullAndNonEmpty_ReturnsFalseBothDirections()
    {
        IReadOnlyDictionary<string, string> a = null;
        IReadOnlyDictionary<string, string> b = new Dictionary<string, string> { ["x"] = "1" };

        Assert.That(a.EntriesEqual(b), Is.False);
        Assert.That(b.EntriesEqual(a), Is.False);
    }

    [Test]
    public void EntriesEqual_BothEmpty_ReturnsTrue()
    {
        IReadOnlyDictionary<string, string> a = new Dictionary<string, string>();
        IReadOnlyDictionary<string, string> b = new Dictionary<string, string>();

        Assert.That(a.EntriesEqual(b), Is.True);
    }

    [Test]
    public void EntriesEqual_SameContentDifferentInsertionOrder_ReturnsTrue()
    {
        IReadOnlyDictionary<string, string> a = new Dictionary<string, string> { ["x"] = "1", ["y"] = "2" };
        IReadOnlyDictionary<string, string> b = new Dictionary<string, string> { ["y"] = "2", ["x"] = "1" };

        Assert.That(a.EntriesEqual(b), Is.True);
    }

    [Test]
    public void EntriesEqual_SameKeyDifferentValue_ReturnsFalse()
    {
        IReadOnlyDictionary<string, string> a = new Dictionary<string, string> { ["x"] = "1" };
        IReadOnlyDictionary<string, string> b = new Dictionary<string, string> { ["x"] = "2" };

        Assert.That(a.EntriesEqual(b), Is.False);
    }

    [Test]
    public void EntriesEqual_DifferentKeys_ReturnsFalse()
    {
        IReadOnlyDictionary<string, string> a = new Dictionary<string, string> { ["x"] = "1" };
        IReadOnlyDictionary<string, string> b = new Dictionary<string, string> { ["y"] = "1" };

        Assert.That(a.EntriesEqual(b), Is.False);
    }

    [Test]
    public void EntriesEqual_DifferentCounts_ReturnsFalse()
    {
        IReadOnlyDictionary<string, string> a = new Dictionary<string, string> { ["x"] = "1" };
        IReadOnlyDictionary<string, string> b = new Dictionary<string, string> { ["x"] = "1", ["y"] = "2" };

        Assert.That(a.EntriesEqual(b), Is.False);
    }

    [Test]
    public void EntriesEqual_IntValues_UsesValueEquality()
    {
        IReadOnlyDictionary<string, int> a = new Dictionary<string, int> { ["x"] = 1, ["y"] = 2 };
        IReadOnlyDictionary<string, int> b = new Dictionary<string, int> { ["x"] = 1, ["y"] = 2 };

        Assert.That(a.EntriesEqual(b), Is.True);
    }

    [Test]
    public void EntriesEqual_NullValues_ComparedByDefaultComparer()
    {
        IReadOnlyDictionary<string, string> a = new Dictionary<string, string> { ["x"] = null };
        IReadOnlyDictionary<string, string> b = new Dictionary<string, string> { ["x"] = null };

        Assert.That(a.EntriesEqual(b), Is.True);

        IReadOnlyDictionary<string, string> c = new Dictionary<string, string> { ["x"] = "v" };
        Assert.That(a.EntriesEqual(c), Is.False);
    }

    [Test]
    public void EntriesEqual_SameReference_ReturnsTrue()
    {
        IReadOnlyDictionary<string, string> a = new Dictionary<string, string> { ["x"] = "1" };

        Assert.That(a.EntriesEqual(a), Is.True);
    }
}
