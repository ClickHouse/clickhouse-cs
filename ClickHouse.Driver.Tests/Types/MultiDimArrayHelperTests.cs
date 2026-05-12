using System;
using System.Linq;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class MultiDimArrayHelperTests
{
    [Test]
    public void EnumerateOutermostRank_Rank1Array_YieldsElementsDirectly()
    {
        var src = new[] { 1, 2, 3 };
        var slices = MultiDimArrayHelper.EnumerateOutermostRank(src).ToList();
        Assert.That(slices, Is.EqualTo(new object[] { 1, 2, 3 }));
    }

    [Test]
    public void EnumerateOutermostRank_Rank2Array_YieldsRank1Slices()
    {
        var src = new int[,] { { 1, 2, 3 }, { 4, 5, 6 } };
        var slices = MultiDimArrayHelper.EnumerateOutermostRank(src).Cast<int[]>().ToList();
        Assert.That(slices.Count, Is.EqualTo(2));
        Assert.That(slices[0], Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(slices[1], Is.EqualTo(new[] { 4, 5, 6 }));
    }

    [Test]
    public void EnumerateOutermostRank_Rank3Array_YieldsRank2Slices()
    {
        var src = new int[2, 2, 2]
        {
            { { 1, 2 }, { 3, 4 } },
            { { 5, 6 }, { 7, 8 } },
        };
        var slices = MultiDimArrayHelper.EnumerateOutermostRank(src).Cast<Array>().ToList();
        Assert.That(slices.Count, Is.EqualTo(2));
        Assert.That(slices[0].Rank, Is.EqualTo(2));
        Assert.That(slices[0].GetLength(0), Is.EqualTo(2));
        Assert.That(slices[0].GetLength(1), Is.EqualTo(2));
        Assert.That(slices[0].GetValue(0, 0), Is.EqualTo(1));
        Assert.That(slices[0].GetValue(0, 1), Is.EqualTo(2));
        Assert.That(slices[0].GetValue(1, 0), Is.EqualTo(3));
        Assert.That(slices[0].GetValue(1, 1), Is.EqualTo(4));
        Assert.That(slices[1].GetValue(0, 0), Is.EqualTo(5));
        Assert.That(slices[1].GetValue(1, 1), Is.EqualTo(8));
    }

    [Test]
    public void EnumerateOutermostRank_EmptyRank1Array_YieldsNothing()
    {
        var slices = MultiDimArrayHelper.EnumerateOutermostRank(Array.Empty<int>()).ToList();
        Assert.That(slices, Is.Empty);
    }

    [Test]
    public void EnumerateOutermostRank_Rank2ArrayWithZeroOuterDim_YieldsNothing()
    {
        var src = new int[0, 5];
        var slices = MultiDimArrayHelper.EnumerateOutermostRank(src).ToList();
        Assert.That(slices, Is.Empty);
    }

    [Test]
    public void EnumerateOutermostRank_Rank2ArrayWithZeroInnerDim_YieldsEmptyRank1Slices()
    {
        var src = new int[3, 0];
        var slices = MultiDimArrayHelper.EnumerateOutermostRank(src).Cast<Array>().ToList();
        Assert.That(slices.Count, Is.EqualTo(3));
        Assert.That(slices.All(s => s.Length == 0), Is.True);
        Assert.That(slices.All(s => s.GetType() == typeof(int[])), Is.True);
    }

    [Test]
    public void EnumerateOutermostRank_PreservesElementType()
    {
        var src = new string[,] { { "a", "b" } };
        var slices = MultiDimArrayHelper.EnumerateOutermostRank(src).ToList();
        Assert.That(slices.Count, Is.EqualTo(1));
        Assert.That(slices[0], Is.TypeOf<string[]>());
        Assert.That(((string[])slices[0])[0], Is.EqualTo("a"));
    }

    [Test]
    public void EnumerateOutermostRank_Rank2ArrayOfReferenceTypeWithNulls_PreservesNulls()
    {
        var src = new string[1, 2];
        src[0, 0] = "x";
        src[0, 1] = null;
        var slice = (string[])MultiDimArrayHelper.EnumerateOutermostRank(src).Single();
        Assert.That(slice[0], Is.EqualTo("x"));
        Assert.That(slice[1], Is.Null);
    }
}
