using System;
using System.Collections.Generic;
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

    // ----- ToMultidimensional<T> -----

    [Test]
    public void ToMultidimensional_Rank2RectangularJaggedInt_ReturnsMatchingMultidim()
    {
        var jagged = new int[][] { new[] { 1, 2, 3 }, new[] { 4, 5, 6 } };
        var result = MultiDimArrayHelper.ToMultidimensional<int[,]>(jagged);
        Assert.That(result.Rank, Is.EqualTo(2));
        Assert.That(result.GetLength(0), Is.EqualTo(2));
        Assert.That(result.GetLength(1), Is.EqualTo(3));
        Assert.That(result[0, 0], Is.EqualTo(1));
        Assert.That(result[0, 2], Is.EqualTo(3));
        Assert.That(result[1, 1], Is.EqualTo(5));
        Assert.That(result[1, 2], Is.EqualTo(6));
    }

    [Test]
    public void ToMultidimensional_Rank2SingleRow_ReturnsRank2WithOneOuter()
    {
        var jagged = new int[][] { new[] { 7, 8, 9 } };
        var result = MultiDimArrayHelper.ToMultidimensional<int[,]>(jagged);
        Assert.That(result.GetLength(0), Is.EqualTo(1));
        Assert.That(result.GetLength(1), Is.EqualTo(3));
        Assert.That(result[0, 2], Is.EqualTo(9));
    }

    [Test]
    public void ToMultidimensional_Rank2EmptyOuter_ReturnsZeroSizedRank2()
    {
        var jagged = Array.Empty<int[]>();
        var result = MultiDimArrayHelper.ToMultidimensional<int[,]>(jagged);
        Assert.That(result.GetLength(0), Is.EqualTo(0));
        Assert.That(result.GetLength(1), Is.EqualTo(0));
        Assert.That(result.Length, Is.EqualTo(0));
    }

    [Test]
    public void ToMultidimensional_Rank2AllEmptyInner_ReturnsRectangularZeroInner()
    {
        var jagged = new int[][] { new int[0], new int[0], new int[0] };
        var result = MultiDimArrayHelper.ToMultidimensional<int[,]>(jagged);
        Assert.That(result.GetLength(0), Is.EqualTo(3));
        Assert.That(result.GetLength(1), Is.EqualTo(0));
    }

    [Test]
    public void ToMultidimensional_Rank3RectangularJagged_ReturnsMatchingMultidim()
    {
        var jagged = new int[][][]
        {
            new int[][] { new[] { 1, 2 }, new[] { 3, 4 } },
            new int[][] { new[] { 5, 6 }, new[] { 7, 8 } },
        };
        var result = MultiDimArrayHelper.ToMultidimensional<int[,,]>(jagged);
        Assert.That(result.GetLength(0), Is.EqualTo(2));
        Assert.That(result.GetLength(1), Is.EqualTo(2));
        Assert.That(result.GetLength(2), Is.EqualTo(2));
        Assert.That(result[0, 0, 0], Is.EqualTo(1));
        Assert.That(result[0, 1, 1], Is.EqualTo(4));
        Assert.That(result[1, 0, 0], Is.EqualTo(5));
        Assert.That(result[1, 1, 1], Is.EqualTo(8));
    }

    [Test]
    public void ToMultidimensional_StringElementType_ReturnsRank2OfString()
    {
        var jagged = new string[][] { new[] { "a", "b" }, new[] { "c", "d" } };
        var result = MultiDimArrayHelper.ToMultidimensional<string[,]>(jagged);
        Assert.That(result[0, 0], Is.EqualTo("a"));
        Assert.That(result[1, 1], Is.EqualTo("d"));
    }

    [Test]
    public void ToMultidimensional_NullableElementType_PreservesNullElements()
    {
        var jagged = new int?[][] { new int?[] { 1, null }, new int?[] { null, 4 } };
        var result = MultiDimArrayHelper.ToMultidimensional<int?[,]>(jagged);
        Assert.That(result[0, 0], Is.EqualTo(1));
        Assert.That(result[0, 1], Is.Null);
        Assert.That(result[1, 0], Is.Null);
        Assert.That(result[1, 1], Is.EqualTo(4));
    }

    [Test]
    public void ToMultidimensional_RaggedRowLengths_ThrowsInvalidOperationException()
    {
        var jagged = new int[][] { new[] { 1, 2, 3 }, new[] { 4, 5 } };
        var ex = Assert.Throws<InvalidOperationException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,]>(jagged));
        Assert.That(ex!.Message, Does.Contain("rectangular"));
        Assert.That(ex.Message, Does.Contain("length"));
        Assert.That(ex.Message, Does.Contain("T[][]"));
    }

    [Test]
    public void ToMultidimensional_NullInnerRow_ThrowsInvalidOperationException()
    {
        var jagged = new int[][] { new[] { 1, 2 }, null };
        var ex = Assert.Throws<InvalidOperationException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,]>(jagged));
        Assert.That(ex!.Message, Does.Contain("T[][]"));
    }

    [Test]
    public void ToMultidimensional_NullValue_ThrowsInvalidOperationException()
    {
        Assert.Throws<InvalidOperationException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,]>(null!));
    }

    [Test]
    public void ToMultidimensional_Rank1Target_ThrowsArgumentException()
    {
        var ex = Assert.Throws<ArgumentException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[]>(new int[] { 1, 2, 3 }));
        Assert.That(ex!.Message, Does.Contain("rank >= 2"));
    }

    [Test]
    public void ToMultidimensional_NonArrayTarget_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(
            () => MultiDimArrayHelper.ToMultidimensional<string>("not an array"));
    }

    [Test]
    public void ToMultidimensional_Rank3RaggedAtMiddleDepth_ThrowsInvalidOperationException()
    {
        var jagged = new int[][][]
        {
            new int[][] { new[] { 1, 2 }, new[] { 3, 4 } },
            new int[][] { new[] { 5, 6 } }, // outer row 2 has 1 sub-row instead of 2
        };
        var ex = Assert.Throws<InvalidOperationException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,,]>(jagged));
        Assert.That(ex!.Message, Does.Contain("T[][]"));
    }

    [Test]
    public void ToMultidimensional_SourceDeeperThanTarget_ThrowsInvalidOperationException()
    {
        // Rank-3 source, rank-2 target — measurement must catch this rather than letting
        // CopyJaggedToMultidim throw an opaque ArgumentException from Array.SetValue.
        var deep = new List<List<List<int>>>
        {
            new() { new() { 1, 2 }, new() { 3, 4 } },
            new() { new() { 5, 6 }, new() { 7, 8 } },
        };

        var ex = Assert.Throws<InvalidOperationException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,]>(deep));
        Assert.That(ex!.Message, Does.Contain("deeper than the target rank"));
        Assert.That(ex.Message, Does.Contain("T[][]"));
    }

    [Test]
    public void ToMultidimensional_RoundTripWithEnumerateOutermostRank_ReturnsOriginalValues()
    {
        // Round-trip: multidim → outermost-rank slices → jagged-shaped List → multidim again.
        var original = new int[2, 3] { { 1, 2, 3 }, { 4, 5, 6 } };
        var jagged = MultiDimArrayHelper.EnumerateOutermostRank(original)
            .Cast<int[]>()
            .ToArray();
        var result = MultiDimArrayHelper.ToMultidimensional<int[,]>(jagged);
        Assert.That(result, Is.EqualTo(original));
    }
}
