using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class MultiDimArrayHelperTests
{
    // ----- Binary write (WriteMultidimensional) — exact wire-byte assertions -----

    public static IEnumerable<TestCaseData> BinaryWriteExactByteCases()
    {
        yield return new TestCaseData(
            "Array(Int32)",
            (object)new[] { 1, 2, 3 },
            new byte[] { 0x03, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00 })
            .SetName("BinaryWrite_Rank1Array_EmitsLengthAndScalars");
        yield return new TestCaseData(
            "Array(Array(Int32))",
            (object)new int[0, 5],
            new byte[] { 0x00 })
            .SetName("BinaryWrite_Rank2EmptyOuter_EmitsZeroLength");
        yield return new TestCaseData(
            "Array(Array(Int32))",
            (object)new int[3, 0],
            new byte[] { 0x03, 0x00, 0x00, 0x00 })
            .SetName("BinaryWrite_Rank2EmptyInner_EmitsOuterLengthThenZeroes");
    }

    [Test]
    [TestCaseSource(nameof(BinaryWriteExactByteCases))]
    public void BinaryWrite_EmitsExpectedWireBytes(string clickHouseType, object value, byte[] expected)
    {
        Assert.That(WriteToBinary(clickHouseType, value), Is.EqualTo(expected));
    }

    // ----- Binary write: rank-N multidim matches equivalent jagged wire -----

    public static IEnumerable<TestCaseData> MultidimVsJaggedWireCases()
    {
        yield return new TestCaseData(
            "Array(Array(Int32))",
            (object)new int[,] { { 1, 2, 3 }, { 4, 5, 6 } },
            (object)new[] { new[] { 1, 2, 3 }, new[] { 4, 5, 6 } })
            .SetName("BinaryWrite_Rank2Multidim_MatchesEquivalentJaggedWire");
        yield return new TestCaseData(
            "Array(Array(Array(Int32)))",
            (object)new int[2, 2, 2]
            {
                { { 1, 2 }, { 3, 4 } },
                { { 5, 6 }, { 7, 8 } },
            },
            (object)new[]
            {
                new[] { new[] { 1, 2 }, new[] { 3, 4 } },
                new[] { new[] { 5, 6 }, new[] { 7, 8 } },
            })
            .SetName("BinaryWrite_Rank3Multidim_MatchesEquivalentJaggedWire");
    }

    [Test]
    [TestCaseSource(nameof(MultidimVsJaggedWireCases))]
    public void BinaryWrite_MultidimMatchesEquivalentJaggedWire(string clickHouseType, object multidim, object jagged)
    {
        Assert.That(WriteToBinary(clickHouseType, multidim),
            Is.EqualTo(WriteToBinary(clickHouseType, jagged)));
    }

    // HTTP format string-output cases (Rank2/Rank3/EmptyOuter/EmptyInner/StringQuoting) live in
    // TypeMappingTests.HttpParameterFormatterNestedArrayCases — no value in duplicating them
    // here. We only need the multidim-vs-jagged equivalence which TypeMappingTests doesn't cover.

    [Test]
    public void HttpFormat_Rank2MultidimMatchesEquivalentJaggedFormat()
    {
        var multidim = new int[,] { { 10, 20, 30 }, { 40, 50, 60 } };
        var jagged = new[] { new[] { 10, 20, 30 }, new[] { 40, 50, 60 } };
        Assert.That(FormatViaHttp("Array(Array(Int32))", multidim),
            Is.EqualTo(FormatViaHttp("Array(Array(Int32))", jagged)));
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
    public void ToMultidimensional_NullValue_ThrowsInvalidCastException()
    {
        // Top-level null is a type-structure mismatch (no collection at all), not a shape issue.
        Assert.Throws<InvalidCastException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,]>(null!));
    }

    [Test]
    public void ToMultidimensional_Rank1Target_ThrowsArgumentException()
    {
        var ex = Assert.Throws<ArgumentException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[]>(new int[] { 1, 2, 3 }));
        Assert.That(ex!.Message, Does.Contain("rank >= 2"));
        // ParamName must not be set: the failing constraint is the generic type T, not the
        // `jagged` argument, so callers shouldn't see a misleading "jagged" in ex.ParamName.
        Assert.That(ex.ParamName, Is.Null);
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
    public void ToMultidimensional_Rank3RaggedAtInnerDepthInLaterSibling_ThrowsInvalidOperationException()
    {
        // Regression: dims[depth+1] used to be reset from each subtree's first child, so a later
        // outer subtree could silently overwrite the inner length established by an earlier one,
        // bypassing the rectangularity check and silently zero-padding short rows.
        var jagged = new int[][][]
        {
            new int[][] { new[] { 1, 2 }, new[] { 3, 4 } },        // inner rows length 2
            new int[][] { new[] { 5, 6, 7 }, new[] { 8, 9, 10 } }, // inner rows length 3 — ragged
        };
        var ex = Assert.Throws<InvalidOperationException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,,]>(jagged));
        Assert.That(ex!.Message, Does.Contain("rectangular"));
        Assert.That(ex.Message, Does.Contain("T[][]"));
    }

    [Test]
    public void ToMultidimensional_SourceShallowerThanTarget_ThrowsInvalidCastException()
    {
        // Rank-1 source asked for as rank-2 target — the source isn't structurally a 2D
        // collection at all. By the type-vs-shape contract this is a type mismatch
        // (InvalidCastException), not a shape failure, and the message must say so.
        var shallow = new[] { 1, 2, 3 };
        var ex = Assert.Throws<InvalidCastException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,]>(shallow));
        Assert.That(ex!.Message, Does.Contain("shallower"));
        Assert.That(ex.Message, Does.Contain("depth 1"));
    }

    [Test]
    public void ToMultidimensional_SourceDeeperThanTarget_ThrowsInvalidCastException()
    {
        // Rank-3 source, rank-2 target — the source's structural depth doesn't match the
        // target rank, so this is a type mismatch (InvalidCastException) rather than a
        // shape error (which would be ragged rows or null intermediate rows).
        var deep = new List<List<List<int>>>
        {
            new() { new() { 1, 2 }, new() { 3, 4 } },
            new() { new() { 5, 6 }, new() { 7, 8 } },
        };

        var ex = Assert.Throws<InvalidCastException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,]>(deep));
        Assert.That(ex!.Message, Does.Contain("deeper than the target rank"));
        Assert.That(ex.Message, Does.Contain("T[][]"));
    }

    [Test]
    public void ToMultidimensional_SourceDeeperThanTargetAtLaterSiblingOnly_ThrowsInvalidCastException()
    {
        // Mismatched leaf siblings: first element is a scalar, a later one is a list. The
        // per-element guard in CopyJaggedToMultidim must catch this and report the exact
        // offending index — same type-mismatch category as a uniformly deeper source.
        var mixed = new object[]
        {
            new object[] { 1, 2 },
            new object[] { 3, new object[] { 99 } },
        };

        var ex = Assert.Throws<InvalidCastException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,]>(mixed));
        Assert.That(ex!.Message, Does.Contain("deeper than the target rank"));
        Assert.That(ex.Message, Does.Contain("[1,1]"));
    }

    [Test]
    public void ToMultidimensional_WrongScalarLeafType_ThrowsInvalidCastException()
    {
        // Well-shaped jagged value, but the leaf scalars are the wrong runtime type for the
        // target element. Must surface as InvalidCastException (per GetFieldValue<T> contract),
        // not the BCL message, and must point at the first offending index.
        var jagged = new object[]
        {
            new object[] { "a", "b" },
            new object[] { "c", "d" },
        };
        var ex = Assert.Throws<InvalidCastException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,]>(jagged));
        Assert.That(ex!.Message, Does.Contain("[0,0]"));
        Assert.That(ex.Message, Does.Contain("Int32"));
        Assert.That(ex.Message, Does.Contain("String"));
    }

    [Test]
    public void ToMultidimensional_NullLeafIntoValueTypeTarget_ThrowsInvalidCastException()
    {
        // Contrast with NullableElementType_PreservesNullElements: a nullable element type
        // accepts null leaves, but a non-nullable value-type target must reject them with a
        // typed exception that pinpoints the offending index.
        var jagged = new object[]
        {
            new object[] { 1, null },
            new object[] { 3, 4 },
        };
        var ex = Assert.Throws<InvalidCastException>(
            () => MultiDimArrayHelper.ToMultidimensional<int[,]>(jagged));
        Assert.That(ex!.Message, Does.Contain("[0,1]"));
        Assert.That(ex.Message, Does.Contain("Int32"));
        Assert.That(ex.Message, Does.Contain("null"));
    }

    [Test]
    public void ToMultidimensional_NullLeafIntoReferenceTypeTarget_Succeeds()
    {
        // Regression guard: the new leaf-assignment wrap must not over-reject legitimate null
        // leaves when the target element type is a reference type.
        var jagged = new object[]
        {
            new object[] { "a", null },
            new object[] { "c", "d" },
        };
        var result = MultiDimArrayHelper.ToMultidimensional<string[,]>(jagged);
        Assert.That(result[0, 0], Is.EqualTo("a"));
        Assert.That(result[0, 1], Is.Null);
        Assert.That(result[1, 1], Is.EqualTo("d"));
    }

    [Test]
    public void ToMultidimensional_RoundTripViaBinaryWriteAndRead_ReturnsOriginalValues()
    {
        // Multidim → binary write → reader produces jagged → ToMultidimensional → equivalent multidim.
        var original = new int[2, 3] { { 1, 2, 3 }, { 4, 5, 6 } };
        var bytes = WriteToBinary("Array(Array(Int32))", original);
        var type = TypeConverter.ParseClickHouseType("Array(Array(Int32))", TypeSettings.Default);
        using var stream = new MemoryStream(bytes);
        using var reader = new ExtendedBinaryReader(stream);
        var jagged = type.Read(reader);
        var result = MultiDimArrayHelper.ToMultidimensional<int[,]>(jagged);
        Assert.That(result, Is.EqualTo(original));
    }

    // ----- Non-zero-bound arrays (Array.CreateInstance with lowerBounds) -----

    private static byte[] WriteToBinary(string clickHouseType, object value)
    {
        var type = TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        type.Write(writer, value);
        writer.Flush();
        return stream.ToArray();
    }

    private static string FormatViaHttp(string clickHouseType, object value, string parameterName = "p")
    {
        var parameter = new ClickHouseDbParameter { ParameterName = parameterName, Value = value };
        return HttpParameterFormatter.Format(parameter, clickHouseType, TypeSettings.Default);
    }

    // ----- Non-zero-bound arrays (Array.CreateInstance with lowerBounds) -----
    //
    // Each case yields (nonZeroBound, zeroBound) — two arrays with identical shape and values
    // but different lower bounds. The write paths must produce identical output for both.

    public static IEnumerable<TestCaseData> NonZeroBoundCases()
    {
        // Rank 2: 2x3, lower bounds {5, 10}
        var nz2 = Array.CreateInstance(typeof(int), new[] { 2, 3 }, new[] { 5, 10 });
        var z2 = new int[2, 3];
        for (var r = 0; r < 2; r++)
        {
            for (var c = 0; c < 3; c++)
            {
                var v = (r * 3) + c + 1;
                z2[r, c] = v;
                nz2.SetValue(v, new[] { 5 + r, 10 + c });
            }
        }
        yield return new TestCaseData("Array(Array(Int32))", (object)nz2, (object)z2)
            .SetName("NonZeroBound_Rank2_MatchesZeroBound");

        // Rank 3: 2x2x2, lower bounds {100, 200, 300}
        var nz3 = Array.CreateInstance(typeof(int), new[] { 2, 2, 2 }, new[] { 100, 200, 300 });
        var z3 = new int[2, 2, 2];
        var v3 = 0;
        for (var a = 0; a < 2; a++)
        {
            for (var b = 0; b < 2; b++)
            {
                for (var c = 0; c < 2; c++)
                {
                    v3++;
                    z3[a, b, c] = v3;
                    nz3.SetValue(v3, new[] { 100 + a, 200 + b, 300 + c });
                }
            }
        }
        yield return new TestCaseData("Array(Array(Array(Int32)))", (object)nz3, (object)z3)
            .SetName("NonZeroBound_Rank3_MatchesZeroBound");
    }

    [Test]
    [TestCaseSource(nameof(NonZeroBoundCases))]
    public void BinaryWrite_NonZeroBound_ProducesSameBytesAsZeroBound(string clickHouseType, object nonZeroBound, object zeroBound)
    {
        Assert.That(WriteToBinary(clickHouseType, nonZeroBound),
            Is.EqualTo(WriteToBinary(clickHouseType, zeroBound)));
    }

    [Test]
    [TestCaseSource(nameof(NonZeroBoundCases))]
    public void HttpFormat_NonZeroBound_ProducesSameStringAsZeroBound(string clickHouseType, object nonZeroBound, object zeroBound)
    {
        Assert.That(FormatViaHttp(clickHouseType, nonZeroBound),
            Is.EqualTo(FormatViaHttp(clickHouseType, zeroBound)));
    }

    // ----- Rank-vs-ClickHouse-type-depth validation -----
    //
    // The triple (typeName, rank, depth) is enough to drive the test — the array's contents
    // don't matter for the mismatch check. Shape (2,2,...,2) of int gives a uniform witness.

    // Message contract lives on the helper itself; the two facade tests below are smokes
    // that confirm both write paths route through ResolveLeafType.

    [Test]
    [TestCase("Array(Int32)", 2, 1)]
    [TestCase("Array(Array(Array(Int32)))", 2, 3)]
    public void ResolveLeafType_RankMismatch_ThrowsWithDetail(string clickHouseType, int rank, int depth)
    {
        var outer = (ArrayType)TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);
        var ex = Assert.Throws<ArgumentException>(
            () => MultiDimArrayHelper.ResolveLeafType(outer, rank));
        Assert.That(ex!.Message, Does.Contain($"rank {rank}"));
        Assert.That(ex.Message, Does.Contain($"depth {depth}"));
        Assert.That(ex.Message, Does.Contain(clickHouseType));
        // Parameter-name context is owned by the HttpParameterFormatter wrap, not this helper.
        // See HttpFormat_RankMismatch_RoutesThroughResolveLeafType for the wrapped-message contract.
    }

    [Test]
    public void HttpFormat_RankMismatch_RoutesThroughResolveLeafType()
    {
        var ex = Assert.Throws<ArgumentException>(
            () => FormatViaHttp("Array(Int32)", new int[2, 2]));
        // Outer wrap must add parameter-name + outer-type context once (and only once).
        Assert.That(ex!.Message, Does.Contain("'p'"));
        Assert.That(ex.Message, Does.Contain("Array(Int32)"));
        // Inner helper message must still describe the rank/depth mismatch.
        Assert.That(ex.Message, Does.Contain("rank 2"));
        Assert.That(ex.Message, Does.Contain("depth 1"));
    }

    [Test]
    public void BinaryWrite_RankMismatch_RoutesThroughResolveLeafType()
    {
        Assert.Throws<ArgumentException>(
            () => WriteToBinary("Array(Int32)", new int[2, 2]));
    }

    [Test]
    public void HttpFormat_ArrayRankMatchesCHDepth_DoesNotThrow()
    {
        // rank=2, depth=2 — happy path, must format cleanly.
        var value = new int[,] { { 1, 2 }, { 3, 4 } };
        Assert.That(
            FormatViaHttp("Array(Array(Int32))", value),
            Is.EqualTo("[[1,2],[3,4]]"));
    }
}
