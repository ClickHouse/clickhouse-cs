using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Helpers for emitting a rectangular multidimensional <see cref="Array"/> as a sequence of jagged
/// slices. ClickHouse's wire format for <c>Array(Array(T))</c> is structurally jagged, so a CLR
/// <c>T[,]</c> passed by the user is sliced along its outermost rank into <c>T[,...]</c> values of
/// rank N-1 before being serialised one row at a time.
/// </summary>
internal static class MultiDimArrayHelper
{
    /// <summary>
    /// Enumerates the outermost-rank slices of <paramref name="array"/>.
    /// For a rank-1 array each element is yielded directly. For higher ranks, each yielded value
    /// is a freshly-allocated <see cref="Array"/> of rank N-1 containing the corresponding row.
    /// </summary>
    public static IEnumerable<object> EnumerateOutermostRank(Array array)
    {
        if (array.Rank == 1)
        {
            foreach (var item in array)
            {
                yield return item;
            }
            yield break;
        }

        var outerLength = array.GetLength(0);
        var innerLengths = new int[array.Rank - 1];
        for (var r = 1; r < array.Rank; r++)
        {
            innerLengths[r - 1] = array.GetLength(r);
        }

        var elementType = array.GetType().GetElementType();
        var srcIndices = new int[array.Rank];
        var dstIndices = new int[innerLengths.Length];

        for (var i = 0; i < outerLength; i++)
        {
            var slice = Array.CreateInstance(elementType, innerLengths);
            srcIndices[0] = i;
            CopySlice(array, srcIndices, 1, slice, dstIndices, 0);
            yield return slice;
        }
    }

    private static void CopySlice(
        Array src,
        int[] srcIndices,
        int srcDim,
        Array dst,
        int[] dstIndices,
        int dstDim)
    {
        if (srcDim == src.Rank)
        {
            dst.SetValue(src.GetValue(srcIndices), dstIndices);
            return;
        }

        var length = src.GetLength(srcDim);
        for (var k = 0; k < length; k++)
        {
            srcIndices[srcDim] = k;
            dstIndices[dstDim] = k;
            CopySlice(src, srcIndices, srcDim + 1, dst, dstIndices, dstDim + 1);
        }
    }
}
