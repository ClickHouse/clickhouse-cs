using System;
using System.Collections;
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

    /// <summary>
    /// Converts a jagged value (as produced by <see cref="ArrayType.Read"/> for an
    /// <c>Array(Array(T))</c> column) into a rectangular multidimensional <see cref="Array"/>
    /// of the requested CLR type <typeparamref name="T"/>. The jagged value must be rectangular —
    /// every nested level must be a fixed length at that level — otherwise an
    /// <see cref="InvalidOperationException"/> is thrown.
    /// </summary>
    /// <typeparam name="T">A multidimensional CLR array type, e.g. <c>int[,]</c> or <c>byte[,,]</c>.
    /// The rank and element type must match the structure of <paramref name="jagged"/>.</typeparam>
    public static T ToMultidimensional<T>(object jagged)
    {
        var targetType = typeof(T);
        if (!targetType.IsArray)
            throw new ArgumentException($"Target type '{targetType}' must be an array.", nameof(jagged));
        var rank = targetType.GetArrayRank();
        if (rank < 2)
            throw new ArgumentException(
                $"Target type '{targetType}' must be a multidimensional array (rank >= 2). " +
                $"For jagged results use the standard reader methods.",
                nameof(jagged));
        var elementType = targetType.GetElementType()!;

        if (jagged is null || jagged is DBNull)
            throw new InvalidOperationException(
                $"Cannot materialise '{targetType}' from a null value.");

        var dims = new int[rank];
        MeasureRectangular(jagged, dims, depth: 0, rank: rank);

        var result = Array.CreateInstance(elementType, dims);
        var indices = new int[rank];
        CopyJaggedToMultidim(jagged, result, indices, depth: 0, rank: rank);
        return (T)(object)result;
    }

    private static void MeasureRectangular(object level, int[] dims, int depth, int rank)
    {
        if (level is null)
        {
            throw new InvalidOperationException(
                $"Cannot materialise a rectangular array: null encountered at depth {depth}. " +
                $"Use a jagged target type (e.g. T[][]) to allow null intermediate rows.");
        }

        if (level is not IList list)
        {
            throw new InvalidOperationException(
                $"Cannot materialise a rectangular array: expected a list-like value at depth {depth}, got '{level.GetType()}'.");
        }

        var length = list.Count;
        if (depth == 0)
        {
            dims[0] = length;
        }
        else if (dims[depth] != length)
        {
            throw new InvalidOperationException(
                $"Cannot materialise a rectangular array: row at depth {depth} has length {length}, " +
                $"expected {dims[depth]} to match sibling rows. " +
                $"Use a jagged target type (e.g. T[][]) for ragged data.");
        }

        if (depth + 1 == rank)
        {
            // Innermost row reached; its elements must be scalars. If they're lists, the source
            // value is deeper than the target rank — fail with a clear message instead of letting
            // CopyJaggedToMultidim throw an opaque ArgumentException from Array.SetValue.
            if (length > 0 && list[0] is IList)
            {
                throw new InvalidOperationException(
                    $"Cannot materialise a rectangular array: source is deeper than the target rank " +
                    $"(target rank {rank}, source has a nested list at depth {depth + 1}). " +
                    $"Use a jagged target type (e.g. T[][]) for deeper sources.");
            }
            return;
        }

        for (var i = 0; i < length; i++)
        {
            var child = list[i];
            if (i == 0 && depth + 1 < rank)
            {
                // Set the expected length for siblings at the next depth from the first child.
                if (child is IList firstChild)
                {
                    dims[depth + 1] = firstChild.Count;
                }
                else
                {
                    throw new InvalidOperationException(
                        $"Cannot materialise a rectangular array: expected a nested list at depth {depth + 1}, got '{child?.GetType()?.ToString() ?? "null"}'. " +
                        $"Use a jagged target type (e.g. T[][]) for ragged data.");
                }
            }
            MeasureRectangular(child!, dims, depth + 1, rank);
        }
    }

    private static void CopyJaggedToMultidim(object level, Array dst, int[] indices, int depth, int rank)
    {
        var list = (IList)level;
        if (depth + 1 == rank)
        {
            for (var i = 0; i < list.Count; i++)
            {
                indices[depth] = i;
                dst.SetValue(list[i], indices);
            }
            return;
        }

        for (var i = 0; i < list.Count; i++)
        {
            indices[depth] = i;
            CopyJaggedToMultidim(list[i]!, dst, indices, depth + 1, rank);
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
