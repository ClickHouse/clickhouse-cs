using System;
using System.Collections;
using System.Text;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Helpers for emitting and materialising rectangular multidimensional <see cref="Array"/> values
/// against ClickHouse's jagged <c>Array(Array(T))</c> wire format. Write-side walks are in-place:
/// no intermediate sub-array allocations, only the unavoidable per-leaf box on <see cref="Array.GetValue(int[])"/>.
/// </summary>
internal static class MultiDimArrayHelper
{
    /// <summary>
    /// Walks the <see cref="ArrayType"/> chain of <paramref name="outer"/> and returns the
    /// innermost (non-<see cref="ArrayType"/>) leaf type that sits at exactly <paramref name="rank"/>
    /// levels of nesting. Throws <see cref="ArgumentException"/> if no leaf exists at that depth —
    /// either the chain is shallower or deeper than <paramref name="rank"/>. The message names
    /// both numbers and the outer type; parameter-name context is added by the caller
    /// (<see cref="HttpParameterFormatter"/>) so it isn't duplicated when wrapped.
    /// </summary>
    public static ClickHouseType ResolveLeafType(ArrayType outer, int rank)
    {
        ClickHouseType t = outer;
        var depth = 0;
        while (t is ArrayType at)
        {
            depth++;
            t = at.UnderlyingType;
        }

        if (depth == rank)
            return t;

        var suggestion = rank > depth ? "shallower" : "deeper";
        throw new ArgumentException(
            $"CLR array rank {rank} does not match ClickHouse type '{outer}' " +
            $"(nested array depth {depth}). Provide a {suggestion} array or change the type hint.");
    }

    /// <summary>
    /// Writes a multidimensional <paramref name="array"/> in ClickHouse binary format. Honours
    /// <see cref="Array.GetLowerBound(int)"/> per axis so non-zero-bound arrays serialise
    /// identically to zero-bound ones.
    /// </summary>
    public static void WriteMultidimensional(ExtendedBinaryWriter writer, Array array, ClickHouseType leafType)
    {
        var indices = new int[array.Rank];
        WriteAxis(writer, array, indices, dim: 0, leafType);
    }

    private static void WriteAxis(ExtendedBinaryWriter writer, Array array, int[] indices, int dim, ClickHouseType leafType)
    {
        var length = array.GetLength(dim);
        var lower = array.GetLowerBound(dim);
        writer.Write7BitEncodedInt(length);

        if (dim == array.Rank - 1)
        {
            for (var i = 0; i < length; i++)
            {
                indices[dim] = lower + i;
                leafType.Write(writer, array.GetValue(indices));
            }
            return;
        }

        for (var i = 0; i < length; i++)
        {
            indices[dim] = lower + i;
            WriteAxis(writer, array, indices, dim + 1, leafType);
        }
    }

    /// <summary>
    /// Appends a multidimensional <paramref name="array"/> in ClickHouse HTTP-parameter text
    /// format (<c>[[1,2],[3,4]]</c>) to <paramref name="sb"/>. Honours
    /// <see cref="Array.GetLowerBound(int)"/> per axis. Leaf scalars dispatch directly to
    /// <see cref="HttpParameterFormatter.Format(ClickHouseType, object, bool, IParameterFormatter, string)"/>
    /// — no closure allocation per call.
    /// </summary>
    public static void AppendMultidimensional(
        StringBuilder sb,
        Array array,
        ClickHouseType leafType,
        IParameterFormatter customFormatter,
        string parameterName)
    {
        var indices = new int[array.Rank];
        AppendAxis(sb, array, indices, dim: 0, leafType, customFormatter, parameterName);
    }

    private static void AppendAxis(
        StringBuilder sb,
        Array array,
        int[] indices,
        int dim,
        ClickHouseType leafType,
        IParameterFormatter customFormatter,
        string parameterName)
    {
        sb.Append('[');
        var length = array.GetLength(dim);
        var lower = array.GetLowerBound(dim);

        if (dim == array.Rank - 1)
        {
            for (var i = 0; i < length; i++)
            {
                if (i > 0) sb.Append(',');
                indices[dim] = lower + i;
                sb.Append(HttpParameterFormatter.Format(leafType, array.GetValue(indices), quote: true, customFormatter, parameterName));
            }
        }
        else
        {
            for (var i = 0; i < length; i++)
            {
                if (i > 0) sb.Append(',');
                indices[dim] = lower + i;
                AppendAxis(sb, array, indices, dim + 1, leafType, customFormatter, parameterName);
            }
        }

        sb.Append(']');
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
            throw new ArgumentException($"Target type '{targetType}' must be an array.");
        var rank = targetType.GetArrayRank();
        if (rank < 2)
            throw new ArgumentException(
                $"Target type '{targetType}' must be a multidimensional array (rank >= 2). " +
                $"For jagged results use the standard reader methods.");
        var elementType = targetType.GetElementType()!;

        if (jagged is null || jagged is DBNull)
            throw new InvalidOperationException(
                $"Cannot materialise '{targetType}' from a null value.");

        var dims = new int[rank];
        Array.Fill(dims, -1);
        MeasureRectangular(jagged, dims, depth: 0, rank: rank);

        // Any dimension never visited (because a parent was empty) is unconstrained — collapse
        // the sentinel to 0 so Array.CreateInstance accepts it. Matches the empty-outer contract.
        for (var i = 0; i < rank; i++)
        {
            if (dims[i] == -1) dims[i] = 0;
        }

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
        if (dims[depth] == -1)
        {
            dims[depth] = length;
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
            // Innermost row reached. The "source-deeper-than-target-rank" check lives in
            // CopyJaggedToMultidim so every leaf element is validated and the error message
            // can pinpoint the exact index.
            return;
        }

        for (var i = 0; i < length; i++)
        {
            MeasureRectangular(list[i]!, dims, depth + 1, rank);
        }
    }

    private static void CopyJaggedToMultidim(object level, Array dst, int[] indices, int depth, int rank)
    {
        var list = (IList)level;
        if (depth + 1 == rank)
        {
            var elementType = dst.GetType().GetElementType()!;
            // A non-nullable value-type element silently accepts null via Array.SetValue
            // (the slot is zeroed), which would corrupt data and bypass the contract.
            // Capture once outside the hot loop.
            var rejectNullLeaf = elementType.IsValueType && Nullable.GetUnderlyingType(elementType) is null;

            for (var i = 0; i < list.Count; i++)
            {
                var item = list[i];
                indices[depth] = i;
                if (item is IList)
                {
                    throw new InvalidOperationException(
                        $"Cannot materialise a rectangular array: source is deeper than the target rank " +
                        $"(target rank {rank}, nested list at indices [{string.Join(",", indices)}]). " +
                        $"Use a jagged target type (e.g. T[][]) for deeper sources.");
                }
                if (item is null && rejectNullLeaf)
                {
                    throw new InvalidCastException(
                        $"Cannot materialise a rectangular array: null leaf at indices [{string.Join(",", indices)}] " +
                        $"is not assignable to non-nullable target element type '{elementType}'. " +
                        $"Use a nullable element type (e.g. {elementType}?[,]) to allow null leaves.");
                }
                try
                {
                    dst.SetValue(item, indices);
                }
                // Array.SetValue throws InvalidCastException for incompatible runtime types.
                // ArgumentException is caught defensively; it isn't reachable on supported runtimes
                // given indices.Length == rank, but a wrap keeps the GetFieldValue<T> contract
                // (InvalidCastException for type mismatches, InvalidOperationException for shape)
                // robust to future BCL changes.
                catch (Exception ex) when (ex is InvalidCastException or ArgumentException)
                {
                    throw new InvalidCastException(
                        $"Cannot materialise a rectangular array: leaf at indices [{string.Join(",", indices)}] " +
                        $"of runtime type '{item?.GetType().FullName ?? "null"}' is not assignable to " +
                        $"target element type '{elementType}'.", ex);
                }
            }
            return;
        }

        for (var i = 0; i < list.Count; i++)
        {
            indices[depth] = i;
            CopyJaggedToMultidim(list[i]!, dst, indices, depth + 1, rank);
        }
    }
}
