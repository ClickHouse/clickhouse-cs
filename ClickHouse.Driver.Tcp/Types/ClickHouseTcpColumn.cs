using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Builds typed columns for <see cref="IClickHouseTcpOperations.InsertAsync"/>. Name each column as listed in the
/// INSERT statement; the server supplies its ClickHouse type, so built columns have a null <see cref="IColumn.TypeName"/>.
/// </summary>
public static class ClickHouseTcpColumn
{
    /// <summary>
    /// Builds a column over a caller-supplied array, one entry per row. The array is taken over as is, not
    /// copied, so do not modify it until the insert has completed.
    /// </summary>
    /// <typeparam name="T">The CLR type of one row's value.</typeparam>
    /// <param name="name">The target column's name.</param>
    /// <param name="values">The values, in row order; its length is the row count.</param>
    /// <returns>A column ready to insert.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> or <paramref name="values"/> is null.</exception>
    public static IColumn<T> Create<T>(string name, T[] values)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(values);
        return new ArrayColumn<T>(name, typeName: null, values);
    }

    /// <summary>
    /// Builds a column from any sequence, which is enumerated once into an array. A sequence that already is a
    /// <typeparamref name="T"/><c>[]</c> is taken over rather than copied, so the array overload's rule applies
    /// to it too: do not modify it until the insert has completed.
    /// </summary>
    /// <typeparam name="T">The CLR type of one row's value.</typeparam>
    /// <param name="name">The target column's name.</param>
    /// <param name="values">The values, in row order.</param>
    /// <returns>A column ready to insert.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> or <paramref name="values"/> is null.</exception>
    public static IColumn<T> Create<T>(string name, IEnumerable<T> values)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(values);
        return Create(name, values as T[] ?? values.ToArray());
    }

    /// <summary>
    /// Builds an <c>Array(T)</c> from a flat inner column and row offsets without rebuilding the values. Offsets
    /// must start at 0, be nondecreasing, and end at the inner column's row count. The result takes ownership of
    /// <paramref name="inner"/> and retains <paramref name="offsets"/> without copying it.
    /// </summary>
    /// <typeparam name="TElement">The CLR type of one element (not of one row).</typeparam>
    /// <param name="name">The target column's name.</param>
    /// <param name="inner">The flat elements of every row, in row order.</param>
    /// <param name="offsets">The per-row offsets into <paramref name="inner"/>.</param>
    /// <returns>A column ready to insert.</returns>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="offsets"/> is empty, does not start at 0, decreases, or does not end at <paramref name="inner"/>'s row count.</exception>
    public static IArrayColumn<TElement> CreateArray<TElement>(string name, IColumn<TElement> inner, int[] offsets)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(offsets);
        ValidateOffsets(offsets, inner.RowCount, name);

        return new ArrayValueColumn<TElement>(name, typeName: null, inner, offsets, offsets.Length - 1, pooledOffsets: false);
    }

    // Checked here rather than at the first read: a caller's offsets decide which elements each row claims, so a
    // bad one either reads past the elements or silently sends the server different rows than the caller built.
    private static void ValidateOffsets(int[] offsets, int elementCount, string name)
    {
        if (offsets.Length == 0)
        {
            throw new ArgumentException(
                $"The offsets for column '{name}' are empty; they need one entry per row plus the leading 0.",
                nameof(offsets));
        }

        if (offsets[0] != 0)
        {
            throw new ArgumentException(
                $"The offsets for column '{name}' start at {offsets[0].ToString(CultureInfo.InvariantCulture)}; the first entry is the start of row 0 and must be 0.",
                nameof(offsets));
        }

        for (int i = 1; i < offsets.Length; i++)
        {
            if (offsets[i] < offsets[i - 1])
            {
                throw new ArgumentException(
                    $"The offsets for column '{name}' go backwards at row {(i - 1).ToString(CultureInfo.InvariantCulture)} " +
                    $"({offsets[i].ToString(CultureInfo.InvariantCulture)} after {offsets[i - 1].ToString(CultureInfo.InvariantCulture)}); each row ends at or after the one before it.",
                    nameof(offsets));
            }
        }

        int end = offsets[offsets.Length - 1];
        if (end != elementCount)
        {
            throw new ArgumentException(
                $"The offsets for column '{name}' end at {end.ToString(CultureInfo.InvariantCulture)}, but the inner column holds {elementCount.ToString(CultureInfo.InvariantCulture)} elements; the last offset is the total element count.",
                nameof(offsets));
        }
    }
}
