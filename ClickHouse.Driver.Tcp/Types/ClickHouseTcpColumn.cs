using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Builds the columns an <see cref="IClickHouseTcpOperations.InsertAsync"/> takes, one call per target column.
/// This is the columnar insert tier: you hand over data already grouped by column, so nothing is transposed and
/// nothing is boxed. To insert rows instead, use <c>InsertRowsAsync</c>.
/// </summary>
/// <remarks>
/// <para>
/// Give each column the <b>target column's name</b>: an insert matches by name, not position, so the order is
/// free. You do not state the ClickHouse type — the server sends the target's schema before any row data and
/// that is what the values are serialized as, so a column built here reports a null
/// <see cref="IColumn.TypeName"/>. If <typeparamref name="T"/> is not a CLR type the target column accepts, the
/// insert fails with an <see cref="ArgumentException"/> naming the target's ClickHouse type, the CLR element
/// type it was given, and the element types it does accept.
/// </para>
/// <para>
/// <b>The statement's column list decides which columns are inserted, not the columns you build.</b> You must
/// supply one column for every name the statement lists, and no others: <c>INSERT INTO t (id, name) VALUES</c>
/// takes exactly <c>id</c> and <c>name</c>, and the server fills <c>t</c>'s remaining columns from their
/// defaults. Listing three names and passing two is an <see cref="ArgumentException"/> naming what is missing —
/// so a subset is something you write into the statement, not something you get by leaving a column out. A
/// statement with no list at all targets every column of the table.
/// </para>
/// <para>
/// Pick <typeparamref name="T"/> to match the target: <c>Int32</c> takes an <c>int</c>, <c>String</c> a
/// <c>string</c>, <c>Array(UInt32)</c> a <c>uint[]</c> per row, <c>Nullable(Int32)</c> an <c>int?</c>. A row of
/// an <c>Array(T)</c> may not be null — use <see cref="Array.Empty{T}"/> for an empty row, or make the target
/// <c>Array(Nullable(T))</c> to carry null elements.
/// </para>
/// <para>
/// A column read out of a <see cref="Block"/> can be passed straight back to an insert without going through
/// this factory, and re-inserts without being rebuilt.
/// </para>
/// </remarks>
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
    /// Builds an <c>Array(T)</c> column in the layout the wire uses: every row's elements concatenated end-to-end
    /// in one column, plus the per-row offsets into it. This is the shape a read produces (see
    /// <see cref="IArrayColumn"/>), and the shape the insert writes with no rebuilding — the alternative,
    /// <c>Create</c> with a <typeparamref name="TElement"/><c>[]</c> per row, costs an array per row and a copy of
    /// every element.
    ///
    /// <para>
    /// Row <c>i</c> holds the elements of <paramref name="inner"/> from <c>offsets[i]</c> (inclusive) to
    /// <c>offsets[i + 1]</c> (exclusive), so <paramref name="offsets"/> starts at <c>0</c>, never decreases, ends
    /// at <paramref name="inner"/>'s row count, and has one more entry than the column has rows. An empty row is
    /// two equal offsets. Both the inner column and the offsets array are taken over as is, not copied: do not
    /// modify them until the insert has completed, and note that disposing the column disposes
    /// <paramref name="inner"/>.
    /// </para>
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
