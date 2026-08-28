using System;
using System.Collections.Generic;
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
/// free and naming a subset of the table inserts only those columns, the server filling the rest from their
/// defaults. You do not state the ClickHouse type — the server sends the target's schema before any row data and
/// that is what the values are serialized as, so a column built here reports a null
/// <see cref="IColumn.TypeName"/>. If <typeparamref name="T"/> is not a CLR type the target column accepts,
/// the insert fails with an <see cref="ArgumentException"/> naming both.
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
}
