using System;
using System.Collections.Generic;
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
}
