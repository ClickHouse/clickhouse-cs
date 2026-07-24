using System;
using System.Collections.Generic;
using System.Threading;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Format;

/// <summary>
/// A decoded block: the columnar unit exchanged for Data, Totals, Extremes, Log, and ProfileEvents. Carries an
/// optional name, the block info, and the columns (each holding <see cref="RowCount"/> values).
///
/// <para>
/// A block <b>borrows</b> its columns' storage, which may be pooled. It is disposed by the reader that produced
/// it — for a streamed query, when the consumer advances to the next block or stops enumerating. A consumer
/// must not read a block's columns after that point; to retain values beyond the block, copy them (for example
/// <c>Values.ToArray()</c>) while iterating.
/// </para>
/// </summary>
public sealed class Block : IDisposable
{
    private string[] columnNames;

    /// <summary>Initializes a new instance of the <see cref="Block"/> class.</summary>
    /// <param name="name">The block name (usually empty for result blocks).</param>
    /// <param name="info">The block info prefix.</param>
    /// <param name="rowCount">The number of rows every column holds.</param>
    /// <param name="columns">The decoded columns, in header order.</param>
    internal Block(string name, BlockInfo info, int rowCount, IReadOnlyList<IColumn> columns)
    {
        Name = name;
        Info = info;
        RowCount = rowCount;
        Columns = columns;
    }

    /// <summary>The block name.</summary>
    public string Name { get; }

    /// <summary>The block info prefix.</summary>
    internal BlockInfo Info { get; }

    /// <summary>The number of rows in the block.</summary>
    public int RowCount { get; }

    /// <summary>The number of columns in the block.</summary>
    public int ColumnCount => Columns.Count;

    /// <summary>The decoded columns, in header order.</summary>
    public IReadOnlyList<IColumn> Columns { get; }

    /// <summary>
    /// The column names, in header order — the same order as <see cref="Columns"/> and the <c>object[]</c> rows
    /// produced by the client's untyped read. Pair this with a row to address values by name. Computed once and
    /// cached; the returned list is owned (safe to retain past the block, unlike the columns themselves).
    /// </summary>
    public IReadOnlyList<string> ColumnNames
    {
        get
        {
            // Fully populate a local, then publish the reference with a release write so a concurrent reader
            // never observes the array before its elements are written. A benign double-compute (two readers
            // racing the first access) yields equivalent arrays, so only the torn-publication needs guarding.
            string[] existing = Volatile.Read(ref columnNames);
            if (existing is not null)
            {
                return existing;
            }

            var names = new string[Columns.Count];
            for (int i = 0; i < names.Length; i++)
            {
                names[i] = Columns[i].Name;
            }

            Volatile.Write(ref columnNames, names);
            return names;
        }
    }

    /// <summary>The column at <paramref name="index"/>.</summary>
    /// <param name="index">The zero-based column index.</param>
    /// <returns>The column at that position.</returns>
    public IColumn this[int index] => Columns[index];

    /// <summary>Releases the columns' storage (returning any pooled buffers). Idempotent.</summary>
    public void Dispose()
    {
        foreach (IColumn column in Columns)
        {
            column.Dispose();
        }
    }
}
