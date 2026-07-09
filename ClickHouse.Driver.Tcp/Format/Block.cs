using System;
using System.Collections.Generic;
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
internal sealed class Block : IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="Block"/> class.</summary>
    /// <param name="name">The block name (usually empty for result blocks).</param>
    /// <param name="info">The block info prefix.</param>
    /// <param name="rowCount">The number of rows every column holds.</param>
    /// <param name="columns">The decoded columns, in header order.</param>
    public Block(string name, BlockInfo info, int rowCount, IReadOnlyList<IColumn> columns)
    {
        Name = name;
        Info = info;
        RowCount = rowCount;
        Columns = columns;
    }

    /// <summary>The block name.</summary>
    public string Name { get; }

    /// <summary>The block info prefix.</summary>
    public BlockInfo Info { get; }

    /// <summary>The number of rows in the block.</summary>
    public int RowCount { get; }

    /// <summary>The number of columns in the block.</summary>
    public int ColumnCount => Columns.Count;

    /// <summary>The decoded columns, in header order.</summary>
    public IReadOnlyList<IColumn> Columns { get; }

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
