using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The columns of one row-oriented insert: a gather buffer per target column, rented for the insert and refilled
/// for each wire block. Peak memory is therefore one block of values wide, not the whole insert.
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
internal class PocoInsertSource<T> : IInsertColumnSource
    where T : class
{
    private readonly PocoColumnBuilder<T>[] builders;
    private readonly PocoRowBuffer<T> rows;
    private readonly IColumn[] columns;
    private readonly int blockRows;

    /// <summary>Rents one gather buffer per target column.</summary>
    /// <param name="builders">The compiled gathers, one per target column in schema order.</param>
    /// <param name="rows">The insert's rows; not owned, and outlives this source.</param>
    /// <param name="blockRows">The most rows one block will hold.</param>
    public PocoInsertSource(PocoColumnBuilder<T>[] builders, PocoRowBuffer<T> rows, int blockRows)
    {
        this.builders = builders;
        this.rows = rows;
        this.blockRows = blockRows;

        columns = new IColumn[builders.Length];
        int rented = 0;
        try
        {
            for (; rented < builders.Length; rented++)
            {
                columns[rented] = builders[rented].CreateColumn(blockRows);
            }
        }
        catch
        {
            // Release the buffers rented before a later column failed.
            for (int i = 0; i < rented; i++)
            {
                columns[i].Dispose();
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public IReadOnlyList<IColumn> Columns => columns;

    /// <inheritdoc/>
    public void Gather(int start, int length)
    {
        if (length > blockRows)
        {
            throw new ArgumentOutOfRangeException(
                nameof(length), length, $"The gather buffers hold {blockRows} row(s), so a longer block cannot be gathered.");
        }

        int offset = rows.Prepare(start, length);
        CheckBlock(rows.Rows, offset, start, length);

        for (int i = 0; i < builders.Length; i++)
        {
            builders[i].Gather(columns[i], rows.Rows, offset, start, length);
        }
    }

    /// <summary>Checks a block's rows before any column reads them. Does nothing unless a row shape needs it.</summary>
    /// <param name="window">The array the block's rows are in.</param>
    /// <param name="offset">The index in <paramref name="window"/> the block begins at.</param>
    /// <param name="rowNumber">The insert row number of that first row, for error messages.</param>
    /// <param name="count">The number of rows in the block.</param>
    protected virtual void CheckBlock(T[] window, int offset, int rowNumber, int count)
    {
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        for (int i = 0; i < columns.Length; i++)
        {
            columns[i]?.Dispose();
        }
    }
}
