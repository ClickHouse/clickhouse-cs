using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Exposes the caller's rows as an array for compiled gathers: an array is used where it is, and any other list
/// is staged one block at a time through a pooled window.
/// </summary>
/// <remarks>
/// The window is sized for a single wire block rather than the whole insert, because a copy of every row
/// reference costs eight bytes a row — a large-object-heap array of its own on a large insert. The caller must
/// not modify the list while the insert runs, since each block is read as it is written.
/// </remarks>
/// <typeparam name="T">The row type.</typeparam>
internal sealed class PocoRowBuffer<T> : IDisposable
    where T : class
{
    private readonly IReadOnlyList<T> source;
    private readonly string parameterName;
    private readonly CancellationToken cancellationToken;
    private readonly bool pooled;
    private T[] rows;

    private PocoRowBuffer(
        IReadOnlyList<T> source,
        T[] rows,
        int count,
        bool pooled,
        string parameterName,
        CancellationToken cancellationToken)
    {
        this.source = source;
        this.rows = rows;
        this.pooled = pooled;
        this.parameterName = parameterName;
        this.cancellationToken = cancellationToken;
        Count = count;
    }

    /// <summary>The array a gather reads through: the caller's own, or the staging window.</summary>
    public T[] Rows => rows;

    /// <summary>The number of rows to insert.</summary>
    public int Count { get; }

    /// <summary>The public parameter name a bad row is blamed on.</summary>
    public string ParameterName => parameterName;

    /// <summary>Prepares to read the caller's rows a block at a time.</summary>
    /// <param name="source">The caller's materialized rows.</param>
    /// <param name="parameterName">The public parameter name to blame for a null row.</param>
    /// <param name="blockRows">The most rows one block will hold, sizing the staging window.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The prepared buffer.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public static PocoRowBuffer<T> Create(IReadOnlyList<T> source, string parameterName, int blockRows, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        // An array is already what the gather indexes, so read the caller's rows where they are.
        if (source is T[] array)
        {
            return new PocoRowBuffer<T>(source: null, array, array.Length, pooled: false, parameterName, cancellationToken);
        }

        int count = source.Count;
        if (count == 0)
        {
            return new PocoRowBuffer<T>(source: null, Array.Empty<T>(), 0, pooled: false, parameterName, cancellationToken);
        }

        int window = Math.Min(blockRows > 0 ? blockRows : count, count);
        return new PocoRowBuffer<T>(source, ArrayPool<T>.Shared.Rent(window), count, pooled: true, parameterName, cancellationToken);
    }

    /// <summary>
    /// Makes rows <c>[start, start + length)</c> readable through <see cref="Rows"/>, staging them into the
    /// window when the caller's rows are not an array, and validates that none of them is null.
    /// </summary>
    /// <param name="start">The zero-based first row of the block.</param>
    /// <param name="length">The number of rows in the block; at most the window's size.</param>
    /// <returns>The index in <see cref="Rows"/> the block begins at.</returns>
    /// <exception cref="ArgumentException">A row of the block is null.</exception>
    /// <exception cref="OperationCanceledException">The insert's token was cancelled.</exception>
    public int Prepare(int start, int length)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (source is null)
        {
            for (int i = 0; i < length; i++)
            {
                if (rows[start + i] is null)
                {
                    throw NullRow(start + i);
                }
            }

            return start;
        }

        for (int i = 0; i < length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            T row = source[start + i];

            if (row is null)
            {
                throw NullRow(start + i);
            }

            rows[i] = row;
        }

        return 0;
    }

    /// <summary>Reads one row without staging it, for a pass over the whole insert.</summary>
    /// <param name="index">The zero-based row number.</param>
    /// <returns>The row, or null if the caller supplied one.</returns>
    public T RowAt(int index) => source is null ? rows[index] : source[index];

    /// <summary>Releases the staging window. A caller's own array is not touched.</summary>
    public void Dispose()
    {
        if (pooled && rows.Length != 0)
        {
            ArrayPool<T>.Shared.Return(rows, clearArray: true);
        }

        rows = Array.Empty<T>();
    }

    private ArgumentException NullRow(int index)
        => new($"Row {index} is null; every row of an insert must be non-null.", parameterName);
}
