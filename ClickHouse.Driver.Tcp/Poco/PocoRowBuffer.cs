using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Exposes materialized rows as an array for compiled gathers, borrowing arrays and pooling copies of other lists.
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
internal sealed class PocoRowBuffer<T> : IDisposable
    where T : class
{
    private T[] rows;
    private readonly bool pooled;

    private PocoRowBuffer(T[] rows, int count, bool pooled)
    {
        this.rows = rows;
        Count = count;
        this.pooled = pooled;
    }

    /// <summary>The rows; the first <see cref="Count"/> entries are the caller's, in order.</summary>
    public T[] Rows => rows;

    /// <summary>The number of rows.</summary>
    public int Count { get; }

    /// <summary>Validates the rows and exposes them as an array.</summary>
    /// <param name="source">The caller's materialized rows.</param>
    /// <param name="parameterName">The public parameter name to blame for a null row.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The prepared buffer.</returns>
    /// <exception cref="ArgumentException">A row is null.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public static PocoRowBuffer<T> Create(IReadOnlyList<T> source, string parameterName, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        if (source is T[] array)
        {
            Validate(array, parameterName, cancellationToken);
            return new PocoRowBuffer<T>(array, array.Length, pooled: false);
        }

        int count = source.Count;
        if (count == 0)
        {
            return new PocoRowBuffer<T>(Array.Empty<T>(), 0, pooled: false);
        }

        T[] buffer = ArrayPool<T>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                T row = source[i];

                if (row is null)
                {
                    throw new ArgumentException($"Row {i} is null; every row of an insert must be non-null.", parameterName);
                }

                buffer[i] = row;
            }
        }
        catch
        {
            ArrayPool<T>.Shared.Return(buffer, clearArray: true);
            throw;
        }

        return new PocoRowBuffer<T>(buffer, count, pooled: true);
    }

    /// <summary>Releases an internally rented array. Caller-owned arrays are not touched.</summary>
    public void Dispose()
    {
        if (pooled && rows.Length != 0)
        {
            ArrayPool<T>.Shared.Return(rows, clearArray: true);
        }

        rows = Array.Empty<T>();
    }

    private static void Validate(T[] source, string parameterName, CancellationToken cancellationToken)
    {
        for (int i = 0; i < source.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (source[i] is null)
            {
                throw new ArgumentException($"Row {i} is null; every row of an insert must be non-null.", parameterName);
            }
        }
    }
}
