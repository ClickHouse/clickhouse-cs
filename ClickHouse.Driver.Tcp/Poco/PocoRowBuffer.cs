using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The caller's rows, materialized into one array so a row-oriented insert can transpose them into columns.
///
/// <para>
/// Materializing up front is what the native protocol's ordering forces: the target column types arrive in the
/// server's sample block, which only comes back <em>after</em> the INSERT has been sent, and building typed columns
/// needs the row count as well as the types. So the whole source is resident before the first byte of row data goes
/// out — a lazy <see cref="IEnumerable{T}"/> is fully enumerated first, and <c>MaxRowsPerBlock</c> bounds wire
/// geometry, not client memory. Streaming a chunk at a time can be added later without changing the public
/// signature.
/// </para>
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
internal sealed class PocoRowBuffer<T> : IDisposable
    where T : class
{
    private const int InitialCapacity = 16;

    private T[] rows;

    private PocoRowBuffer(T[] rows, int count)
    {
        this.rows = rows;
        Count = count;
    }

    /// <summary>The rows; the first <see cref="Count"/> entries are the caller's, in order.</summary>
    public T[] Rows => rows;

    /// <summary>The number of rows.</summary>
    public int Count { get; }

    /// <summary>
    /// Drains <paramref name="source"/> into a pooled array.
    /// </summary>
    /// <param name="source">The caller's rows.</param>
    /// <param name="parameterName">The public parameter name to blame for a null row.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The buffer; dispose it to return the array to the pool.</returns>
    /// <exception cref="ArgumentException">A row is null.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public static PocoRowBuffer<T> Materialize(IEnumerable<T> source, string parameterName, CancellationToken cancellationToken)
    {
        // Draining the source is the one part of an insert that runs before any I/O, and it can be the long part —
        // a source of millions of rows is enumerated in full here — so the token is observed as it goes rather than
        // only once the connection is rented. Checked before the rent as well as per row, so an already-cancelled
        // call is refused even for an empty source.
        cancellationToken.ThrowIfCancellationRequested();

        // A counted source sizes the rent exactly; anything else grows by doubling.
        int capacity = source.TryGetNonEnumeratedCount(out int counted) ? counted : 0;
        T[] buffer = ArrayPool<T>.Shared.Rent(Math.Max(capacity, InitialCapacity));
        int count = 0;

        try
        {
            foreach (T row in source)
            {
                // Tested every row, not at the growth points: a counted source rents once and never grows, so a long
                // one would otherwise be drained in full whatever the token said. The read is a field test against a
                // token that is usually None, next to a source's own MoveNext — so it costs nothing measurable.
                cancellationToken.ThrowIfCancellationRequested();

                // Checked here rather than in the compiled gather, which would report a bare NullReferenceException
                // from a delegate with no name of its own.
                if (row is null)
                {
                    throw new ArgumentException($"Row {count} is null; every row of an insert must be non-null.", parameterName);
                }

                if (count == buffer.Length)
                {
                    T[] grown = ArrayPool<T>.Shared.Rent(buffer.Length * 2);
                    Array.Copy(buffer, grown, count);
                    ArrayPool<T>.Shared.Return(buffer, clearArray: true);
                    buffer = grown;
                }

                buffer[count++] = row;
            }
        }
        catch
        {
            ArrayPool<T>.Shared.Return(buffer, clearArray: true);
            throw;
        }

        return new PocoRowBuffer<T>(buffer, count);
    }

    /// <summary>Returns the array to the pool. The rows themselves are the caller's and are not touched.</summary>
    public void Dispose()
    {
        if (rows.Length != 0)
        {
            // Cleared, so a pooled array does not keep the caller's rows alive after the insert.
            ArrayPool<T>.Shared.Return(rows, clearArray: true);
        }

        rows = Array.Empty<T>();
    }
}
