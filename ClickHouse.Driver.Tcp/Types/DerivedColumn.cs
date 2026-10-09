using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The view that <see cref="Block.ReadAs{T}(string)"/> gives for a column that is not already an
/// <see cref="IColumn{T}"/> (decision D1). On the first access to <see cref="Values"/> or the indexer, it converts the
/// whole column one time, with one bulk read of a derived reader, into a pooled array. Both members then read that
/// array. The block that owns the view gives the array back to the pool when it is disposed.
/// </summary>
/// <typeparam name="T">The CLR type of one value.</typeparam>
internal sealed class DerivedColumn<T> : IColumn<T>, IDerivedColumn
{
    private readonly IColumn source;
    private readonly BoundReader<T> reader;
    private readonly object gate = new();
    private T[] values;
    private bool released;

    /// <summary>Initializes a view over <paramref name="source"/>.</summary>
    /// <param name="source">The decoded column, which stays owned by its block.</param>
    /// <param name="reader">The derived reader, bound to <paramref name="source"/>.</param>
    public DerivedColumn(IColumn source, BoundReader<T> reader)
    {
        this.source = source ?? throw new ArgumentNullException(nameof(source));
        this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
    }

    /// <inheritdoc/>
    public string Name => source.Name;

    /// <inheritdoc/>
    public string TypeName => source.TypeName;

    /// <inheritdoc/>
    public int RowCount => source.RowCount;

    /// <summary>The converted values. The first access converts the whole column.</summary>
    /// <exception cref="ObjectDisposedException">The block of the column is disposed.</exception>
    public ReadOnlySpan<T> Values => Converted().AsSpan(0, source.RowCount);

    /// <inheritdoc/>
    public T this[int row] => Values[row];

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>Does nothing: the block that owns the view releases it (see <see cref="Release"/>).</summary>
    public void Dispose()
    {
    }

    /// <inheritdoc/>
    public void Release()
    {
        lock (gate)
        {
            if (values is { Length: > 0 })
            {
                ArrayPool<T>.Shared.Return(values, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
            }

            values = null;
            released = true;
        }
    }

    // The lock makes the conversion run one time when threads access the view for the first time together.
    private T[] Converted()
    {
        T[] current = Volatile.Read(ref values);
        if (current is not null)
        {
            return current;
        }

        lock (gate)
        {
            if (values is not null)
            {
                return values;
            }

            if (released)
            {
                throw new ObjectDisposedException(
                    nameof(Block),
                    $"Column '{source.Name}' was read as {typeof(T)} from a block that is disposed. Read the values while the block is alive.");
            }

            int rows = source.RowCount;
            T[] converted = rows == 0 ? Array.Empty<T>() : ArrayPool<T>.Shared.Rent(rows);
            try
            {
                reader.Fill(0, converted.AsSpan(0, rows));
            }
            catch (Exception e)
            {
                if (rows > 0)
                {
                    ArrayPool<T>.Shared.Return(converted, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
                }

                if (e is NullValueException nullValue)
                {
                    throw DerivedColumn.NullFailure(source, typeof(T), nullValue.Row, nullValue);
                }

                throw;
            }

            Volatile.Write(ref values, converted);
            return converted;
        }
    }
}

/// <summary>A view of <see cref="DerivedColumn{T}"/>, which its block releases.</summary>
internal interface IDerivedColumn
{
    /// <summary>Gives the converted values back to the pool. A later access throws <see cref="ObjectDisposedException"/>.</summary>
    void Release();
}

/// <summary>The messages of <see cref="DerivedColumn{T}"/>.</summary>
internal static class DerivedColumn
{
    /// <summary>The failure of a NULL row in a column that is read as a type that cannot hold NULL.</summary>
    /// <param name="column">The column.</param>
    /// <param name="target">The CLR type that the column is read as.</param>
    /// <param name="row">The zero-based row of the column that has NULL.</param>
    /// <param name="inner">The exception of the reader, or null.</param>
    /// <returns>The exception to throw.</returns>
    public static InvalidOperationException NullFailure(IColumn column, Type target, int row, Exception inner = null)
        => new(
            $"Column '{column.Name}' ({column.TypeName}) has NULL at row {row}, and the target type {target} cannot hold NULL. " +
            "Read the column as a nullable type, or remove the NULL values in the query.",
            inner);
}
