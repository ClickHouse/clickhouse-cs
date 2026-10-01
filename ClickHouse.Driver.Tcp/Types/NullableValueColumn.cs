using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A <c>Nullable(T)</c> column for a value-type inner: it pairs the inner column (which holds a decoded value —
/// possibly a placeholder — for every row) with the wire null-map, and surfaces each row as a
/// <see cref="Nullable{T}"/> that is <see langword="null"/> where the map marks the row null. The inner column's
/// storage is borrowed for this column's lifetime and released on <see cref="Dispose"/>.
///
/// <para>
/// This dense shape (a full inner column plus a null-map) is the wire's own layout, so it is also the zero-copy
/// input for writing: handed back to the <c>Nullable(T)</c> codec it is serialized without rebuilding a values
/// array (see <see cref="Inner"/> / <see cref="NullMap"/>). A row-materialization tier can construct it directly
/// to insert without going through the boxed <c>T?</c> form.
/// </para>
/// </summary>
/// <typeparam name="T">The inner value type (e.g. <see cref="int"/> for <c>Nullable(Int32)</c>).</typeparam>
internal sealed class NullableValueColumn<T> : IColumn<T?>, INullableColumn<T>
    where T : struct
{
    private readonly IColumn<T> inner;
    private readonly int rowCount;
    private readonly bool pooledMap;
    private byte[] nullMap;
    private T?[] cache;

    /// <summary>Initializes a nullable column over an inner value column and its null-map.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The full <c>Nullable(...)</c> type string.</param>
    /// <param name="inner">The inner column holding one decoded value (or placeholder) per row; it sets this column's <see cref="RowCount"/>.</param>
    /// <param name="nullMap">The per-row null-map: a non-zero byte marks the row null. May be longer than the row count.</param>
    /// <param name="pooledMap">Whether <paramref name="nullMap"/> was rented and should be returned on dispose.</param>
    /// <exception cref="ArgumentNullException"><paramref name="inner"/> or <paramref name="nullMap"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="nullMap"/> is shorter than the inner column's row count.</exception>
    public NullableValueColumn(string name, string typeName, IColumn<T> inner, byte[] nullMap, bool pooledMap)
    {
        Name = name;
        TypeName = typeName;
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        this.nullMap = nullMap ?? throw new ArgumentNullException(nameof(nullMap));
        this.pooledMap = pooledMap;

        // The inner column holds one value per row, so it is the single source of truth for the height and no
        // separate count is accepted — the two cannot disagree. The null-map is the one input that still can: it is
        // typically a pooled buffer longer than the row count, so only a short map is an error, and rejecting it
        // here is what keeps NullMap's slice and the indexer in bounds.
        rowCount = inner.RowCount;
        if (nullMap.Length < rowCount)
        {
            throw new ArgumentException(
                $"The null-map for column '{name}' ({typeName}) is {nullMap.Length} bytes, shorter than the inner column's {rowCount} rows.",
                nameof(nullMap));
        }
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => rowCount;

    /// <inheritdoc/>
    public IColumn<T> Inner => inner;

    /// <inheritdoc/>
    public ReadOnlySpan<byte> NullMap => nullMap.AsSpan(0, rowCount);

    /// <summary>
    /// The rows as <see cref="Nullable{T}"/>, materialized once and cached. Prefer the indexer when only some
    /// rows are needed, to avoid building this array.
    /// </summary>
    public ReadOnlySpan<T?> Values
    {
        get
        {
            if (cache is null)
            {
                // Rent rather than allocate: this is a convenience view consumers copy out of, so it only needs
                // to live until Dispose returns it to the pool. Single-consumer per connection, so the lazy fill
                // needs no synchronization. The rented buffer may be longer than rowCount; Values slices to it.
                T?[] decoded = ArrayPool<T?>.Shared.Rent(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    decoded[i] = this[i];
                }

                cache = decoded;
            }

            return cache.AsSpan(0, rowCount);
        }
    }

    /// <inheritdoc/>
    // Index through the RowCount-sliced null-map so an out-of-range row throws rather than reading a stale slot
    // of the pooled buffer, which can be longer than RowCount.
    public T? this[int row] => NullMap[row] != 0 ? null : inner[row];

    /// <inheritdoc/>
    public object GetValue(int row) => NullMap[row] != 0 ? null : inner[row];

    /// <inheritdoc/>
    public void Dispose()
    {
        inner.Dispose();
        if (pooledMap && nullMap.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(nullMap);
        }

        nullMap = Array.Empty<byte>();

        if (cache is not null)
        {
            ArrayPool<T?>.Shared.Return(cache, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T?>());
            cache = null;
        }
    }
}
