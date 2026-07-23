using System;
using System.Buffers;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A ClickHouse <c>Array(T)</c> column: it pairs a flat inner column holding every element of every row
/// end-to-end with a per-row offsets array, and surfaces each row as a freshly materialized
/// <typeparamref name="TElement"/>[]. This is the dense shape the wire uses (offsets + a single concatenated
/// values stream), so it is also the zero-copy source for writing — handed back to the <c>Array(T)</c> codec it
/// serializes straight from the inner column and offsets without rebuilding anything.
///
/// <para>
/// Not to be confused with <see cref="ArrayColumn{T}"/>, the generic array-backed storage used by leaf columns:
/// this type is the composite whose <em>values</em> are arrays (its element type is
/// <typeparamref name="TElement"/>[]). <typeparamref name="TElement"/> may itself be an array
/// (<c>Array(Array(T))</c>) or a nullable (<c>Array(Nullable(T))</c>) — whatever the inner codec surfaces.
/// </para>
///
/// <para>
/// The inner column's storage and the offsets are borrowed for this column's lifetime; the inner column is
/// disposed and the offsets returned (when pooled) on <see cref="Dispose"/>. Each <see cref="this"/> access
/// copies the row's slice into a new array, so retain those arrays freely, but read the column itself only while
/// the owning block is alive.
/// </para>
/// </summary>
/// <typeparam name="TElement">The inner codec's CLR element type; each row surfaces as <typeparamref name="TElement"/>[].</typeparam>
internal sealed class ArrayValueColumn<TElement> : IColumn<TElement[]>, IArrayColumn<TElement>
{
    private readonly IColumn<TElement> inner;
    private readonly int rowCount;
    private readonly bool pooledOffsets;
    private int[] offsets;
    private TElement[][] cache;

    /// <summary>Initializes an array column over a flat inner column and its per-row offsets.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The full <c>Array(...)</c> type string.</param>
    /// <param name="inner">The inner column holding every row's elements concatenated end-to-end.</param>
    /// <param name="offsets">The per-row offsets: <c>offsets[0]</c> is 0 and <c>offsets[i + 1]</c> is the exclusive end of row <c>i</c>'s slice; must have at least <paramref name="rowCount"/> + 1 entries.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooledOffsets">Whether <paramref name="offsets"/> was rented and should be returned on dispose.</param>
    public ArrayValueColumn(string name, string typeName, IColumn<TElement> inner, int[] offsets, int rowCount, bool pooledOffsets)
    {
        Name = name;
        TypeName = typeName;
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        this.offsets = offsets ?? throw new ArgumentNullException(nameof(offsets));
        this.rowCount = rowCount;
        this.pooledOffsets = pooledOffsets;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => rowCount;

    /// <summary>The flat inner column (every row's elements concatenated) — the zero-copy write source.</summary>
    internal IColumn<TElement> Inner => inner;

    /// <inheritdoc/>
    public ReadOnlySpan<TElement> InnerValues => inner.Values;

    /// <inheritdoc/>
    public ReadOnlySpan<int> Offsets => offsets.AsSpan(0, rowCount + 1);

    /// <summary>
    /// The rows as arrays, materialized once and cached. Each row is copied out of <see cref="InnerValues"/> into
    /// a freshly allocated <typeparamref name="TElement"/>[] (plus one outer array for the whole column) — the
    /// arrays are caller-owned and outlive the block, at the cost of an allocation per row. To read without
    /// allocating, use <see cref="InnerValues"/> + <see cref="Offsets"/> instead, and copy only if you must retain
    /// past the block. Prefer the indexer when only some rows are needed, to avoid building the whole jagged array.
    /// </summary>
    public ReadOnlySpan<TElement[]> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new TElement[rowCount][];
                for (int i = 0; i < rowCount; i++)
                {
                    decoded[i] = Materialize(i);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, rowCount);
        }
    }

    /// <inheritdoc/>
    public TElement[] this[int row] => cache is not null ? cache[row] : Materialize(row);

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <inheritdoc/>
    public void Dispose()
    {
        inner.Dispose();
        if (pooledOffsets && offsets.Length != 0)
        {
            ArrayPool<int>.Shared.Return(offsets);
        }

        offsets = Array.Empty<int>();
        cache = null;
    }

    /// <summary>Copies row <paramref name="row"/>'s slice of the inner values into a new array.</summary>
    private TElement[] Materialize(int row)
    {
        int start = offsets[row];
        int length = offsets[row + 1] - start;
        return length == 0 ? Array.Empty<TElement>() : inner.Values.Slice(start, length).ToArray();
    }
}
