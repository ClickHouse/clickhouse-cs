using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A lazy write-path view that flattens the per-row arrays of an ergonomic jagged column
/// (<see cref="IColumn{T}"/> of <c>T[]</c>) into one logical element sequence, so a composite (<c>Array</c>,
/// <c>Map</c>, <c>Nested</c>) can hand its inner codec every element as a single contiguous-looking column —
/// which a sectioned inner encoding (a null-map, a dictionary, nested offsets) needs so it emits its section
/// once spanning the whole run — without copying the elements into a flat buffer first.
///
/// <para>
/// The flat index of an element is mapped to its (row, offset-within-row) through the row offsets. Access is
/// typically sequential, so a one-row cursor short-circuits the common case; an out-of-order access falls back
/// to a binary search over the offsets. The view has no contiguous span (its elements live in separate row
/// arrays), so it is read per element. It borrows the source: disposing it does nothing.
/// </para>
/// </summary>
/// <typeparam name="T">The inner element type.</typeparam>
internal sealed class ConcatColumn<T> : IColumn<T>
{
    private readonly IColumn<T[]> source;
    private readonly int sliceStart;
    private readonly int[] offsets;
    private readonly int total;
    private int hintRow;

    /// <summary>Initializes a flattening view over the rows [<paramref name="sliceStart"/>, sliceStart + sliceRows) of <paramref name="source"/>.</summary>
    /// <param name="typeName">The inner element type name the view reports (the inner codec's own type).</param>
    /// <param name="source">The ergonomic jagged column.</param>
    /// <param name="sliceStart">The first source row the view flattens.</param>
    /// <param name="offsets">The slice-relative cumulative per-row element ends: length <c>sliceRows + 1</c>, <c>offsets[0] = 0</c>.</param>
    /// <param name="total">The total element count in the slice (<c>offsets[sliceRows]</c>).</param>
    public ConcatColumn(string typeName, IColumn<T[]> source, int sliceStart, int[] offsets, int total)
    {
        TypeName = typeName;
        this.source = source;
        this.sliceStart = sliceStart;
        this.offsets = offsets;
        this.total = total;
    }

    private int SliceRows => offsets.Length - 1;

    /// <inheritdoc/>
    public string Name => source.Name;

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => total;

    /// <inheritdoc/>
    public T this[int flat]
    {
        get
        {
            int row = RowOf(flat);
            return source[sliceStart + row][flat - offsets[row]];
        }
    }

    /// <inheritdoc/>
    public object GetValue(int flat) => this[flat];

    /// <summary>Not supported: the elements live in separate row arrays, so there is no single span.</summary>
    /// <exception cref="NotSupportedException">Always — read the view per element.</exception>
    public ReadOnlySpan<T> Values => throw new NotSupportedException($"{nameof(ConcatColumn<T>)} has no contiguous span; read it per element.");

    /// <inheritdoc/>
    public void Dispose()
    {
    }

    // Maps a flat element index to its source row: offsets[row] <= flat < offsets[row + 1]. Sequential access
    // walks off the cached row cheaply; anything else binary-searches the offsets. Empty rows (equal consecutive
    // offsets) hold no flat index, so the search naturally skips them.
    private int RowOf(int flat)
    {
        int row = hintRow;
        if (row < SliceRows && flat >= offsets[row] && flat < offsets[row + 1])
        {
            return row;
        }

        if (row + 1 < SliceRows && flat >= offsets[row + 1] && flat < offsets[row + 2])
        {
            hintRow = row + 1;
            return row + 1;
        }

        // Largest row with offsets[row] <= flat, via an upper-bound search over the offset boundaries.
        int lo = 0;
        int hi = SliceRows;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (offsets[mid] <= flat)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        hintRow = lo - 1;
        return hintRow;
    }
}
