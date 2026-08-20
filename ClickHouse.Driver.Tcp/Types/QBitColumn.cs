using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A decoded <c>QBit(T, N)</c> column: the bit-plane blob exactly as it arrived, plus the geometry needed to
/// read it. The blob is <c>BitWidth</c> planes, each holding one <c>ceil(N / 8)</c>-byte bitmap per row, and the
/// planes are stored most-significant first — so the plane for bit <c>b</c> is at wire index
/// <c>BitWidth - 1 - b</c>. See <see cref="IQBitColumn"/> for the layout a caller sees.
///
/// <para>
/// The blob is kept transposed rather than de-transposed on read, so a column read from the server and inserted
/// straight back is a byte copy with no transposition at all — the common shape for a vector workload, where the
/// distance is computed server-side and the client never looks at a vector. The per-row vector view is
/// therefore materialized lazily by <see cref="QBitColumnBase{T}"/>, never eagerly.
/// </para>
///
/// <para>
/// This non-generic base carries everything that does not depend on whether the elements surface as
/// <see cref="float"/> or <see cref="double"/>, which is what lets the codec's dense write path recognise a
/// QBit column and copy its planes without knowing the element type.
/// </para>
///
/// <para>
/// The blob is rented from <see cref="ArrayPool{T}"/> and returned on <see cref="Dispose"/>; like every column,
/// the bytes and any span returned by <see cref="GetPlane"/> are borrowed for the block's lifetime.
/// </para>
/// </summary>
internal abstract class QBitColumn : IQBitColumn
{
    private readonly int rowCount;
    private readonly bool pooled;
    private byte[] blob;

    /// <summary>Initializes a column over a bit-plane blob.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string (e.g. <c>QBit(Float32, 4)</c>).</param>
    /// <param name="dimension">The vector length <c>N</c>.</param>
    /// <param name="bitWidth">The number of planes — the stored element's bit width.</param>
    /// <param name="blob">The plane blob (may be longer than used).</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooled">Whether <paramref name="blob"/> was rented and should be returned on dispose.</param>
    protected QBitColumn(string name, string typeName, int dimension, int bitWidth, byte[] blob, int rowCount, bool pooled)
    {
        Name = name;
        TypeName = typeName;
        Dimension = dimension;
        BitWidth = bitWidth;
        BytesPerRow = (dimension + 7) / 8;
        this.blob = blob ?? throw new ArgumentNullException(nameof(blob));
        this.rowCount = rowCount;
        this.pooled = pooled;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => rowCount;

    /// <inheritdoc/>
    public int Dimension { get; }

    /// <inheritdoc/>
    public int BitWidth { get; }

    /// <inheritdoc/>
    public int BytesPerRow { get; }

    /// <inheritdoc/>
    public ReadOnlySpan<byte> GetPlane(int bit)
    {
        if ((uint)bit >= (uint)BitWidth)
        {
            throw new ArgumentOutOfRangeException(
                nameof(bit),
                $"Bit {bit} is outside the {BitWidth} plane(s) of column '{Name}' ({TypeName}).");
        }

        return WirePlane(BitWidth - 1 - bit, 0, rowCount);
    }

    /// <inheritdoc/>
    public abstract object GetValue(int row);

    /// <inheritdoc/>
    public virtual void Dispose()
    {
        if (pooled && blob.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(blob);
        }

        blob = Array.Empty<byte>();
    }

    /// <summary>
    /// The rows <c>[start, start + length)</c> of the plane at <paramref name="wireIndex"/> — plane order as
    /// stored, most significant first — as a zero-copy slice of the blob. The write path emits planes in this
    /// order, and a row range within one plane is contiguous, so a dense re-insert is one copy per plane.
    /// </summary>
    /// <param name="wireIndex">The plane's index in stored order, 0 being the most significant bit.</param>
    /// <param name="start">The zero-based first row of the range.</param>
    /// <param name="length">The number of rows in the range.</param>
    /// <returns>The range's bytes within that plane, <c>length * BytesPerRow</c> bytes.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The range lies outside the column's rows.</exception>
    internal ReadOnlySpan<byte> WirePlane(int wireIndex, int start, int length)
    {
        // Bound the range against rowCount, not the blob: the blob is rented and may be longer, so slicing it
        // directly would let an over-long range read a stale pooled region instead of failing fast. The products
        // cannot overflow — the read path sized the blob with a checked total, and this range fits in it.
        if (start < 0 || length < 0 || start + (long)length > rowCount)
        {
            throw new ArgumentOutOfRangeException(
                length < 0 ? nameof(length) : nameof(start),
                $"Rows [{start}, {start + (long)length}) lie outside the {rowCount} row(s) of column '{Name}'.");
        }

        return blob.AsSpan(((wireIndex * rowCount) + start) * BytesPerRow, length * BytesPerRow);
    }

    /// <summary>The bitmap of one row within the plane at <paramref name="wireIndex"/>.</summary>
    /// <param name="wireIndex">The plane's index in stored order, 0 being the most significant bit.</param>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The row's <c>BytesPerRow</c> bytes within that plane.</returns>
    protected ReadOnlySpan<byte> WirePlaneRow(int wireIndex, int row) => WirePlane(wireIndex, row, 1);

    /// <summary>Bounds a row index against the column's rows.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <exception cref="IndexOutOfRangeException">The row lies outside the column.</exception>
    protected void CheckRow(int row)
    {
        if ((uint)row >= (uint)rowCount)
        {
            throw new IndexOutOfRangeException();
        }
    }
}

/// <summary>
/// The typed per-row view over a <see cref="QBitColumn"/>: each row's vector as a
/// <typeparamref name="T"/>[], de-transposed on demand and cached.
/// </summary>
/// <typeparam name="T">The CLR element type a row's vector surfaces as — <see cref="float"/> or <see cref="double"/>.</typeparam>
internal abstract class QBitColumnBase<T> : QBitColumn, IColumn<T[]>
    where T : struct
{
    private T[][] cache;

    /// <summary>Initializes the typed view.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="dimension">The vector length <c>N</c>.</param>
    /// <param name="bitWidth">The number of planes.</param>
    /// <param name="blob">The plane blob.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooled">Whether <paramref name="blob"/> was rented.</param>
    protected QBitColumnBase(string name, string typeName, int dimension, int bitWidth, byte[] blob, int rowCount, bool pooled)
        : base(name, typeName, dimension, bitWidth, blob, rowCount, pooled)
    {
    }

    /// <summary>
    /// The rows as per-row vectors, materialized once and cached. Every row costs
    /// <c>BitWidth * BytesPerRow</c> byte fetches to de-transpose — 4 KiB for a 1024-dimension
    /// <c>Float32</c> embedding — so this is built on first use, not on read. Prefer
    /// <see cref="QBitColumn.GetPlane"/> where the planes themselves are what is wanted.
    /// </summary>
    public ReadOnlySpan<T[]> Values
    {
        get
        {
            if (cache is null)
            {
                // Rent rather than allocate: this is a convenience view consumers copy out of, so it only needs
                // to live until Dispose returns it to the pool. Single-consumer per connection, so the lazy fill
                // needs no synchronization. The rented buffer may be longer than RowCount; Values slices to it.
                //
                // De-transposed one whole row at a time, rather than one plane across all rows: a row's output
                // vector then stays in cache for all BitWidth planes that write into it, where sweeping
                // plane-by-plane would re-traverse the entire materialized output once per plane.
                T[][] decoded = ArrayPool<T[]>.Shared.Rent(RowCount);
                for (int i = 0; i < RowCount; i++)
                {
                    decoded[i] = DetransposeRow(i);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, RowCount);
        }
    }

    /// <inheritdoc/>
    // The cache is rented and may be longer than RowCount, so slice before indexing to keep an out-of-range row
    // failing fast rather than returning a stale slot; the uncached path is bounded by DetransposeRow.
    public T[] this[int row] => cache is not null ? cache.AsSpan(0, RowCount)[row] : DetransposeRow(row);

    /// <inheritdoc/>
    public override object GetValue(int row) => this[row];

    /// <inheritdoc/>
    public override void Dispose()
    {
        base.Dispose();

        if (cache is not null)
        {
            // The elements are array references, so clear on return to avoid the pool pinning decoded rows.
            ArrayPool<T[]>.Shared.Return(cache, clearArray: true);
            cache = null;
        }
    }

    /// <summary>Rebuilds one row's vector from the planes.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The row's <see cref="QBitColumn.Dimension"/>-element vector.</returns>
    protected abstract T[] DetransposeRow(int row);
}

/// <summary>
/// A <c>QBit(Float32, N)</c> or <c>QBit(BFloat16, N)</c> column. Both surface as <see cref="float"/>: a
/// brain-float is the top 16 bits of an IEEE-754 <see cref="float"/>, so its 16 planes rebuild the high half of
/// the 32-bit pattern and the low half stays zero — the same widening <c>BFloat16ColumnCodec</c> does for a
/// plain <c>BFloat16</c> column.
/// </summary>
internal sealed class QBitFloatColumn : QBitColumnBase<float>
{
    /// <summary>Initializes a single-precision QBit column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="dimension">The vector length <c>N</c>.</param>
    /// <param name="bitWidth">16 for <c>BFloat16</c>, 32 for <c>Float32</c>.</param>
    /// <param name="blob">The plane blob.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooled">Whether <paramref name="blob"/> was rented.</param>
    public QBitFloatColumn(string name, string typeName, int dimension, int bitWidth, byte[] blob, int rowCount, bool pooled)
        : base(name, typeName, dimension, bitWidth, blob, rowCount, pooled)
    {
    }

    /// <inheritdoc/>
    protected override float[] DetransposeRow(int row)
    {
        CheckRow(row);

        var vector = new float[Dimension];

        // Accumulate through the float's own storage rather than a separate integer scratch: the bits being
        // gathered *are* the IEEE-754 pattern, so the vector holds the finished values once the last plane is in.
        Span<uint> bits = MemoryMarshal.Cast<float, uint>(vector.AsSpan());
        for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
        {
            ReadOnlySpan<byte> plane = WirePlaneRow(wireIndex, row);
            uint mask = 1u << (BitWidth - 1 - wireIndex);
            for (int i = 0; i < bits.Length; i++)
            {
                if ((plane[i >> 3] & (1 << (i & 7))) != 0)
                {
                    bits[i] |= mask;
                }
            }
        }

        // A brain-float's 16 bits are the float's high half, so shift them up into it.
        if (BitWidth == 16)
        {
            for (int i = 0; i < bits.Length; i++)
            {
                bits[i] <<= 16;
            }
        }

        return vector;
    }
}

/// <summary>A <c>QBit(Float64, N)</c> column: 64 planes rebuilding each element's IEEE-754 double pattern.</summary>
internal sealed class QBitDoubleColumn : QBitColumnBase<double>
{
    /// <summary>Initializes a double-precision QBit column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="dimension">The vector length <c>N</c>.</param>
    /// <param name="blob">The plane blob.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooled">Whether <paramref name="blob"/> was rented.</param>
    public QBitDoubleColumn(string name, string typeName, int dimension, byte[] blob, int rowCount, bool pooled)
        : base(name, typeName, dimension, bitWidth: 64, blob, rowCount, pooled)
    {
    }

    /// <inheritdoc/>
    protected override double[] DetransposeRow(int row)
    {
        CheckRow(row);

        var vector = new double[Dimension];
        Span<ulong> bits = MemoryMarshal.Cast<double, ulong>(vector.AsSpan());
        for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
        {
            ReadOnlySpan<byte> plane = WirePlaneRow(wireIndex, row);
            ulong mask = 1UL << (BitWidth - 1 - wireIndex);
            for (int i = 0; i < bits.Length; i++)
            {
                if ((plane[i >> 3] & (1 << (i & 7))) != 0)
                {
                    bits[i] |= mask;
                }
            }
        }

        return vector;
    }
}
