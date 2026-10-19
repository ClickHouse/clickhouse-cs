using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Maps elements to bytes within a <c>QBit</c> row bitmap.
/// </summary>
internal static class QBitLayout
{
    /// <summary>
    /// Returns the byte containing an eight-element group. Row bitmaps are big-endian, so group 0 occupies the
    /// last byte.
    /// </summary>
    public static int ByteOfGroup(int group, int bytesPerRow) => bytesPerRow - 1 - group;

    /// <summary>
    /// Returns <c>ceil(span / 8)</c> without overflowing when <paramref name="span"/> is positive.
    /// </summary>
    public static int BytesPerRow(int span) => ((span - 1) / 8) + 1;
}

/// <summary>
/// Stores a decoded Native <c>QBit(T, N)</c> body in its transposed form. Planes are ordered most-significant
/// first, and each plane contains one fixed-width bitmap per row. A rented body is returned on
/// <see cref="Dispose"/>; plane spans borrow the body's storage. Typed row vectors are materialized lazily.
/// </summary>
internal abstract class QBitColumn : IQBitColumn
{
    private readonly int rowCount;
    private readonly bool pooled;
    private byte[] blob;

    protected QBitColumn(string name, string typeName, int dimension, int bitWidth, byte[] blob, int rowCount, bool pooled)
    {
        Name = name;
        TypeName = typeName;
        Dimension = dimension;
        BitWidth = bitWidth;
        BytesPerRow = QBitLayout.BytesPerRow(dimension);
        this.blob = blob ?? throw new ArgumentNullException(nameof(blob));
        this.rowCount = rowCount;
        this.pooled = pooled;
    }

    public string Name { get; }

    public string TypeName { get; }

    public int RowCount => rowCount;

    public int Dimension { get; }

    public int BitWidth { get; }

    public int BytesPerRow { get; }

    public int Stride => Dimension;

    public int GroupCount => 1;

    public ReadOnlySpan<byte> GetPlane(int bit)
    {
        if (GroupCount != 1)
        {
            throw new InvalidOperationException(
                $"Column '{Name}' ({TypeName}) has {GroupCount} plane groups, so a plane is not one contiguous run; use GetPlane(bit, group).");
        }

        return GetPlane(bit, group: 0);
    }

    public ReadOnlySpan<byte> GetPlane(int bit, int group)
    {
        if ((uint)bit >= (uint)BitWidth)
        {
            throw new ArgumentOutOfRangeException(
                nameof(bit),
                $"Bit {bit} is outside the {BitWidth} plane(s) of column '{Name}' ({TypeName}).");
        }

        if ((uint)group >= (uint)GroupCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(group),
                $"Group {group} is outside the {GroupCount} group(s) of column '{Name}' ({TypeName}).");
        }

        return WirePlane(((group * BitWidth) + BitWidth - 1 - bit), 0, rowCount);
    }

    public abstract object GetValue(int row);

    public virtual void Dispose()
    {
        if (pooled && blob.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(blob);
        }

        blob = Array.Empty<byte>();
    }

    /// <summary>
    /// Returns a zero-copy row range from a plane indexed in wire order, most-significant plane first.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The requested range is outside the logical row count.</exception>
    internal ReadOnlySpan<byte> WirePlane(int wireIndex, int start, int length)
    {
        // Validate against the logical row count because a rented blob can contain unused trailing bytes.
        if (start < 0 || length < 0 || start + (long)length > rowCount)
        {
            throw new ArgumentOutOfRangeException(
                length < 0 ? nameof(length) : nameof(start),
                $"Rows [{start}, {start + (long)length}) lie outside the {rowCount} row(s) of column '{Name}'.");
        }

        return blob.AsSpan(((wireIndex * rowCount) + start) * BytesPerRow, length * BytesPerRow);
    }

    /// <summary>Gets whether rows decode 32 elements at a time with 256-bit vectors.</summary>
    protected static bool DecodesBlocks => Vector256.IsHardwareAccelerated && BitConverter.IsLittleEndian;

    /// <summary>Gets the distance in bytes between two planes of the body.</summary>
    protected int PlaneStride => rowCount * BytesPerRow;

    /// <summary>
    /// Returns the body from <paramref name="row"/>'s bitmap in plane 0 to the end of its bitmap in the last plane;
    /// plane <c>p</c> starts <c>p * <see cref="PlaneStride"/></c> bytes in.
    /// </summary>
    /// <exception cref="IndexOutOfRangeException"><paramref name="row"/> is outside the logical row count.</exception>
    internal ReadOnlySpan<byte> RowPlanes(int row)
    {
        CheckRow(row);
        return blob.AsSpan(row * BytesPerRow, ((BitWidth - 1) * PlaneStride) + BytesPerRow);
    }

    protected void CheckRow(int row)
    {
        if ((uint)row >= (uint)rowCount)
        {
            throw new IndexOutOfRangeException();
        }
    }

    /// <summary>
    /// Decodes one row without SIMD into the zeroed <paramref name="raw"/>. <typeparamref name="T"/> holds each
    /// element's bit pattern, and plane <c>p</c> is bit <c>bits(T) - 1 - p</c> of it.
    /// </summary>
    internal void DetransposeScalar<T>(ReadOnlySpan<byte> planes, Span<T> raw)
        where T : unmanaged, IBinaryInteger<T>
    {
        int top = (Unsafe.SizeOf<T>() * 8) - 1;
        for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
        {
            ReadOnlySpan<byte> plane = planes.Slice(wireIndex * PlaneStride, BytesPerRow);
            int shift = top - wireIndex;
            for (int i = 0; i < raw.Length; i++)
            {
                int bit = (plane[QBitLayout.ByteOfGroup(i >> 3, BytesPerRow)] >> (i & 7)) & 1;
                raw[i] |= T.CreateTruncating(bit) << shift;
            }
        }
    }

    /// <summary>
    /// Decodes one row in 32-element blocks. A complete block's four bytes in a plane are bytes <c>offset</c> to
    /// <c>offset + 3</c> of the row bitmap, its last group first. The remainder's groups are the first bytes of the
    /// row bitmap: they are copied into a four-byte-per-plane buffer, with zeros for the missing groups, decoded as
    /// a block, and only the remainder's elements are kept.
    /// </summary>
    protected void DetransposeBlocks<T>(ReadOnlySpan<byte> planes, Span<T> raw, BlockReader<T> readBlock)
        where T : unmanaged
    {
        int planeStride = PlaneStride;
        int blocks = Dimension >> 5;
        for (int block = 0; block < blocks; block++)
        {
            readBlock(planes, BytesPerRow - 4 - (block << 2), planeStride, raw.Slice(block << 5, 32));
        }

        int rest = Dimension & 31;
        if (rest != 0)
        {
            int groups = (rest + 7) >> 3;
            Span<byte> partial = stackalloc byte[BitWidth * 4];
            partial.Clear();
            // At most four bytes per plane: a byte loop is cheaper than a copy call.
            for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
            {
                ReadOnlySpan<byte> source = planes.Slice(wireIndex * planeStride, groups);
                Span<byte> target = partial.Slice((wireIndex * 4) + 4 - groups, groups);
                for (int group = 0; group < source.Length; group++)
                {
                    target[group] = source[group];
                }
            }

            Span<T> padded = stackalloc T[32];
            readBlock(partial, 0, 4, padded);
            padded.Slice(0, rest).CopyTo(raw.Slice(blocks << 5));
        }
    }

    /// <summary>
    /// Reads eight planes of a complete block. Byte <c>j</c> of the result holds element <c>j</c>'s bits from these
    /// planes, the first plane in its most significant bit.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static Vector256<byte> GatherPlanes(ReadOnlySpan<byte> planes, ref int offset, int planeStride)
    {
        // One range check covers the eight loads, so the loop itself needs no bounds checks. Each address is
        // computed from the start of the span, so no reference ever points past its end.
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(planeStride);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(offset + (7L * planeStride) + sizeof(uint), planes.Length);
        ref byte start = ref MemoryMarshal.GetReference(planes);
        Vector256<byte> bytes = Vector256<byte>.Zero;
        for (int i = 0; i < 8; i++)
        {
            uint mask = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref start, offset + (i * planeStride)));

            // Shift the earlier planes up and add this plane's bit: the set lanes are all ones, that is -1.
            bytes = bytes + bytes - ExpandBits(BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(mask) : mask);
        }

        offset += 8 * planeStride;
        return bytes;
    }

    /// <summary>Reads the next 32 planes of a block as 32 lanes of 32 bits, most significant bit first.</summary>
    protected static (Vector256<uint> Lanes0, Vector256<uint> Lanes8, Vector256<uint> Lanes16, Vector256<uint> Lanes24) GatherWords(
        ReadOnlySpan<byte> planes, ref int offset, int planeStride)
    {
        Vector256<byte> byte3 = GatherPlanes(planes, ref offset, planeStride);
        Vector256<byte> byte2 = GatherPlanes(planes, ref offset, planeStride);
        Vector256<byte> byte1 = GatherPlanes(planes, ref offset, planeStride);
        Vector256<byte> byte0 = GatherPlanes(planes, ref offset, planeStride);
        (Vector256<ushort> high0, Vector256<ushort> high16) = Join(byte3, byte2);
        (Vector256<ushort> low0, Vector256<ushort> low16) = Join(byte1, byte0);
        (Vector256<uint> lanes0, Vector256<uint> lanes8) = Join(high0, low0);
        (Vector256<uint> lanes16, Vector256<uint> lanes24) = Join(high16, low16);
        return (lanes0, lanes8, lanes16, lanes24);
    }

    /// <summary>Joins two vectors of bytes into 16-bit lanes, in lane order; <paramref name="high"/> is the high byte.</summary>
    protected static (Vector256<ushort> Lanes0, Vector256<ushort> Lanes16) Join(Vector256<byte> high, Vector256<byte> low)
    {
        (Vector256<ushort> high0, Vector256<ushort> high16) = Vector256.Widen(high);
        (Vector256<ushort> low0, Vector256<ushort> low16) = Vector256.Widen(low);
        return ((high0 << 8) | low0, (high16 << 8) | low16);
    }

    /// <summary>Joins two vectors of 16-bit lanes into 32-bit lanes, in lane order.</summary>
    protected static (Vector256<uint> Lanes0, Vector256<uint> Lanes8) Join(Vector256<ushort> high, Vector256<ushort> low)
    {
        (Vector256<uint> high0, Vector256<uint> high8) = Vector256.Widen(high);
        (Vector256<uint> low0, Vector256<uint> low8) = Vector256.Widen(low);
        return ((high0 << 16) | low0, (high8 << 16) | low8);
    }

    /// <summary>Joins two vectors of 32-bit lanes into 64-bit lanes, in lane order.</summary>
    protected static (Vector256<ulong> Lanes0, Vector256<ulong> Lanes4) Join(Vector256<uint> high, Vector256<uint> low)
    {
        (Vector256<ulong> high0, Vector256<ulong> high4) = Vector256.Widen(high);
        (Vector256<ulong> low0, Vector256<ulong> low4) = Vector256.Widen(low);
        return ((high0 << 32) | low0, (high4 << 32) | low4);
    }

    /// <summary>Returns a vector whose byte <c>j</c> is all ones when bit <c>j</c> of <paramref name="mask"/> is set.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<byte> ExpandBits(uint mask)
    {
        // Byte j takes byte j / 8 of the mask: indices 0, 1, 18, 19, eight times each. Every index stays in its
        // own 128-bit half, so this is one in-lane byte shuffle.
        Vector256<byte> spread = Vector256.Shuffle(
            Vector256.Create(mask).AsByte(),
            Vector256.Create(0UL, 0x0101010101010101UL, 0x1212121212121212UL, 0x1313131313131313UL).AsByte());
        Vector256<byte> select = Vector256.Create(0x8040201008040201UL).AsByte();
        return Vector256.Equals(spread & select, select);
    }

    /// <summary>Reads one 32-element block into <paramref name="block"/>; see <see cref="GatherPlanes"/>.</summary>
    protected delegate void BlockReader<T>(ReadOnlySpan<byte> planes, int offset, int planeStride, Span<T> block);
}

/// <summary>
/// Provides lazily materialized <typeparamref name="T"/>[] rows over a transposed <see cref="QBitColumn"/>.
/// </summary>
internal abstract class QBitColumnBase<T> : QBitColumn, IColumn<T[]>
    where T : struct
{
    private T[][] cache;

    protected QBitColumnBase(string name, string typeName, int dimension, int bitWidth, byte[] blob, int rowCount, bool pooled)
        : base(name, typeName, dimension, bitWidth, blob, rowCount, pooled)
    {
    }

    /// <summary>
    /// Gets row vectors, materializing and caching all rows on first access. Use <see cref="GetPlane(int)"/> to
    /// inspect transposed data without this allocation.
    /// </summary>
    public ReadOnlySpan<T[]> Values
    {
        get
        {
            if (cache is null)
            {
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

    // Slice to RowCount so indexing cannot reach unused entries in the pooled array.
    public T[] this[int row] => cache is not null ? cache.AsSpan(0, RowCount)[row] : DetransposeRow(row);

    public override object GetValue(int row) => this[row];

    public override void Dispose()
    {
        base.Dispose();

        if (cache is not null)
        {
            // Clear references so the pool does not retain decoded row arrays.
            ArrayPool<T[]>.Shared.Return(cache, clearArray: true);
            cache = null;
        }
    }

    protected abstract T[] DetransposeRow(int row);
}

/// <summary>
/// Decodes <c>QBit(Float32, N)</c> and <c>QBit(BFloat16, N)</c> rows as <see cref="float"/> arrays. A
/// <c>BFloat16</c> value fills the high 16 bits of the widened result.
/// </summary>
internal sealed class QBitFloatColumn : QBitColumnBase<float>
{
    public QBitFloatColumn(string name, string typeName, int dimension, int bitWidth, byte[] blob, int rowCount, bool pooled)
        : base(name, typeName, dimension, bitWidth, blob, rowCount, pooled)
    {
    }

    private static readonly BlockReader<uint> Float32Reader = ReadFloat32Block;
    private static readonly BlockReader<uint> BFloat16Reader = ReadBFloat16Block;

    protected override float[] DetransposeRow(int row)
    {
        ReadOnlySpan<byte> planes = RowPlanes(row);
        var vector = new float[Dimension];

        // Build each IEEE-754 pattern directly in the destination array. BFloat16 fills the high 16 bits.
        Span<uint> bits = MemoryMarshal.Cast<float, uint>(vector.AsSpan());
        if (DecodesBlocks)
        {
            DetransposeBlocks(planes, bits, BitWidth == 16 ? BFloat16Reader : Float32Reader);
        }
        else
        {
            DetransposeScalar(planes, bits);
        }

        return vector;
    }

    private static void ReadFloat32Block(ReadOnlySpan<byte> planes, int offset, int planeStride, Span<uint> block)
    {
        (Vector256<uint> lanes0, Vector256<uint> lanes8, Vector256<uint> lanes16, Vector256<uint> lanes24) =
            GatherWords(planes, ref offset, planeStride);
        lanes0.CopyTo(block);
        lanes8.CopyTo(block.Slice(8));
        lanes16.CopyTo(block.Slice(16));
        lanes24.CopyTo(block.Slice(24));
    }

    private static void ReadBFloat16Block(ReadOnlySpan<byte> planes, int offset, int planeStride, Span<uint> block)
    {
        Vector256<byte> byte3 = GatherPlanes(planes, ref offset, planeStride);
        Vector256<byte> byte2 = GatherPlanes(planes, ref offset, planeStride);
        (Vector256<ushort> high0, Vector256<ushort> high16) = Join(byte3, byte2);
        (Vector256<uint> lanes0, Vector256<uint> lanes8) = Vector256.Widen(high0);
        (Vector256<uint> lanes16, Vector256<uint> lanes24) = Vector256.Widen(high16);
        (lanes0 << 16).CopyTo(block);
        (lanes8 << 16).CopyTo(block.Slice(8));
        (lanes16 << 16).CopyTo(block.Slice(16));
        (lanes24 << 16).CopyTo(block.Slice(24));
    }
}

/// <summary>
/// Decodes <c>QBit(Int8, N)</c> rows from each element's two's-complement bit pattern.
/// </summary>
internal sealed class QBitSByteColumn : QBitColumnBase<sbyte>
{
    public QBitSByteColumn(string name, string typeName, int dimension, byte[] blob, int rowCount, bool pooled)
        : base(name, typeName, dimension, bitWidth: 8, blob, rowCount, pooled)
    {
    }

    protected override sbyte[] DetransposeRow(int row)
    {
        ReadOnlySpan<byte> planes = RowPlanes(row);
        var vector = new sbyte[Dimension];

        // Use an unsigned view so the sign plane sets bit 7 without numeric conversion.
        Span<byte> bits = MemoryMarshal.Cast<sbyte, byte>(vector.AsSpan());
        if (DecodesBlocks)
        {
            DetransposeBlocks(planes, bits, ReadBlock);
        }
        else
        {
            DetransposeScalar(planes, bits);
        }

        return vector;
    }

    private static void ReadBlock(ReadOnlySpan<byte> planes, int offset, int planeStride, Span<byte> block)
        => GatherPlanes(planes, ref offset, planeStride).CopyTo(block);
}

/// <summary>Decodes <c>QBit(Float64, N)</c> rows from each element's IEEE-754 bit pattern.</summary>
internal sealed class QBitDoubleColumn : QBitColumnBase<double>
{
    public QBitDoubleColumn(string name, string typeName, int dimension, byte[] blob, int rowCount, bool pooled)
        : base(name, typeName, dimension, bitWidth: 64, blob, rowCount, pooled)
    {
    }

    protected override double[] DetransposeRow(int row)
    {
        ReadOnlySpan<byte> planes = RowPlanes(row);
        var vector = new double[Dimension];
        Span<ulong> bits = MemoryMarshal.Cast<double, ulong>(vector.AsSpan());
        if (DecodesBlocks)
        {
            DetransposeBlocks(planes, bits, ReadBlock);
        }
        else
        {
            DetransposeScalar(planes, bits);
        }

        return vector;
    }

    private static void ReadBlock(ReadOnlySpan<byte> planes, int offset, int planeStride, Span<ulong> block)
    {
        // Rebuild the high and low 32 bits of each element, then join them.
        (Vector256<uint> high0, Vector256<uint> high8, Vector256<uint> high16, Vector256<uint> high24) =
            GatherWords(planes, ref offset, planeStride);
        (Vector256<uint> low0, Vector256<uint> low8, Vector256<uint> low16, Vector256<uint> low24) =
            GatherWords(planes, ref offset, planeStride);
        (Vector256<ulong> lanes0, Vector256<ulong> lanes4) = Join(high0, low0);
        (Vector256<ulong> lanes8, Vector256<ulong> lanes12) = Join(high8, low8);
        (Vector256<ulong> lanes16, Vector256<ulong> lanes20) = Join(high16, low16);
        (Vector256<ulong> lanes24, Vector256<ulong> lanes28) = Join(high24, low24);
        lanes0.CopyTo(block);
        lanes4.CopyTo(block.Slice(4));
        lanes8.CopyTo(block.Slice(8));
        lanes12.CopyTo(block.Slice(12));
        lanes16.CopyTo(block.Slice(16));
        lanes20.CopyTo(block.Slice(20));
        lanes24.CopyTo(block.Slice(24));
        lanes28.CopyTo(block.Slice(28));
    }
}
