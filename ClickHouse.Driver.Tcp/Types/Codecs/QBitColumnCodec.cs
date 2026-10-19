using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Encodes and decodes the Native layout of <c>QBit(T, N)</c>. The body has no state prefix and contains <c>bits(T)</c>
/// planes in most-significant-first order. Each plane contains one big-endian <c>ceil(N / 8)</c>-byte bitmap per
/// row. Element <c>i</c> is bit <c>i % 8</c> of byte <c>ceil(N / 8) - 1 - i / 8</c>. The body size is
/// <c>bits(T) * rowCount * ceil(N / 8)</c> bytes. Supported element types are <c>Int8</c>, <c>BFloat16</c>,
/// <c>Float32</c>, and <c>Float64</c>.
/// </summary>
internal abstract class QBitColumnCodec : IColumnCodec
{
    /// <summary>The target size of the buffer a transposed write fills one block of rows at a time.</summary>
    private const int TransposeBlockBytes = 4096;

    protected QBitColumnCodec(string typeName, int dimension, int bitWidth)
    {
        TypeName = typeName;
        Dimension = dimension;
        BitWidth = bitWidth;
        BytesPerRow = QBitLayout.BytesPerRow(dimension);
    }

    public string TypeName { get; }

    public abstract Type ElementType { get; }

    public abstract object NullPlaceholder { get; }

    protected int Dimension { get; }

    protected int BitWidth { get; }

    protected int BytesPerRow { get; }

    /// <summary>Gets the plane-group width. Strided types are rejected, so this equals <see cref="Dimension"/>.</summary>
    protected int Stride => Dimension;

    /// <summary>Creates a codec for an unstrided <c>QBit(T, N)</c> type.</summary>
    /// <exception cref="FormatException">The argument count or dimension is invalid.</exception>
    /// <exception cref="NotSupportedException">The element type or layout is unsupported.</exception>
    public static QBitColumnCodec Create(TypeNode node)
    {
        // The three-argument form uses a group-major strided layout that this codec cannot decode.
        if (node.Arguments.Count == 3)
        {
            throw new NotSupportedException(
                $"QBit type '{node}' is strided; this client does not support the strided QBit layout yet.");
        }

        if (node.Arguments.Count != 2)
        {
            throw new FormatException(
                $"QBit type '{node}' must have exactly two arguments: the element type and the vector length.");
        }

        string typeName = node.ToString();
        string element = node.Arguments[0].Name.Trim();
        string token = node.Arguments[1].Name.Trim();

        if (!int.TryParse(token, NumberStyles.None, CultureInfo.InvariantCulture, out int dimension) || dimension <= 0)
        {
            throw new FormatException(
                $"QBit type '{node}' has an invalid vector length '{token}'; expected a positive integer.");
        }

        // These are the element types whose bit-plane widths this codec implements.
        return element switch
        {
            "Int8" => new QBitSByteColumnCodec(typeName, dimension),
            "BFloat16" => new QBitFloatColumnCodec(typeName, dimension, bitWidth: 16),
            "Float32" => new QBitFloatColumnCodec(typeName, dimension, bitWidth: 32),
            "Float64" => new QBitDoubleColumnCodec(typeName, dimension),
            _ => throw new NotSupportedException(
                $"QBit type '{node}' has element type '{element}'; this client encodes Int8, BFloat16, Float32 and Float64."),
        };
    }

    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            return CreateColumn(columnName, columnType, Array.Empty<byte>(), rowCount: 0, pooled: false);
        }

        int byteCount = checked(BitWidth * rowCount * BytesPerRow);
        byte[] blob = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            await reader.ReadBytesAsync(blob.AsMemory(0, byteCount), cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // No column owns the rented buffer after a failed read.
            ArrayPool<byte>.Shared.Return(blob);
            throw;
        }

        return CreateColumn(columnName, columnType, blob, rowCount, pooled: true);
    }

    public abstract bool CanWrite(IColumn column);

    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // A row range is contiguous within each plane, but planes are spaced by the source column's full row
        // count. Dense copies require identical plane grouping; equal body sizes do not imply equal layouts.
        if (column is QBitColumn dense && dense.Dimension == Dimension && dense.BitWidth == BitWidth && dense.Stride == Stride)
        {
            for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
            {
                writer.WriteBytes(dense.WirePlane(wireIndex, start, length));
            }

            return;
        }

        WriteTransposed(writer, column, start, length);
    }

    protected abstract IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled);

    /// <summary>
    /// Writes one row's bitmap into every plane. <paramref name="destination"/> starts at the row's bitmap in plane 0;
    /// plane <c>p</c> starts <c>p * planeStride</c> bytes later. Implementations write every byte of the bitmap in
    /// every plane, so the destination needs no clearing.
    /// </summary>
    /// <exception cref="ArgumentException">The vector is null or its length differs from <see cref="Dimension"/>.</exception>
    protected abstract void TransposeRow(IColumn column, int row, Span<byte> destination, int planeStride);

    /// <summary>
    /// Transposes one row without SIMD. <typeparamref name="T"/> holds each element's bit pattern, and plane
    /// <c>p</c> is bit <c>bits(T) - 1 - p</c> of it.
    /// </summary>
    internal void TransposeScalar<T>(ReadOnlySpan<T> raw, Span<byte> destination, int planeStride)
        where T : unmanaged, IBinaryInteger<T>
    {
        int top = (Unsafe.SizeOf<T>() * 8) - 1;
        for (int group = 0; group < BytesPerRow; group++)
        {
            int first = group << 3;
            ReadOnlySpan<T> elements = raw.Slice(first, Math.Min(8, raw.Length - first));
            int slot = QBitLayout.ByteOfGroup(group, BytesPerRow);
            for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
            {
                int shift = top - wireIndex;
                int bits = 0;
                for (int i = 0; i < elements.Length; i++)
                {
                    bits |= int.CreateTruncating((elements[i] >> shift) & T.One) << i;
                }

                destination[(wireIndex * planeStride) + slot] = (byte)bits;
            }
        }
    }

    /// <summary>
    /// Writes the vector in 32-element blocks. A complete block's four bytes in a plane are bytes
    /// <c>offset</c> to <c>offset + 3</c> of the row bitmap, its last group first. The remainder is padded with zero
    /// elements, which contribute no bits, and written through a four-byte-per-plane buffer, from which only the
    /// bytes of its groups are copied: they are the first bytes of the row bitmap.
    /// </summary>
    protected void TransposeBlocks<T>(ReadOnlySpan<T> raw, Span<byte> destination, int planeStride, BlockWriter<T> writeBlock)
        where T : unmanaged
    {
        int blocks = Dimension >> 5;
        for (int block = 0; block < blocks; block++)
        {
            writeBlock(raw.Slice(block << 5, 32), destination, BytesPerRow - 4 - (block << 2), planeStride);
        }

        int rest = Dimension & 31;
        if (rest != 0)
        {
            Span<T> padded = stackalloc T[32];
            raw.Slice(blocks << 5).CopyTo(padded);
            padded.Slice(rest).Clear();
            Span<byte> partial = stackalloc byte[BitWidth * 4];
            writeBlock(padded, partial, 0, 4);

            // At most four bytes per plane: a byte loop is cheaper than a copy call.
            int groups = (rest + 7) >> 3;
            for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
            {
                ReadOnlySpan<byte> source = partial.Slice((wireIndex * 4) + 4 - groups, groups);
                Span<byte> target = destination.Slice(wireIndex * planeStride, groups);
                for (int group = 0; group < source.Length; group++)
                {
                    target[group] = source[group];
                }
            }
        }
    }

    /// <summary>
    /// Writes eight planes of a complete block. Each <see cref="Vector256.ExtractMostSignificantBits{T}"/> call
    /// yields one plane's bit of all 32 elements, element <c>j</c> in bit <c>j</c>, and adding the vector to itself
    /// exposes the next plane. The block's last group has the lowest address, so the mask is stored byte-reversed.
    /// </summary>
    /// <returns>The offset of the block in the next plane.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static int StorePlanes(Span<byte> destination, int offset, Vector256<byte> bytes, int planeStride)
    {
        // One range check covers the eight stores, so the loop itself needs no bounds checks. Each address is
        // computed from the start of the span, so no reference ever points past its end.
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(planeStride);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(offset + (7L * planeStride) + sizeof(uint), destination.Length);
        ref byte start = ref MemoryMarshal.GetReference(destination);
        for (int i = 0; i < 8; i++)
        {
            Unsafe.WriteUnaligned(
                ref Unsafe.Add(ref start, offset + (i * planeStride)),
                BinaryPrimitives.ReverseEndianness(bytes.ExtractMostSignificantBits()));
            bytes += bytes;
        }

        return offset + (8 * planeStride);
    }

    /// <summary>
    /// Writes the 32 planes of a complete block from 32 lanes of 32 bits, most significant bit first.
    /// </summary>
    /// <returns>The offset of the block in the plane after the last.</returns>
    protected static int StoreWords(
        Span<byte> destination,
        int offset,
        int planeStride,
        Vector256<uint> lanes0,
        Vector256<uint> lanes8,
        Vector256<uint> lanes16,
        Vector256<uint> lanes24)
    {
        (Vector256<byte> byte3, Vector256<byte> byte2) = HighBytes(lanes0, lanes8, lanes16, lanes24);
        (Vector256<byte> byte1, Vector256<byte> byte0) = LowBytes(lanes0, lanes8, lanes16, lanes24);
        offset = StorePlanes(destination, offset, byte3, planeStride);
        offset = StorePlanes(destination, offset, byte2, planeStride);
        offset = StorePlanes(destination, offset, byte1, planeStride);
        return StorePlanes(destination, offset, byte0, planeStride);
    }

    /// <summary>Returns bytes 3 and 2 of 32 lanes of 32 bits, in lane order.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static (Vector256<byte> Byte3, Vector256<byte> Byte2) HighBytes(
        Vector256<uint> lanes0, Vector256<uint> lanes8, Vector256<uint> lanes16, Vector256<uint> lanes24)
    {
        Vector256<ushort> first = Vector256.Narrow(lanes0 >> 16, lanes8 >> 16);
        Vector256<ushort> second = Vector256.Narrow(lanes16 >> 16, lanes24 >> 16);
        return (Vector256.Narrow(first >> 8, second >> 8), Vector256.Narrow(first, second));
    }

    /// <summary>Returns bytes 1 and 0 of 32 lanes of 32 bits, in lane order.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static (Vector256<byte> Byte1, Vector256<byte> Byte0) LowBytes(
        Vector256<uint> lanes0, Vector256<uint> lanes8, Vector256<uint> lanes16, Vector256<uint> lanes24)
    {
        // Narrow truncates, so these keep the low 16 bits of each lane.
        Vector256<ushort> first = Vector256.Narrow(lanes0, lanes8);
        Vector256<ushort> second = Vector256.Narrow(lanes16, lanes24);
        return (Vector256.Narrow(first >> 8, second >> 8), Vector256.Narrow(first, second));
    }

    /// <summary>
    /// Returns a non-null vector with exactly <see cref="Dimension"/> elements.
    /// </summary>
    /// <exception cref="ArgumentException">The vector is null or its length differs from <see cref="Dimension"/>.</exception>
    protected T[] Validate<T>(T[] vector, int row)
    {
        if (vector is null)
        {
            throw new ArgumentException(
                $"A {TypeName} column cannot hold a null vector (at row {row}); wrap the type in Nullable to write nulls.",
                nameof(vector));
        }

        if (vector.Length != Dimension)
        {
            throw new ArgumentException(
                $"A {TypeName} vector at row {row} has {vector.Length} element(s); every vector must have exactly {Dimension}.",
                nameof(vector));
        }

        return vector;
    }

    /// <summary>
    /// Transposes row vectors into a plane-major body. Rows go first into a small buffer laid out like the body but
    /// holding only a block of rows per plane, and each plane's run is then copied into the body. A row writes the
    /// same offset in every plane: in the body those offsets are a plane apart and can share cache and TLB sets,
    /// while the small buffer stays in L1 cache.
    /// </summary>
    private void WriteTransposed(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (length == 0)
        {
            return;
        }

        int planeBytes = checked(length * BytesPerRow);
        int bodyBytes = checked(BitWidth * planeBytes);
        int blockRows = Math.Clamp(TransposeBlockBytes / (BitWidth * BytesPerRow), 1, length);
        byte[] body = ArrayPool<byte>.Shared.Rent(bodyBytes);

        // A slice that fits in one block is transposed straight into the body, which is then as small.
        byte[] block = blockRows < length ? ArrayPool<byte>.Shared.Rent(BitWidth * blockRows * BytesPerRow) : body;
        int blockPlane = blockRows < length ? blockRows * BytesPerRow : planeBytes;
        try
        {
            for (int first = 0; first < length; first += blockRows)
            {
                int rows = Math.Min(blockRows, length - first);
                for (int r = 0; r < rows; r++)
                {
                    TransposeRow(column, start + first + r, block.AsSpan(r * BytesPerRow), blockPlane);
                }

                if (block != body)
                {
                    int run = rows * BytesPerRow;
                    for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
                    {
                        block.AsSpan(wireIndex * blockPlane, run).CopyTo(body.AsSpan((wireIndex * planeBytes) + (first * BytesPerRow)));
                    }
                }
            }

            writer.WriteBytes(body.AsSpan(0, bodyBytes));
        }
        finally
        {
            if (block != body)
            {
                ArrayPool<byte>.Shared.Return(block);
            }

            ArrayPool<byte>.Shared.Return(body);
        }
    }

    /// <summary>Writes one 32-element block; see <see cref="TransposeBlocks{T}"/> for <paramref name="offset"/>.</summary>
    protected delegate void BlockWriter<T>(ReadOnlySpan<T> block, Span<byte> destination, int offset, int planeStride);
}

/// <summary>
/// Handles <c>QBit(Int8, N)</c> as eight most-significant-first planes over each element's two's-complement byte.
/// </summary>
internal sealed class QBitSByteColumnCodec : QBitColumnCodec
{
    private sbyte[] nullPlaceholder;

    public QBitSByteColumnCodec(string typeName, int dimension)
        : base(typeName, dimension, bitWidth: 8)
    {
    }

    public override Type ElementType => typeof(sbyte[]);

    /// <summary>Gets the all-zero vector used for null positions in a nullable column.</summary>
    public override object NullPlaceholder => nullPlaceholder ??= new sbyte[Dimension];

    public override bool CanWrite(IColumn column) => column is IColumn<sbyte[]>;

    protected override IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled)
        => new QBitSByteColumn(name, typeName, Dimension, blob, rowCount, pooled);

    protected override void TransposeRow(IColumn column, int row, Span<byte> destination, int planeStride)
    {
        sbyte[] vector = Validate(((IColumn<sbyte[]>)column)[row], row);

        // Use the element's two's-complement bit pattern.
        ReadOnlySpan<byte> raw = MemoryMarshal.AsBytes(vector.AsSpan());
        if (QBitLayout.TransposesBlocks)
        {
            TransposeBlocks(raw, destination, planeStride, WriteBlock);
        }
        else
        {
            TransposeScalar(raw, destination, planeStride);
        }
    }

    private static void WriteBlock(ReadOnlySpan<byte> block, Span<byte> destination, int offset, int planeStride)
        => StorePlanes(destination, offset, Vector256.Create(block), planeStride);
}

/// <summary>
/// Handles <c>QBit(Float32, N)</c> and <c>QBit(BFloat16, N)</c> as <see cref="float"/> arrays. <c>BFloat16</c>
/// retains only the high 16 bits of each value.
/// </summary>
internal sealed class QBitFloatColumnCodec : QBitColumnCodec
{
    private readonly BlockWriter<uint> writeBlock;
    private float[] nullPlaceholder;

    public QBitFloatColumnCodec(string typeName, int dimension, int bitWidth)
        : base(typeName, dimension, bitWidth)
    {
        writeBlock = bitWidth == 16 ? WriteBFloat16Block : WriteFloat32Block;
    }

    public override Type ElementType => typeof(float[]);

    /// <summary>Gets the lazily allocated all-zero vector used for null positions in a nullable column.</summary>
    public override object NullPlaceholder => nullPlaceholder ??= new float[Dimension];

    public override bool CanWrite(IColumn column) => column is IColumn<float[]>;

    protected override IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled)
        => new QBitFloatColumn(name, typeName, Dimension, BitWidth, blob, rowCount, pooled);

    protected override void TransposeRow(IColumn column, int row, Span<byte> destination, int planeStride)
    {
        float[] vector = Validate(((IColumn<float[]>)column)[row], row);

        // BFloat16 planes are the high 16 bits of the widened float.
        ReadOnlySpan<uint> raw = MemoryMarshal.Cast<float, uint>(vector.AsSpan());
        if (QBitLayout.TransposesBlocks)
        {
            TransposeBlocks(raw, destination, planeStride, writeBlock);
        }
        else
        {
            TransposeScalar(raw, destination, planeStride);
        }
    }

    private static void WriteFloat32Block(ReadOnlySpan<uint> block, Span<byte> destination, int offset, int planeStride)
        => StoreWords(
            destination,
            offset,
            planeStride,
            Vector256.Create(block),
            Vector256.Create(block.Slice(8)),
            Vector256.Create(block.Slice(16)),
            Vector256.Create(block.Slice(24)));

    private static void WriteBFloat16Block(ReadOnlySpan<uint> block, Span<byte> destination, int offset, int planeStride)
    {
        (Vector256<byte> byte3, Vector256<byte> byte2) = HighBytes(
            Vector256.Create(block), Vector256.Create(block.Slice(8)), Vector256.Create(block.Slice(16)), Vector256.Create(block.Slice(24)));
        offset = StorePlanes(destination, offset, byte3, planeStride);
        StorePlanes(destination, offset, byte2, planeStride);
    }
}

/// <summary>Handles <c>QBit(Float64, N)</c> as 64 planes over each element's IEEE-754 bit pattern.</summary>
internal sealed class QBitDoubleColumnCodec : QBitColumnCodec
{
    private double[] nullPlaceholder;

    public QBitDoubleColumnCodec(string typeName, int dimension)
        : base(typeName, dimension, bitWidth: 64)
    {
    }

    public override Type ElementType => typeof(double[]);

    /// <summary>Gets the all-zero vector used for null positions in a nullable column.</summary>
    public override object NullPlaceholder => nullPlaceholder ??= new double[Dimension];

    public override bool CanWrite(IColumn column) => column is IColumn<double[]>;

    protected override IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled)
        => new QBitDoubleColumn(name, typeName, Dimension, blob, rowCount, pooled);

    protected override void TransposeRow(IColumn column, int row, Span<byte> destination, int planeStride)
    {
        double[] vector = Validate(((IColumn<double[]>)column)[row], row);
        ReadOnlySpan<ulong> raw = MemoryMarshal.Cast<double, ulong>(vector.AsSpan());
        if (QBitLayout.TransposesBlocks)
        {
            TransposeBlocks(raw, destination, planeStride, WriteBlock);
        }
        else
        {
            TransposeScalar(raw, destination, planeStride);
        }
    }

    private static void WriteBlock(ReadOnlySpan<ulong> block, Span<byte> destination, int offset, int planeStride)
    {
        // Split each element into its high and low 32 bits, then each half into bytes.
        Vector256<ulong> lanes0 = Vector256.Create(block);
        Vector256<ulong> lanes4 = Vector256.Create(block.Slice(4));
        Vector256<ulong> lanes8 = Vector256.Create(block.Slice(8));
        Vector256<ulong> lanes12 = Vector256.Create(block.Slice(12));
        Vector256<ulong> lanes16 = Vector256.Create(block.Slice(16));
        Vector256<ulong> lanes20 = Vector256.Create(block.Slice(20));
        Vector256<ulong> lanes24 = Vector256.Create(block.Slice(24));
        Vector256<ulong> lanes28 = Vector256.Create(block.Slice(28));
        Vector256<uint> high0 = Vector256.Narrow(lanes0 >> 32, lanes4 >> 32);
        Vector256<uint> high8 = Vector256.Narrow(lanes8 >> 32, lanes12 >> 32);
        Vector256<uint> high16 = Vector256.Narrow(lanes16 >> 32, lanes20 >> 32);
        Vector256<uint> high24 = Vector256.Narrow(lanes24 >> 32, lanes28 >> 32);
        Vector256<uint> low0 = Vector256.Narrow(lanes0, lanes4);
        Vector256<uint> low8 = Vector256.Narrow(lanes8, lanes12);
        Vector256<uint> low16 = Vector256.Narrow(lanes16, lanes20);
        Vector256<uint> low24 = Vector256.Narrow(lanes24, lanes28);

        offset = StoreWords(destination, offset, planeStride, high0, high8, high16, high24);
        StoreWords(destination, offset, planeStride, low0, low8, low16, low24);
    }
}
