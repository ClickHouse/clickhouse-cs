using System;
using System.Buffers;
using System.Globalization;
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

        return element switch
        {
            "Int8" => new QBitSByteColumnCodec(typeName, dimension),
            "BFloat16" => new QBitFloatColumnCodec(typeName, dimension, bitWidth: 16),
            "Float32" => new QBitFloatColumnCodec(typeName, dimension, bitWidth: 32),
            "Float64" => new QBitDoubleColumnCodec(typeName, dimension),
            _ => throw new NotSupportedException(
                $"QBit type '{node}' has element type '{element}'; only Int8, BFloat16, Float32 and Float64 are supported."),
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

    /// <summary>Transposes row vectors into a plane-major body.</summary>
    protected abstract void WriteTransposed(ClickHouseBinaryWriter writer, IColumn column, int start, int length);

    /// <summary>
    /// Rents scratch space and clears its used region because transpose implementations only set bits.
    /// </summary>
    protected byte[] RentScratch(int length, out int byteCount)
    {
        byteCount = checked(BitWidth * length * BytesPerRow);
        byte[] scratch = ArrayPool<byte>.Shared.Rent(byteCount);
        Array.Clear(scratch, 0, byteCount);
        return scratch;
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

    protected override void WriteTransposed(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var typed = (IColumn<sbyte[]>)column;
        byte[] scratch = RentScratch(length, out int byteCount);
        try
        {
            int planeStride = length * BytesPerRow;
            for (int r = 0; r < length; r++)
            {
                sbyte[] vector = Validate(typed[start + r], start + r);
                int rowBase = r * BytesPerRow;
                for (int i = 0; i < vector.Length; i++)
                {
                    // Preserve the element's two's-complement bit pattern.
                    uint raw = unchecked((byte)vector[i]);
                    int slot = QBitLayout.ByteOfGroup(i >> 3, BytesPerRow);
                    byte bit = (byte)(1 << (i & 7));
                    for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
                    {
                        if (((raw >> (7 - wireIndex)) & 1) != 0)
                        {
                            scratch[(wireIndex * planeStride) + rowBase + slot] |= bit;
                        }
                    }
                }
            }

            writer.WriteBytes(scratch.AsSpan(0, byteCount));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }
}

/// <summary>
/// Handles <c>QBit(Float32, N)</c> and <c>QBit(BFloat16, N)</c> as <see cref="float"/> arrays. <c>BFloat16</c>
/// retains only the high 16 bits of each value.
/// </summary>
internal sealed class QBitFloatColumnCodec : QBitColumnCodec
{
    private float[] nullPlaceholder;

    public QBitFloatColumnCodec(string typeName, int dimension, int bitWidth)
        : base(typeName, dimension, bitWidth)
    {
    }

    public override Type ElementType => typeof(float[]);

    /// <summary>Gets the lazily allocated all-zero vector used for null positions in a nullable column.</summary>
    public override object NullPlaceholder => nullPlaceholder ??= new float[Dimension];

    public override bool CanWrite(IColumn column) => column is IColumn<float[]>;

    protected override IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled)
        => new QBitFloatColumn(name, typeName, Dimension, BitWidth, blob, rowCount, pooled);

    protected override void WriteTransposed(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var typed = (IColumn<float[]>)column;
        byte[] scratch = RentScratch(length, out int byteCount);
        try
        {
            bool simd = Vector256.IsHardwareAccelerated;
            int planeStride = length * BytesPerRow;
            for (int r = 0; r < length; r++)
            {
                float[] vector = Validate(typed[start + r], start + r);
                int rowBase = (r * BytesPerRow);
                int whole = simd ? Dimension >> 3 : 0;

                if (whole != 0)
                {
                    TransposeGroups(scratch, vector, whole, rowBase, planeStride);
                }

                // Handle the tail, or the entire vector when SIMD is unavailable.
                for (int i = whole << 3; i < vector.Length; i++)
                {
                    uint raw = BitConverter.SingleToUInt32Bits(vector[i]);
                    int slot = QBitLayout.ByteOfGroup(i >> 3, BytesPerRow);
                    byte bit = (byte)(1 << (i & 7));
                    for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
                    {
                        // BFloat16 planes are the high 16 bits of the widened float.
                        if (((raw >> (31 - wireIndex)) & 1) != 0)
                        {
                            scratch[(wireIndex * planeStride) + rowBase + slot] |= bit;
                        }
                    }
                }
            }

            writer.WriteBytes(scratch.AsSpan(0, byteCount));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    /// <summary>
    /// Transposes complete eight-element groups. Each <see cref="Vector256{T}.ExtractMostSignificantBits"/> call
    /// produces one plane byte; shifting the lanes left exposes the next plane.
    /// </summary>
    private void TransposeGroups(byte[] scratch, float[] vector, int whole, int rowBase, int planeStride)
    {
        ref uint source = ref Unsafe.As<float, uint>(ref MemoryMarshal.GetArrayDataReference(vector));
        for (int group = 0; group < whole; group++)
        {
            Vector256<uint> lanes = Vector256.LoadUnsafe(ref source, (nuint)(group << 3));
            int slot = rowBase + QBitLayout.ByteOfGroup(group, BytesPerRow);
            for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
            {
                scratch[(wireIndex * planeStride) + slot] = (byte)lanes.ExtractMostSignificantBits();
                lanes <<= 1;
            }
        }
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

    protected override void WriteTransposed(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var typed = (IColumn<double[]>)column;
        byte[] scratch = RentScratch(length, out int byteCount);
        try
        {
            bool simd = Vector256.IsHardwareAccelerated;
            int planeStride = length * BytesPerRow;
            for (int r = 0; r < length; r++)
            {
                double[] vector = Validate(typed[start + r], start + r);
                int rowBase = r * BytesPerRow;
                int whole = simd ? Dimension >> 3 : 0;

                if (whole != 0)
                {
                    TransposeGroups(scratch, vector, whole, rowBase, planeStride);
                }

                for (int i = whole << 3; i < vector.Length; i++)
                {
                    ulong raw = BitConverter.DoubleToUInt64Bits(vector[i]);
                    int slot = QBitLayout.ByteOfGroup(i >> 3, BytesPerRow);
                    byte bit = (byte)(1 << (i & 7));
                    for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
                    {
                        if (((raw >> (63 - wireIndex)) & 1) != 0)
                        {
                            scratch[(wireIndex * planeStride) + rowBase + slot] |= bit;
                        }
                    }
                }
            }

            writer.WriteBytes(scratch.AsSpan(0, byteCount));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    /// <summary>
    /// Transposes complete eight-element groups. Each four-lane vector contributes one nibble to a plane byte.
    /// </summary>
    private void TransposeGroups(byte[] scratch, double[] vector, int whole, int rowBase, int planeStride)
    {
        ref ulong source = ref Unsafe.As<double, ulong>(ref MemoryMarshal.GetArrayDataReference(vector));
        for (int group = 0; group < whole; group++)
        {
            Vector256<ulong> low = Vector256.LoadUnsafe(ref source, (nuint)(group << 3));
            Vector256<ulong> high = Vector256.LoadUnsafe(ref source, (nuint)((group << 3) + 4));
            int slot = rowBase + QBitLayout.ByteOfGroup(group, BytesPerRow);
            for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
            {
                uint bits = low.ExtractMostSignificantBits() | (high.ExtractMostSignificantBits() << 4);
                scratch[(wireIndex * planeStride) + slot] = (byte)bits;
                low <<= 1;
                high <<= 1;
            }
        }
    }
}
