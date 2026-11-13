using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>QBit(X, N)</c> from vectors of the CLR type of <c>X</c>: the bit planes of the elements, most significant plane
/// first. Each plane holds one big-endian <c>ceil(N / 8)</c>-byte bitmap for each vector (<see cref="QBitLayout"/>), so
/// the body is <c>bits(X) * count * ceil(N / 8)</c> bytes. A position that the source marks gets the zero vector.
/// </summary>
/// <typeparam name="T">The CLR type of one element.</typeparam>
internal abstract class QBitWriter<T> : ColumnWriter<T[]>
    where T : unmanaged
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="typeName">The QBit type, for the messages.</param>
    /// <param name="dimension">The number of elements of each vector (<c>N</c>).</param>
    /// <param name="bitWidth">The number of planes (<c>bits(X)</c>).</param>
    protected QBitWriter(string typeName, int dimension, int bitWidth)
    {
        TypeName = typeName;
        Dimension = dimension;
        BitWidth = bitWidth;
        BytesPerRow = QBitLayout.BytesPerRow(dimension);
    }

    /// <summary>The QBit type.</summary>
    protected string TypeName { get; }

    /// <summary>The number of elements of each vector.</summary>
    protected int Dimension { get; }

    /// <summary>The number of planes.</summary>
    protected int BitWidth { get; }

    /// <summary>The number of bytes of the bitmap of one vector in one plane.</summary>
    protected int BytesPerRow { get; }

    /// <inheritdoc/>
    /// <exception cref="ArgumentException">A vector that the source does not mark is null, or does not have <c>N</c> elements.</exception>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<T[]> values, IColumnWriteState state)
    {
        int count = values.Count;
        int byteCount = checked(BitWidth * count * BytesPerRow);
        byte[] scratch = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            // The transposition only sets bits.
            Array.Clear(scratch, 0, byteCount);
            int planeStride = count * BytesPerRow;
            ReadOnlySpan<byte> absent = values.Absent;
            bool marked = values.HasAbsent;
            int position = 0;
            for (int r = 0; r < values.RunCount; r++)
            {
                foreach (T[] vector in values.Run(r))
                {
                    if (!(marked && absent[position] != 0))
                    {
                        Transpose(scratch, Validate(vector, values.FirstRow + position), position * BytesPerRow, planeStride);
                    }

                    position++;
                }
            }

            writer.WriteBytes(scratch.AsSpan(0, byteCount));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    /// <summary>Sets the bits of one vector in every plane.</summary>
    /// <param name="scratch">The planes, cleared before the first vector.</param>
    /// <param name="vector">The vector, with <see cref="Dimension"/> elements.</param>
    /// <param name="rowBase">The offset of the bitmap of the vector in each plane.</param>
    /// <param name="planeStride">The number of bytes of one plane.</param>
    protected abstract void Transpose(byte[] scratch, T[] vector, int rowBase, int planeStride);

    // The parameter name is "vector": the refused value is the vector of a row.
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
    private T[] Validate(T[] vector, int row)
    {
        if (vector is null)
        {
            throw new ArgumentException(
                $"A {TypeName} column cannot hold a null vector (at row {row}); wrap the type in Nullable to write nulls.",
                "vector");
        }

        if (vector.Length != Dimension)
        {
            throw new ArgumentException(
                $"A {TypeName} vector at row {row} has {vector.Length} element(s); every vector must have exactly {Dimension}.",
                "vector");
        }

        return vector;
    }
#pragma warning restore CA2208
}

/// <summary><c>QBit(Int8, N)</c>: eight planes over the two's-complement byte of each element.</summary>
internal sealed class QBitSByteWriter : QBitWriter<sbyte>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="typeName">The QBit type, for the messages.</param>
    /// <param name="dimension">The number of elements of each vector.</param>
    public QBitSByteWriter(string typeName, int dimension)
        : base(typeName, dimension, bitWidth: 8)
    {
    }

    /// <inheritdoc/>
    protected override void Transpose(byte[] scratch, sbyte[] vector, int rowBase, int planeStride)
    {
        for (int i = 0; i < vector.Length; i++)
        {
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
}

/// <summary>
/// <c>QBit(Float32, N)</c> and <c>QBit(BFloat16, N)</c>: the planes over the IEEE-754 bits of each <see cref="float"/>.
/// <c>BFloat16</c> has the 16 high planes.
/// </summary>
internal sealed class QBitFloatWriter : QBitWriter<float>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="typeName">The QBit type, for the messages.</param>
    /// <param name="dimension">The number of elements of each vector.</param>
    /// <param name="bitWidth">32 for <c>Float32</c>, 16 for <c>BFloat16</c>.</param>
    public QBitFloatWriter(string typeName, int dimension, int bitWidth)
        : base(typeName, dimension, bitWidth)
    {
    }

    /// <inheritdoc/>
    protected override void Transpose(byte[] scratch, float[] vector, int rowBase, int planeStride)
    {
        int whole = Vector256.IsHardwareAccelerated ? Dimension >> 3 : 0;
        if (whole != 0)
        {
            TransposeGroups(scratch, vector, whole, rowBase, planeStride);
        }

        // The elements after the whole groups, or every element when the vector path is not available.
        for (int i = whole << 3; i < vector.Length; i++)
        {
            uint raw = BitConverter.SingleToUInt32Bits(vector[i]);
            int slot = QBitLayout.ByteOfGroup(i >> 3, BytesPerRow);
            byte bit = (byte)(1 << (i & 7));
            for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
            {
                if (((raw >> (31 - wireIndex)) & 1) != 0)
                {
                    scratch[(wireIndex * planeStride) + rowBase + slot] |= bit;
                }
            }
        }
    }

    // Each Vector256.ExtractMostSignificantBits gives one plane byte of a group of eight; a shift of the lanes to the
    // left gives the next plane.
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

/// <summary><c>QBit(Float64, N)</c>: 64 planes over the IEEE-754 bits of each <see cref="double"/>.</summary>
internal sealed class QBitDoubleWriter : QBitWriter<double>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="typeName">The QBit type, for the messages.</param>
    /// <param name="dimension">The number of elements of each vector.</param>
    public QBitDoubleWriter(string typeName, int dimension)
        : base(typeName, dimension, bitWidth: 64)
    {
    }

    /// <inheritdoc/>
    protected override void Transpose(byte[] scratch, double[] vector, int rowBase, int planeStride)
    {
        int whole = Vector256.IsHardwareAccelerated ? Dimension >> 3 : 0;
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

    // Each of the two vectors of four lanes gives four bits of one plane byte of a group of eight.
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
