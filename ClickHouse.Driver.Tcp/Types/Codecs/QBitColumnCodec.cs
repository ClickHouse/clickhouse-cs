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
/// A codec for the ClickHouse <c>QBit(T, N)</c> column: an <c>N</c>-element vector stored with its bit planes
/// transposed, so a vector search can read only the high-order planes and compute an approximate distance at
/// reduced precision (<c>L2DistanceTransposed</c>, <c>cosineDistanceTransposed</c>).
///
/// <para>
/// The column carries no state prefix. Its body is <c>bits(T)</c> planes, ordered from the <b>most</b>
/// significant bit of <c>T</c> down to bit 0; each plane holds one <c>ceil(N / 8)</c>-byte bitmap per row, rows
/// contiguous within the plane. Element <c>i</c> sits at bit <c>i % 8</c> of the bitmap byte
/// <see cref="QBitLayout.ByteOfGroup"/> names — the bytes run in the reverse of the element order, so group 0 is
/// the last byte. The body is plane-major and exactly <c>bits(T) * num_rows * ceil(N / 8)</c> bytes — every row
/// the same width.
/// </para>
///
/// <para>
/// <c>T</c> is <c>BFloat16</c>, <c>Float32</c> or <c>Float64</c> only; the server rejects any other element
/// type. Note this is the <c>Native</c> layout: over <c>RowBinary</c> the same type is a plain array, which is
/// why the HTTP driver's <c>QBitType</c> reads a length-prefixed run of values instead.
/// </para>
/// </summary>
internal abstract class QBitColumnCodec : IColumnCodec
{
    /// <summary>Initializes the shared geometry.</summary>
    /// <param name="typeName">The canonical type string.</param>
    /// <param name="dimension">The vector length <c>N</c>.</param>
    /// <param name="bitWidth">The stored element's bit width — the number of planes.</param>
    protected QBitColumnCodec(string typeName, int dimension, int bitWidth)
    {
        TypeName = typeName;
        Dimension = dimension;
        BitWidth = bitWidth;
        BytesPerRow = (dimension + 7) / 8;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public abstract Type ElementType { get; }

    /// <inheritdoc/>
    public abstract object NullPlaceholder { get; }

    /// <summary>The vector length <c>N</c>.</summary>
    protected int Dimension { get; }

    /// <summary>The number of bit planes — the stored element's bit width.</summary>
    protected int BitWidth { get; }

    /// <summary>The bytes one row occupies within a single plane, <c>ceil(N / 8)</c>.</summary>
    protected int BytesPerRow { get; }

    /// <summary>Builds a <c>QBit(T, N)</c> codec from its element type and dimension arguments.</summary>
    /// <param name="node">The parsed <c>QBit</c> type node.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The type does not have exactly one element type and one positive integer dimension.</exception>
    /// <exception cref="NotSupportedException">The element type is not one the server allows.</exception>
    public static QBitColumnCodec Create(TypeNode node)
    {
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

        // The same three the server allows; anything else is rejected at CREATE TABLE, so a column of one can
        // only reach us from a server that has changed, not from a table a user could have made today.
        return element switch
        {
            "BFloat16" => new QBitFloatColumnCodec(typeName, dimension, bitWidth: 16),
            "Float32" => new QBitFloatColumnCodec(typeName, dimension, bitWidth: 32),
            "Float64" => new QBitDoubleColumnCodec(typeName, dimension),
            _ => throw new NotSupportedException(
                $"QBit type '{node}' has element type '{element}'; only BFloat16, Float32 and Float64 are supported."),
        };
    }

    /// <inheritdoc/>
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
            // The column never took ownership of the rent, so return it rather than leak it on a read failure.
            ArrayPool<byte>.Shared.Return(blob);
            throw;
        }

        return CreateColumn(columnName, columnType, blob, rowCount, pooled: true);
    }

    /// <inheritdoc/>
    public abstract bool CanWrite(IColumn column);

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // A QBit column of the same geometry already holds the planes the wire wants, so the range is copied out
        // plane by plane with no transposition — the hot path when a column read from the server is inserted
        // straight back. One copy per plane rather than one for the whole range: the body is plane-major, so a
        // row range is contiguous *within* a plane but the planes themselves are strided by the source's own row
        // count, which is not this range's length unless the whole column is being written.
        if (column is QBitColumn dense && dense.Dimension == Dimension && dense.BitWidth == BitWidth)
        {
            for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
            {
                writer.WriteBytes(dense.WirePlane(wireIndex, start, length));
            }

            return;
        }

        WriteTransposed(writer, column, start, length);
    }

    /// <summary>Builds the decoded column over a plane blob.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string from the block header.</param>
    /// <param name="blob">The plane blob.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooled">Whether <paramref name="blob"/> was rented.</param>
    /// <returns>The column.</returns>
    protected abstract IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled);

    /// <summary>
    /// Transposes an ergonomic per-row vector column into the plane-major body. Rents a scratch the size of the
    /// slice's wire bytes, because plane-major output cannot be streamed a row at a time: plane 0 needs every row
    /// before plane 1 begins. The dense path above avoids this entirely.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column to transpose.</param>
    /// <param name="start">The zero-based first row to write.</param>
    /// <param name="length">The number of rows to write.</param>
    protected abstract void WriteTransposed(ClickHouseBinaryWriter writer, IColumn column, int start, int length);

    /// <summary>
    /// Rents a zeroed scratch buffer for the slice's plane-major body. Rented memory is dirty and the transpose
    /// only ever sets bits, so the used region must be cleared first.
    /// </summary>
    /// <param name="length">The number of rows the slice covers.</param>
    /// <param name="byteCount">The used size of the returned buffer.</param>
    /// <returns>The rented buffer, zeroed over <paramref name="byteCount"/> bytes.</returns>
    protected byte[] RentScratch(int length, out int byteCount)
    {
        byteCount = checked(BitWidth * length * BytesPerRow);
        byte[] scratch = ArrayPool<byte>.Shared.Rent(byteCount);
        Array.Clear(scratch, 0, byteCount);
        return scratch;
    }

    /// <summary>
    /// Validates one row's vector and returns it, blaming the row when it is null or the wrong length. A QBit row
    /// is never null on the wire — <c>Nullable</c> carries that and substitutes the placeholder at a null
    /// position — and the vector length is fixed by the type, so neither can be silently padded.
    /// </summary>
    /// <param name="vector">The row's vector.</param>
    /// <param name="row">The row index, for the message.</param>
    /// <returns>The validated vector.</returns>
    /// <exception cref="ArgumentException">The vector is null or not <see cref="Dimension"/> elements.</exception>
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
/// The <c>QBit(Float32, N)</c> and <c>QBit(BFloat16, N)</c> codec. Both surface as <see cref="float"/>[]: a
/// brain-float is the top 16 bits of an IEEE-754 <see cref="float"/>, so on write the low 16 bits are dropped —
/// the same narrowing <see cref="BFloat16ColumnCodec"/> does for a plain <c>BFloat16</c> column.
/// </summary>
internal sealed class QBitFloatColumnCodec : QBitColumnCodec
{
    private float[] nullPlaceholder;

    /// <summary>Initializes the single-precision codec.</summary>
    /// <param name="typeName">The canonical type string.</param>
    /// <param name="dimension">The vector length <c>N</c>.</param>
    /// <param name="bitWidth">16 for <c>BFloat16</c>, 32 for <c>Float32</c>.</param>
    public QBitFloatColumnCodec(string typeName, int dimension, int bitWidth)
        : base(typeName, dimension, bitWidth)
    {
    }

    /// <inheritdoc/>
    public override Type ElementType => typeof(float[]);

    /// <summary>
    /// The placeholder for a null row is an all-zero vector, so the values stream stays aligned at a
    /// <c>Nullable(QBit(T, N))</c> null position — the width every row occupies. Built on first use: a codec is
    /// resolved per column per block, so a pure read would otherwise allocate a vector per block that only the
    /// Nullable write path ever touches.
    /// </summary>
    public override object NullPlaceholder => nullPlaceholder ??= new float[Dimension];

    /// <inheritdoc/>
    public override bool CanWrite(IColumn column) => column is IColumn<float[]>;

    /// <inheritdoc/>
    protected override IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled)
        => new QBitFloatColumn(name, typeName, Dimension, BitWidth, blob, rowCount, pooled);

    /// <inheritdoc/>
    protected override void WriteTransposed(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var typed = (IColumn<float[]>)column;
        byte[] scratch = RentScratch(length, out int byteCount);
        try
        {
            // Hoisted out of the row loop, as UuidColumnCodec does: the answer is the same for every row.
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

                // The elements past the last whole group of 8, and every element when there is no hardware
                // acceleration. They occupy byte `whole` of the row, which the vector path never writes, so the
                // two cannot collide.
                for (int i = whole << 3; i < vector.Length; i++)
                {
                    uint raw = BitConverter.SingleToUInt32Bits(vector[i]);
                    int slot = QBitLayout.ByteOfGroup(i >> 3, BytesPerRow);
                    byte bit = (byte)(1 << (i & 7));
                    for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
                    {
                        // Plane `wireIndex` is bit 31 - wireIndex of the float, for a brain-float too: its 16
                        // bits *are* the float's high half, so its planes are the float's top 16.
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
    /// Transposes the first <paramref name="whole"/> groups of 8 elements of one row.
    /// <see cref="Vector256{T}.ExtractMostSignificantBits"/> gathers the top bit of 8 lanes into a byte, which
    /// <em>is</em> one plane byte for 8 <see cref="float"/>s in the order the wire wants (element <c>i</c> at bit
    /// <c>i</c>), so a plane costs one extract plus one shift rather than 8 test-and-sets. Walking the planes
    /// most significant first is then just shifting the vector left one bit each step.
    /// </summary>
    /// <param name="scratch">The zeroed plane-major slice buffer.</param>
    /// <param name="vector">The row's vector.</param>
    /// <param name="whole">The number of complete 8-element groups.</param>
    /// <param name="rowBase">The row's byte offset within a plane.</param>
    /// <param name="planeStride">The bytes one plane occupies for the whole slice.</param>
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

/// <summary>The <c>QBit(Float64, N)</c> codec: 64 planes over each element's IEEE-754 double pattern.</summary>
internal sealed class QBitDoubleColumnCodec : QBitColumnCodec
{
    private double[] nullPlaceholder;

    /// <summary>Initializes the double-precision codec.</summary>
    /// <param name="typeName">The canonical type string.</param>
    /// <param name="dimension">The vector length <c>N</c>.</param>
    public QBitDoubleColumnCodec(string typeName, int dimension)
        : base(typeName, dimension, bitWidth: 64)
    {
    }

    /// <inheritdoc/>
    public override Type ElementType => typeof(double[]);

    /// <summary>The all-zero placeholder vector; see <see cref="QBitFloatColumnCodec.NullPlaceholder"/>.</summary>
    public override object NullPlaceholder => nullPlaceholder ??= new double[Dimension];

    /// <inheritdoc/>
    public override bool CanWrite(IColumn column) => column is IColumn<double[]>;

    /// <inheritdoc/>
    protected override IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled)
        => new QBitDoubleColumn(name, typeName, Dimension, blob, rowCount, pooled);

    /// <inheritdoc/>
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
    /// Transposes the first <paramref name="whole"/> groups of 8 elements of one row. A
    /// <see cref="Vector256{T}"/> of <see cref="double"/> holds only 4 lanes, so a plane byte takes two extracts
    /// — the low group in bits 3..0 and the high group in bits 7..4 — against the single extract the
    /// <see cref="float"/> path needs.
    /// </summary>
    /// <param name="scratch">The zeroed plane-major slice buffer.</param>
    /// <param name="vector">The row's vector.</param>
    /// <param name="whole">The number of complete 8-element groups.</param>
    /// <param name="rowBase">The row's byte offset within a plane.</param>
    /// <param name="planeStride">The bytes one plane occupies for the whole slice.</param>
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
