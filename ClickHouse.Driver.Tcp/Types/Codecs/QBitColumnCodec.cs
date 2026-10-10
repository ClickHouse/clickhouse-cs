using System;
using System.Buffers;
using System.Globalization;
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

    /// <summary>The number of elements of each vector.</summary>
    internal int Dimension { get; }

    /// <summary>The number of bit planes: the number of bits of the element type.</summary>
    internal int BitWidth { get; }

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

    /// <inheritdoc/>
    // The decoded column of the same layout: the same element type, dimension and plane grouping. Equal body sizes do
    // not imply equal layouts.
    public bool CanWrite(IColumn column)
        => column is QBitColumn dense
            && dense.Dimension == Dimension
            && dense.BitWidth == BitWidth
            && dense.Stride == Stride
            && column.ElementType == ElementType;

    /// <inheritdoc/>
    // A row range is contiguous within each plane, but planes are spaced by the source column's full row count, so
    // each plane is one copy.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var dense = (QBitColumn)column;
        for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
        {
            writer.WriteBytes(dense.WirePlane(wireIndex, start, length));
        }
    }

    protected abstract IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled);
}

/// <summary>
/// Handles <c>QBit(Int8, N)</c> as eight most-significant-first planes over each element's two's-complement byte.
/// </summary>
internal sealed class QBitSByteColumnCodec : QBitColumnCodec
{
    public QBitSByteColumnCodec(string typeName, int dimension)
        : base(typeName, dimension, bitWidth: 8)
    {
    }

    public override Type ElementType => typeof(sbyte[]);

    protected override IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled)
        => new QBitSByteColumn(name, typeName, Dimension, blob, rowCount, pooled);
}

/// <summary>
/// Handles <c>QBit(Float32, N)</c> and <c>QBit(BFloat16, N)</c> as <see cref="float"/> arrays. <c>BFloat16</c>
/// retains only the high 16 bits of each value.
/// </summary>
internal sealed class QBitFloatColumnCodec : QBitColumnCodec
{
    public QBitFloatColumnCodec(string typeName, int dimension, int bitWidth)
        : base(typeName, dimension, bitWidth)
    {
    }

    public override Type ElementType => typeof(float[]);

    protected override IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled)
        => new QBitFloatColumn(name, typeName, Dimension, BitWidth, blob, rowCount, pooled);
}

/// <summary>Handles <c>QBit(Float64, N)</c> as 64 planes over each element's IEEE-754 bit pattern.</summary>
internal sealed class QBitDoubleColumnCodec : QBitColumnCodec
{
    public QBitDoubleColumnCodec(string typeName, int dimension)
        : base(typeName, dimension, bitWidth: 64)
    {
    }

    public override Type ElementType => typeof(double[]);

    protected override IColumn CreateColumn(string name, string typeName, byte[] blob, int rowCount, bool pooled)
        => new QBitDoubleColumn(name, typeName, Dimension, blob, rowCount, pooled);
}
