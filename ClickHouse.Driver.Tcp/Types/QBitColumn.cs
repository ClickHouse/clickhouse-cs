using System;
using System.Buffers;
using System.Runtime.InteropServices;

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

    protected ReadOnlySpan<byte> WirePlaneRow(int wireIndex, int row) => WirePlane(wireIndex, row, 1);

    protected void CheckRow(int row)
    {
        if ((uint)row >= (uint)rowCount)
        {
            throw new IndexOutOfRangeException();
        }
    }
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

    protected override float[] DetransposeRow(int row)
    {
        CheckRow(row);

        var vector = new float[Dimension];

        // Build each IEEE-754 pattern directly in the destination array.
        Span<uint> bits = MemoryMarshal.Cast<float, uint>(vector.AsSpan());
        for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
        {
            ReadOnlySpan<byte> plane = WirePlaneRow(wireIndex, row);
            uint mask = 1u << (BitWidth - 1 - wireIndex);
            for (int i = 0; i < bits.Length; i++)
            {
                if ((plane[QBitLayout.ByteOfGroup(i >> 3, BytesPerRow)] & (1 << (i & 7))) != 0)
                {
                    bits[i] |= mask;
                }
            }
        }

        // BFloat16 occupies the high half of the widened float.
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
        CheckRow(row);

        var vector = new sbyte[Dimension];

        // Use an unsigned view so the sign plane sets bit 7 without numeric conversion.
        Span<byte> bits = MemoryMarshal.Cast<sbyte, byte>(vector.AsSpan());
        for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
        {
            ReadOnlySpan<byte> plane = WirePlaneRow(wireIndex, row);
            byte mask = (byte)(1 << (BitWidth - 1 - wireIndex));
            for (int i = 0; i < bits.Length; i++)
            {
                if ((plane[QBitLayout.ByteOfGroup(i >> 3, BytesPerRow)] & (1 << (i & 7))) != 0)
                {
                    bits[i] |= mask;
                }
            }
        }

        return vector;
    }
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
        CheckRow(row);

        var vector = new double[Dimension];
        Span<ulong> bits = MemoryMarshal.Cast<double, ulong>(vector.AsSpan());
        for (int wireIndex = 0; wireIndex < BitWidth; wireIndex++)
        {
            ReadOnlySpan<byte> plane = WirePlaneRow(wireIndex, row);
            ulong mask = 1UL << (BitWidth - 1 - wireIndex);
            for (int i = 0; i < bits.Length; i++)
            {
                if ((plane[QBitLayout.ByteOfGroup(i >> 3, BytesPerRow)] & (1 << (i & 7))) != 0)
                {
                    bits[i] |= mask;
                }
            }
        }

        return vector;
    }
}
