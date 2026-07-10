using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;

namespace ClickHouse.Driver.Tcp.Numerics;

/// <summary>
/// An unsigned 256-bit integer, the CLR representation of ClickHouse <c>UInt256</c>.
/// Stored as four little-endian 64-bit limbs. The BCL has no native 256-bit type, so this
/// fills the gap (128-bit uses <see cref="UInt128"/>).
/// </summary>
public readonly struct UInt256 : IEquatable<UInt256>, IComparable<UInt256>
{
    /// <summary>The number of bytes in the little-endian wire representation.</summary>
    public const int Size = 32;

    // Limbs, least-significant first.
    private readonly ulong e0;
    private readonly ulong e1;
    private readonly ulong e2;
    private readonly ulong e3;

    /// <summary>Initializes a new instance from four 64-bit limbs, least-significant first.</summary>
    public UInt256(ulong e0, ulong e1, ulong e2, ulong e3)
    {
        this.e0 = e0;
        this.e1 = e1;
        this.e2 = e2;
        this.e3 = e3;
    }

    /// <summary>The zero value.</summary>
    public static UInt256 Zero => default;

    /// <summary>Reads a little-endian <see cref="UInt256"/> from a 32-byte span.</summary>
    public static UInt256 ReadLittleEndian(ReadOnlySpan<byte> source)
    {
        if (source.Length < Size)
        {
            throw new ArgumentException($"Need at least {Size} bytes.", nameof(source));
        }

        return new UInt256(
            BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(0, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(8, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(16, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(24, 8)));
    }

    /// <summary>Writes this value little-endian into a 32-byte span.</summary>
    public void WriteLittleEndian(Span<byte> destination)
    {
        if (destination.Length < Size)
        {
            throw new ArgumentException($"Need at least {Size} bytes.", nameof(destination));
        }

        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(0, 8), e0);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(8, 8), e1);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(16, 8), e2);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(24, 8), e3);
    }

    /// <summary>Converts this value to a non-negative <see cref="BigInteger"/>.</summary>
    public BigInteger ToBigInteger()
        => ((BigInteger)e3 << 192) | ((BigInteger)e2 << 128) | ((BigInteger)e1 << 64) | e0;

    /// <summary>Creates a <see cref="UInt256"/> from a non-negative <see cref="BigInteger"/>.</summary>
    public static UInt256 FromBigInteger(BigInteger value)
    {
        if (value.Sign < 0)
        {
            throw new OverflowException("UInt256 cannot represent a negative value.");
        }

        if (value > (BigInteger.One << 256) - 1)
        {
            throw new OverflowException("Value does not fit in UInt256.");
        }

        const ulong mask = ulong.MaxValue;
        return new UInt256(
            (ulong)(value & mask),
            (ulong)((value >> 64) & mask),
            (ulong)((value >> 128) & mask),
            (ulong)((value >> 192) & mask));
    }

    /// <inheritdoc/>
    public bool Equals(UInt256 other) => e0 == other.e0 && e1 == other.e1 && e2 == other.e2 && e3 == other.e3;

    /// <inheritdoc/>
    public override bool Equals(object obj) => obj is UInt256 other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(e0, e1, e2, e3);

    /// <inheritdoc/>
    public int CompareTo(UInt256 other)
    {
        if (e3 != other.e3)
        {
            return e3 < other.e3 ? -1 : 1;
        }

        if (e2 != other.e2)
        {
            return e2 < other.e2 ? -1 : 1;
        }

        if (e1 != other.e1)
        {
            return e1 < other.e1 ? -1 : 1;
        }

        if (e0 != other.e0)
        {
            return e0 < other.e0 ? -1 : 1;
        }

        return 0;
    }

    /// <inheritdoc/>
    public override string ToString() => ToBigInteger().ToString(CultureInfo.InvariantCulture);

    public static bool operator ==(UInt256 left, UInt256 right) => left.Equals(right);

    public static bool operator !=(UInt256 left, UInt256 right) => !left.Equals(right);

    public static bool operator <(UInt256 left, UInt256 right) => left.CompareTo(right) < 0;

    public static bool operator >(UInt256 left, UInt256 right) => left.CompareTo(right) > 0;

    public static bool operator <=(UInt256 left, UInt256 right) => left.CompareTo(right) <= 0;

    public static bool operator >=(UInt256 left, UInt256 right) => left.CompareTo(right) >= 0;
}
