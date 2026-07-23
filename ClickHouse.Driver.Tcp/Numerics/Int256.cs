using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Runtime.InteropServices;

namespace ClickHouse.Driver.Tcp.Numerics;

/// <summary>
/// A signed 256-bit integer (two's complement), the CLR representation of ClickHouse <c>Int256</c>.
/// Stored as four little-endian 64-bit limbs.
/// </summary>
[StructLayout(LayoutKind.Sequential)] // Necessary to match byte order on the wire
public readonly struct Int256 : IEquatable<Int256>, IComparable<Int256>
{
    /// <summary>The number of bytes in the little-endian wire representation.</summary>
    public const int Size = 32;

    private const ulong SignBit = 0x8000_0000_0000_0000UL;

    // Limbs, least-significant first; e3 holds the sign bit.
    private readonly ulong e0;
    private readonly ulong e1;
    private readonly ulong e2;
    private readonly ulong e3;

    /// <summary>Initializes a new instance from four 64-bit limbs, least-significant first.</summary>
    public Int256(ulong e0, ulong e1, ulong e2, ulong e3)
    {
        this.e0 = e0;
        this.e1 = e1;
        this.e2 = e2;
        this.e3 = e3;
    }

    /// <summary>The zero value.</summary>
    public static Int256 Zero => default;

    /// <summary>True when the value is negative.</summary>
    public bool IsNegative => (e3 & SignBit) != 0;

    /// <summary>Reads a little-endian <see cref="Int256"/> from a 32-byte span.</summary>
    public static Int256 ReadLittleEndian(ReadOnlySpan<byte> source)
    {
        if (source.Length < Size)
        {
            throw new ArgumentException($"Need at least {Size} bytes.", nameof(source));
        }

        return new Int256(
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

    /// <summary>Converts this value to a signed <see cref="BigInteger"/>.</summary>
    public BigInteger ToBigInteger()
    {
        BigInteger magnitude = ((BigInteger)e3 << 192) | ((BigInteger)e2 << 128) | ((BigInteger)e1 << 64) | e0;
        return IsNegative ? magnitude - (BigInteger.One << 256) : magnitude;
    }

    /// <summary>Creates an <see cref="Int256"/> from a <see cref="BigInteger"/> in the representable range.</summary>
    public static Int256 FromBigInteger(BigInteger value)
    {
        BigInteger max = (BigInteger.One << 255) - 1;
        BigInteger min = -(BigInteger.One << 255);
        if (value < min || value > max)
        {
            throw new OverflowException("Value does not fit in Int256.");
        }

        BigInteger bits = value.Sign < 0 ? value + (BigInteger.One << 256) : value;
        const ulong mask = ulong.MaxValue;
        return new Int256(
            (ulong)(bits & mask),
            (ulong)((bits >> 64) & mask),
            (ulong)((bits >> 128) & mask),
            (ulong)((bits >> 192) & mask));
    }

    /// <inheritdoc/>
    public bool Equals(Int256 other) => e0 == other.e0 && e1 == other.e1 && e2 == other.e2 && e3 == other.e3;

    /// <inheritdoc/>
    public override bool Equals(object obj) => obj is Int256 other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(e0, e1, e2, e3);

    /// <inheritdoc/>
    public int CompareTo(Int256 other)
    {
        bool thisNeg = IsNegative;
        bool otherNeg = other.IsNegative;
        if (thisNeg != otherNeg)
        {
            return thisNeg ? -1 : 1;
        }

        // Same sign: unsigned limb comparison yields the correct signed order in two's complement.
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

    public static bool operator ==(Int256 left, Int256 right) => left.Equals(right);

    public static bool operator !=(Int256 left, Int256 right) => !left.Equals(right);

    public static bool operator <(Int256 left, Int256 right) => left.CompareTo(right) < 0;

    public static bool operator >(Int256 left, Int256 right) => left.CompareTo(right) > 0;

    public static bool operator <=(Int256 left, Int256 right) => left.CompareTo(right) <= 0;

    public static bool operator >=(Int256 left, Int256 right) => left.CompareTo(right) >= 0;
}
