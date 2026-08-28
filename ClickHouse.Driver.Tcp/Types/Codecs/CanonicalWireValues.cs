using System;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>A byte array whose equality is its encoded content.</summary>
internal readonly struct ByteArrayWireValue : IEquatable<ByteArrayWireValue>
{
    public ByteArrayWireValue(byte[] bytes) => Bytes = bytes;

    public byte[] Bytes { get; }

    public bool Equals(ByteArrayWireValue other)
        => ReferenceEquals(Bytes, other.Bytes)
            || (Bytes is not null && other.Bytes is not null && Bytes.AsSpan().SequenceEqual(other.Bytes));

    public override bool Equals(object obj) => obj is ByteArrayWireValue other && Equals(other);

    public override int GetHashCode()
    {
        if (Bytes is null)
        {
            return 0;
        }

        HashCode hash = default;
        hash.AddBytes(Bytes);
        return hash.ToHashCode();
    }
}

/// <summary>The two 64-bit words written for an IPv6 value.</summary>
internal readonly struct IPv6WireValue : IEquatable<IPv6WireValue>
{
    public IPv6WireValue(ulong first, ulong second)
    {
        First = first;
        Second = second;
    }

    public ulong First { get; }

    public ulong Second { get; }

    public bool Equals(IPv6WireValue other) => First == other.First && Second == other.Second;

    public override bool Equals(object obj) => obj is IPv6WireValue other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(First, Second);
}
