using System;
using System.Buffers.Binary;
using System.Numerics;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// CityHash v1.0.2, the historical variant ClickHouse uses for the compression frame's 128-bit checksum.
/// <para>
/// This is <b>not</b> modern Google CityHash. Release 1.1 changed the 128-bit functions, so a 1.1+
/// implementation disagrees with ClickHouse on every input and every frame it writes is rejected as
/// corrupt. A faithful port of the reference <c>city.cc</c> that ClickHouse vendors as
/// <c>contrib/cityhash102</c>.
/// </para>
/// <para>
/// Only the 128-bit entry point is ported, because the frame checksum is all the drivers need.
/// </para>
/// </summary>
internal static class CityHash102
{
    // Some primes between 2^63 and 2^64, named as in the reference.
    private const ulong K0 = 0xc3a5c85c97cb3127UL;
    private const ulong K1 = 0xb492b66fbe98f273UL;
    private const ulong K2 = 0x9ae16a3b2f90404fUL;
    private const ulong K3 = 0xc949d7c7509e6557UL;

    /// <summary>
    /// Computes the CityHash v1.0.2 128-bit hash of <paramref name="data"/>. The frame writes
    /// <c>Low</c> then <c>High</c>, each little-endian.
    /// </summary>
    /// <param name="data">The bytes to hash. May be empty.</param>
    /// <returns>The low and high halves of the 128-bit result.</returns>
    public static (ulong Low, ulong High) Hash128(ReadOnlySpan<byte> data)
    {
        int len = data.Length;
        if (len >= 16)
        {
            return Hash128WithSeed(data.Slice(16), Fetch64(data, 0) ^ K3, Fetch64(data, 8));
        }

        if (len >= 8)
        {
            // The reference passes a null pointer and length 0 on this branch; an empty span is the same.
            return Hash128WithSeed(default, Fetch64(data, 0) ^ ((ulong)len * K0), Fetch64(data, len - 8) ^ K1);
        }

        return Hash128WithSeed(data, K0, K1);
    }

    /// <summary>Hashes <paramref name="s"/> with a 128-bit seed, per the reference <c>CityHash128WithSeed</c>.</summary>
    private static (ulong Low, ulong High) Hash128WithSeed(ReadOnlySpan<byte> s, ulong seedLow, ulong seedHigh)
    {
        if (s.Length < 128)
        {
            return CityMurmur(s, seedLow, seedHigh);
        }

        // 56 bytes of state: v, w, x, y and z.
        ulong x = seedLow;
        ulong y = seedHigh;
        ulong z = (ulong)s.Length * K1;
        (ulong First, ulong Second) v;
        (ulong First, ulong Second) w;
        v.First = Rotate(y ^ K1, 49) * K1 + Fetch64(s, 0);
        v.Second = Rotate(v.First, 42) * K1 + Fetch64(s, 8);
        w.First = Rotate(y + z, 35) * K1 + x;
        w.Second = Rotate(x + Fetch64(s, 88), 53) * K1;

        // The same inner loop as CityHash64, manually unrolled to two 64-byte halves per pass.
        int pos = 0;
        int remaining = s.Length;
        do
        {
            x = Rotate(x + y + v.First + Fetch64(s, pos + 16), 37) * K1;
            y = Rotate(y + v.Second + Fetch64(s, pos + 48), 42) * K1;
            x ^= w.Second;
            y ^= v.First;
            z = Rotate(z ^ w.First, 33);
            v = WeakHashLen32WithSeeds(s, pos, v.Second * K1, x + w.First);
            w = WeakHashLen32WithSeeds(s, pos + 32, z + w.Second, y);
            (z, x) = (x, z);
            pos += 64;

            x = Rotate(x + y + v.First + Fetch64(s, pos + 16), 37) * K1;
            y = Rotate(y + v.Second + Fetch64(s, pos + 48), 42) * K1;
            x ^= w.Second;
            y ^= v.First;
            z = Rotate(z ^ w.First, 33);
            v = WeakHashLen32WithSeeds(s, pos, v.Second * K1, x + w.First);
            w = WeakHashLen32WithSeeds(s, pos + 32, z + w.Second, y);
            (z, x) = (x, z);
            pos += 64;

            remaining -= 128;
        }
        while (remaining >= 128);

        y += Rotate(w.First, 37) * K0 + z;
        x += Rotate(v.First + z, 49) * K0;

        // Hash up to four 32-byte chunks from the end. These indices walk backwards and can land before
        // `pos`, which is in range only because the loop above ran at least once, so pos >= 128.
        for (int tailDone = 0; tailDone < remaining;)
        {
            tailDone += 32;
            y = Rotate(y - x, 42) * K0 + v.Second;
            w.First += Fetch64(s, pos + remaining - tailDone + 16);
            x = Rotate(x, 49) * K0 + w.First;
            w.First += v.First;
            v = WeakHashLen32WithSeeds(s, pos + remaining - tailDone, v.First, v.Second);
        }

        x = HashLen16(x, v.First);
        y = HashLen16(y, w.First);
        return (HashLen16(x + v.Second, w.Second) + y, HashLen16(x + w.Second, y + v.Second));
    }

    /// <summary>The reference <c>CityMurmur</c>: a 128-bit hash for any length, used below 128 bytes.</summary>
    private static (ulong Low, ulong High) CityMurmur(ReadOnlySpan<byte> s, ulong seedLow, ulong seedHigh)
    {
        ulong a = seedLow;
        ulong b = seedHigh;
        ulong c = 0;
        ulong d = 0;

        // Signed on purpose: the reference uses ssize_t, so a length below 16 must compare as negative.
        long l = (long)s.Length - 16;
        if (l <= 0)
        {
            a = ShiftMix(a * K1) * K1;
            c = (b * K1) + HashLen0to16(s);
            d = ShiftMix(a + (s.Length >= 8 ? Fetch64(s, 0) : c));
        }
        else
        {
            c = HashLen16(Fetch64(s, s.Length - 8) + K1, a);
            d = HashLen16(b + (ulong)s.Length, c + Fetch64(s, s.Length - 16));
            a += d;

            int pos = 0;
            do
            {
                a ^= ShiftMix(Fetch64(s, pos) * K1) * K1;
                a *= K1;
                b ^= a;
                c ^= ShiftMix(Fetch64(s, pos + 8) * K1) * K1;
                c *= K1;
                d ^= c;
                pos += 16;
                l -= 16;
            }
            while (l > 0);
        }

        a = HashLen16(a, c);
        b = HashLen16(d, b);
        return (a ^ b, HashLen16(b, a));
    }

    /// <summary>Hashes 0 to 16 bytes, per the reference <c>HashLen0to16</c>.</summary>
    private static ulong HashLen0to16(ReadOnlySpan<byte> s)
    {
        int len = s.Length;
        if (len > 8)
        {
            ulong a = Fetch64(s, 0);
            ulong b = Fetch64(s, len - 8);
            return HashLen16(a, Rotate(b + (ulong)len, len)) ^ b;
        }

        if (len >= 4)
        {
            ulong a = Fetch32(s, 0);
            return HashLen16((ulong)len + (a << 3), Fetch32(s, len - 4));
        }

        if (len > 0)
        {
            uint y = s[0] + ((uint)s[len >> 1] << 8);
            uint z = (uint)len + ((uint)s[len - 1] << 2);
            return ShiftMix((y * K2) ^ (z * K3)) * K2;
        }

        return K2;
    }

    /// <summary>A 16-byte hash of 32 bytes at <paramref name="pos"/> plus two seeds. Quick and dirty, per the reference.</summary>
    private static (ulong First, ulong Second) WeakHashLen32WithSeeds(ReadOnlySpan<byte> s, int pos, ulong a, ulong b)
        => WeakHashLen32WithSeeds(Fetch64(s, pos), Fetch64(s, pos + 8), Fetch64(s, pos + 16), Fetch64(s, pos + 24), a, b);

    /// <summary>The six-word core of <c>WeakHashLen32WithSeeds</c>.</summary>
    private static (ulong First, ulong Second) WeakHashLen32WithSeeds(ulong w, ulong x, ulong y, ulong z, ulong a, ulong b)
    {
        a += w;
        b = Rotate(b + a + z, 21);
        ulong c = a;
        a += x;
        a += y;
        b += Rotate(a, 44);
        return (a + z, b + c);
    }

    /// <summary>Collapses a 128-bit value to 64 bits (Murmur-inspired), per the reference <c>Hash128to64</c>.</summary>
    private static ulong Hash128to64(ulong low, ulong high)
    {
        const ulong Mul = 0x9ddfea08eb382d69UL;
        ulong a = (low ^ high) * Mul;
        a ^= a >> 47;
        ulong b = (high ^ a) * Mul;
        b ^= b >> 47;
        b *= Mul;
        return b;
    }

    /// <summary>Hashes two words, treating them as the low and high halves of a 128-bit value.</summary>
    private static ulong HashLen16(ulong u, ulong v) => Hash128to64(u, v);

    /// <summary>
    /// Bitwise right rotate. Covers both the reference's <c>Rotate</c> and its <c>RotateByAtLeast1</c>:
    /// the former guards against a shift of 0 only because shifting a 64-bit value by 64 is undefined in
    /// C++, which <see cref="BitOperations.RotateRight(ulong, int)"/> is not.
    /// </summary>
    private static ulong Rotate(ulong value, int shift) => BitOperations.RotateRight(value, shift);

    /// <summary>The reference <c>ShiftMix</c>.</summary>
    private static ulong ShiftMix(ulong value) => value ^ (value >> 47);

    /// <summary>Reads a little-endian 64-bit word, as the reference's <c>Fetch64</c> does on a little-endian host.</summary>
    private static ulong Fetch64(ReadOnlySpan<byte> s, int index) => BinaryPrimitives.ReadUInt64LittleEndian(s.Slice(index));

    /// <summary>Reads a little-endian 32-bit word, as the reference's <c>Fetch32</c> does on a little-endian host.</summary>
    private static uint Fetch32(ReadOnlySpan<byte> s, int index) => BinaryPrimitives.ReadUInt32LittleEndian(s.Slice(index));
}
