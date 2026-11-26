using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The dictionary of a LowCardinality write over a fixed-width leaf: it gives each distinct canonical value one key,
/// in the order of first use. A canonical value is a wire value (see <see cref="IWriteConversion{T, TWire}"/>), so
/// two values share an entry exactly when they encode to the same bytes. For the floats the canonical value is the
/// bit pattern, so +0 and -0 get two entries, and so do two NaN values with different payloads.
/// </summary>
/// <remarks>
/// The reserved entries are as for <see cref="ByteInterner"/>. One interner serves one write, on one thread. Dispose
/// it to return the entry buffer to the pool.
/// </remarks>
/// <typeparam name="TWire">The canonical value of the leaf.</typeparam>
internal sealed class FixedInterner<TWire> : IDisposable
    where TWire : unmanaged, IEquatable<TWire>
{
    private const int InitialEntries = 64;

    // The keys of a 16-byte canonical value (UInt128, Int128) are hashed as two 64-bit halves (WideKey): its own hash
    // costs more than the rest of a lookup. Every other canonical value is its own key.
    private readonly Dictionary<TWire, int> keys = Unsafe.SizeOf<TWire>() == 16 ? null : new();
    private readonly Dictionary<WideKey, int> wideKeys = Unsafe.SizeOf<TWire>() == 16 ? new() : null;

    private TWire[] entries;
    private int count;

    /// <summary>Initializes an interner with the reserved entries.</summary>
    /// <param name="placeholder">The canonical placeholder of the leaf.</param>
    /// <param name="nullable">Whether the dictionary also has the NULL slot (<c>LowCardinality(Nullable(X))</c>).</param>
    public FixedInterner(TWire placeholder, bool nullable)
    {
        entries = ArrayPool<TWire>.Shared.Rent(InitialEntries);
        if (nullable)
        {
            Append(placeholder);
        }

        Intern(placeholder);
    }

    /// <summary>The number of entries, the reserved ones included.</summary>
    public int Count => count;

    /// <summary>The entries, in key order. The span is valid only until the next call to <see cref="Intern"/>.</summary>
    public ReadOnlySpan<TWire> Entries => entries.AsSpan(0, count);

    /// <summary>The key of <paramref name="value"/>: the key of an equal entry, or a new key at the end.</summary>
    /// <param name="value">The canonical value.</param>
    /// <returns>The key.</returns>
    public int Intern(TWire value)
    {
        ObjectDisposedException.ThrowIf(entries is null, this);
        ref int key = ref Unsafe.SizeOf<TWire>() == 16
            ? ref CollectionsMarshal.GetValueRefOrAddDefault(wideKeys, WideKey.Of(ref value), out bool exists)
            : ref CollectionsMarshal.GetValueRefOrAddDefault(keys, value, out exists);
        if (!exists)
        {
            key = Append(value);
        }

        return key;
    }

    /// <summary>Returns the entry buffer to the pool. The interner cannot be used after this.</summary>
    public void Dispose()
    {
        if (entries is null)
        {
            return;
        }

        ArrayPool<TWire>.Shared.Return(entries);
        entries = null;
    }

    private int Append(TWire value)
    {
        if (count == entries.Length)
        {
            TWire[] larger = ArrayPool<TWire>.Shared.Rent((int)Math.Min(count * 2L, Array.MaxLength));
            entries.AsSpan(0, count).CopyTo(larger);
            ArrayPool<TWire>.Shared.Return(entries);
            entries = larger;
        }

        entries[count] = value;
        return count++;
    }
}

/// <summary>
/// A 16-byte canonical value as the key of a <see cref="FixedInterner{TWire}"/>: two keys are equal when their bits are
/// equal, which for <see cref="UInt128"/> and <see cref="Int128"/> is when the values are equal. The hash mixes the two
/// halves with two multiplications.
/// </summary>
internal readonly struct WideKey : IEquatable<WideKey>
{
    private readonly ulong lower;
    private readonly ulong upper;

    private WideKey(ulong lower, ulong upper)
    {
        this.lower = lower;
        this.upper = upper;
    }

    /// <summary>The key of a 16-byte value.</summary>
    /// <typeparam name="TWire">The type of the value, 16 bytes wide.</typeparam>
    /// <param name="value">The value.</param>
    /// <returns>The key.</returns>
    public static WideKey Of<TWire>(ref TWire value)
        where TWire : unmanaged
    {
        ref ulong halves = ref Unsafe.As<TWire, ulong>(ref value);
        return new WideKey(halves, Unsafe.Add(ref halves, 1));
    }

    /// <inheritdoc/>
    public bool Equals(WideKey other) => lower == other.lower && upper == other.upper;

    /// <inheritdoc/>
    public override bool Equals(object obj) => obj is WideKey other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => (int)(((lower ^ (upper * 0x9E3779B97F4A7C15UL)) * 0xBF58476D1CE4E5B9UL) >> 32);
}
