using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The dictionary of a LowCardinality write over a fixed-width leaf: it gives each distinct canonical value one key,
/// in the order of first use. A canonical value is a wire value (see <see cref="IWriteConversion{T, TCanon}"/>), so
/// two values share an entry exactly when they encode to the same bytes. For the floats the canonical value is the
/// bit pattern, so +0 and -0 get two entries, and so do two NaN values with different payloads.
/// </summary>
/// <remarks>
/// The reserved entries are as for <see cref="ByteInterner"/>. One interner serves one write, on one thread. Dispose
/// it to return the entry buffer to the pool.
/// </remarks>
/// <typeparam name="TCanon">The canonical value of the leaf.</typeparam>
internal sealed class FixedInterner<TCanon> : IDisposable
    where TCanon : unmanaged, IEquatable<TCanon>
{
    private const int InitialEntries = 64;

    private readonly Dictionary<TCanon, int> keys = new();

    private TCanon[] entries;
    private int count;

    /// <summary>Initializes an interner with the reserved entries.</summary>
    /// <param name="placeholder">The canonical placeholder of the leaf.</param>
    /// <param name="nullable">Whether the dictionary also has the NULL slot (<c>LowCardinality(Nullable(X))</c>).</param>
    public FixedInterner(TCanon placeholder, bool nullable)
    {
        entries = ArrayPool<TCanon>.Shared.Rent(InitialEntries);
        if (nullable)
        {
            Append(placeholder);
        }

        Intern(placeholder);
    }

    /// <summary>The number of entries, the reserved ones included.</summary>
    public int Count => count;

    /// <summary>The entries, in key order. The span is valid only until the next call to <see cref="Intern"/>.</summary>
    public ReadOnlySpan<TCanon> Entries => entries.AsSpan(0, count);

    /// <summary>The key of <paramref name="value"/>: the key of an equal entry, or a new key at the end.</summary>
    /// <param name="value">The canonical value.</param>
    /// <returns>The key.</returns>
    public int Intern(TCanon value)
    {
        ObjectDisposedException.ThrowIf(entries is null, this);
        ref int key = ref CollectionsMarshal.GetValueRefOrAddDefault(keys, value, out bool exists);
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

        ArrayPool<TCanon>.Shared.Return(entries);
        entries = null;
    }

    private int Append(TCanon value)
    {
        if (count == entries.Length)
        {
            TCanon[] larger = ArrayPool<TCanon>.Shared.Rent((int)Math.Min(count * 2L, Array.MaxLength));
            entries.AsSpan(0, count).CopyTo(larger);
            ArrayPool<TCanon>.Shared.Return(entries);
            entries = larger;
        }

        entries[count] = value;
        return count++;
    }
}
