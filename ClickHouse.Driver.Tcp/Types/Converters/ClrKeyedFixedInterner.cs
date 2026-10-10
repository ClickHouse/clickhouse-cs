using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// Interns the CLR values of a fixed-width leaf into a <see cref="FixedInterner{TCanon}"/>, with a fast path: when equal
/// CLR values always convert to equal canonical values
/// (<see cref="FixedLeafWriter{T, TCanon}.ClrEqualityImpliesCanonicalEquality"/>), a value is looked up by its CLR value
/// first, and it is converted and interned only when that lookup fails. So a <see cref="Guid"/> is converted to its wire
/// bytes once for each distinct value, not once for each row.
/// </summary>
/// <remarks>
/// <para>
/// The <see cref="FixedInterner{TCanon}"/> stays the only source of keys and entries. The CLR lookup only remembers keys
/// that it gave before, so the interner can stop it at any value with no change to the result.
/// </para>
/// <para>
/// The CLR lookup costs a hash and an insert for each new value. So after <see cref="ProbeValues"/> lookups, the interner
/// stops it when more than half of them failed (the values do not repeat much), as <see cref="ClrKeyedByteInterner{T}"/>
/// does. A leaf whose CLR value is its canonical value has no CLR lookup: the canonical lookup is the same lookup.
/// </para>
/// <para>
/// One interner serves one write, on one thread. Dispose it to return its buffer.
/// </para>
/// </remarks>
/// <typeparam name="T">The CLR type that the leaf writes.</typeparam>
/// <typeparam name="TCanon">The canonical value of the leaf.</typeparam>
internal sealed class ClrKeyedFixedInterner<T, TCanon> : IDisposable
    where TCanon : unmanaged, IEquatable<TCanon>
{
    /// <summary>The number of CLR lookups after which the interner decides whether to keep the CLR lookup.</summary>
    public const int ProbeValues = 1024;

    private readonly FixedLeafWriter<T, TCanon> leaf;
    private readonly FixedInterner<TCanon> canonical;

    private Dictionary<T, int> clrKeys;

    // The CLR lookups left before the interner decides whether to keep the CLR lookup, and the misses so far.
    private int probeLeft = ProbeValues;
    private int misses;

    /// <summary>Initializes an interner for one write.</summary>
    /// <param name="leaf">The leaf whose canonical values are interned.</param>
    /// <param name="nullable">Whether the dictionary also has the NULL slot (<c>LowCardinality(Nullable(X))</c>).</param>
    public ClrKeyedFixedInterner(FixedLeafWriter<T, TCanon> leaf, bool nullable)
    {
        this.leaf = leaf ?? throw new ArgumentNullException(nameof(leaf));
        canonical = new FixedInterner<TCanon>(leaf.Placeholder, nullable);
        clrKeys = typeof(T) != typeof(TCanon) && leaf.ClrEqualityImpliesCanonicalEquality ? new Dictionary<T, int>() : null;
    }

    /// <summary>The entries, which <see cref="FixedLeafWriter{T, TCanon}.Encode"/> writes as the dictionary.</summary>
    public FixedInterner<TCanon> Entries => canonical;

    /// <summary>Whether the interner still looks values up by their CLR value.</summary>
    public bool UsesClrKeys => clrKeys is not null;

    /// <summary>The key of <paramref name="value"/>.</summary>
    /// <remarks>
    /// A value that the CLR lookup finds costs one lookup and one decrement. A new value goes to a method that is not
    /// inlined, as does the end of the probe, so this method stays small enough to inline.
    /// </remarks>
    /// <param name="value">The value. The caller handles a NULL of a nullable column before it calls this.</param>
    /// <returns>The key.</returns>
    /// <exception cref="ArgumentException">
    /// The leaf cannot store the value. The message names the value by the dictionary slot that it would take.
    /// </exception>
    /// <exception cref="OverflowException">The value is outside the range of the leaf.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Intern(T value)
    {
        Dictionary<T, int> map = clrKeys;

        // A null has no CLR key. The leaf decides what a null means, and refuses it.
        if (map is not null && value is not null)
        {
            if (map.TryGetValue(value, out int key))
            {
                if (--probeLeft == 0)
                {
                    EndProbe();
                }

                return key;
            }

            return InternNew(value, map);
        }

        return canonical.Intern(leaf.ToCanonical(value, canonical.Count));
    }

    /// <summary>Returns the buffer to the pool. The interner cannot be used after this.</summary>
    public void Dispose()
    {
        canonical.Dispose();
        clrKeys = null;
    }

    // The key of a value that the CLR lookup does not find. A refused value throws before the CLR lookup gets an entry
    // for it.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private int InternNew(T value, Dictionary<T, int> map)
    {
        int key = canonical.Intern(leaf.ToCanonical(value, canonical.Count));
        map.Add(value, key);
        misses++;
        if (--probeLeft == 0)
        {
            EndProbe();
        }

        return key;
    }

    // The end of the probe: the CLR lookup stops when more than half of the probe lookups failed.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void EndProbe()
    {
        if (misses * 2 > ProbeValues)
        {
            clrKeys = null;
        }
    }
}
