using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// Interns the CLR values of a byte-run leaf into a <see cref="ByteInterner"/>, with a fast path: when the leaf
/// states that equal CLR values have equal canonical bytes
/// (<see cref="BytesLeafWriter{T}.ClrEqualityImpliesCanonicalEquality"/>), a value is looked up by its CLR value
/// first, and it is converted and interned only when that lookup fails.
/// </summary>
/// <remarks>
/// <para>
/// The <see cref="ByteInterner"/> stays the only source of keys. The CLR lookup only remembers keys that it gave
/// before, so the interner can stop it at any value with no change to the result.
/// </para>
/// <para>
/// The CLR lookup costs a hash and an insert for each new value. So after <see cref="ProbeValues"/> lookups, the
/// interner stops it when more than half of them failed (the values do not repeat much). This is a heuristic: a
/// column whose first values are distinct and whose later values repeat keeps the slower path.
/// </para>
/// <para>One interner serves one write, on one thread. Dispose it to return its buffers.</para>
/// </remarks>
/// <typeparam name="T">The CLR type that the leaf writes.</typeparam>
internal sealed class ClrKeyedByteInterner<T> : IDisposable
{
    /// <summary>The number of CLR lookups after which the interner decides whether to keep the CLR lookup.</summary>
    public const int ProbeValues = 1024;

    private readonly BytesLeafWriter<T> leaf;
    private readonly ByteInterner canonical;

    private Dictionary<T, int> clrKeys;
    private byte[] scratch = Array.Empty<byte>();
    private int lookups;
    private int misses;

    /// <summary>Initializes an interner for one write.</summary>
    /// <param name="leaf">The leaf whose canonical bytes are interned.</param>
    /// <param name="nullable">Whether the dictionary also has the NULL slot (<c>LowCardinality(Nullable(X))</c>).</param>
    public ClrKeyedByteInterner(BytesLeafWriter<T> leaf, bool nullable)
    {
        this.leaf = leaf ?? throw new ArgumentNullException(nameof(leaf));

        // The interner copies the placeholder, so the scratch that holds it can serve the values next.
        canonical = new ByteInterner(leaf.GetPlaceholder(ref scratch), nullable);
        clrKeys = leaf.ClrEqualityImpliesCanonicalEquality ? new Dictionary<T, int>() : null;
    }

    /// <summary>The entries, which <see cref="BytesLeafWriter{T}.Encode"/> writes as the dictionary.</summary>
    public ByteInterner Entries => canonical;

    /// <summary>Whether the interner still looks values up by their CLR value.</summary>
    public bool UsesClrKeys => clrKeys is not null;

    /// <summary>The key of <paramref name="value"/>.</summary>
    /// <param name="value">The value. The caller handles a NULL of a nullable column before it calls this.</param>
    /// <param name="position">The zero-based position of the value in the write, for error messages.</param>
    /// <returns>The key.</returns>
    /// <exception cref="ArgumentException">The leaf cannot store the value.</exception>
    public int Intern(T value, int position)
    {
        Dictionary<T, int> map = clrKeys;

        // A null has no CLR key. The leaf decides what a null means, and usually refuses it.
        if (map is null || value is null)
        {
            return canonical.Intern(leaf.ToCanonical(value, position, ref scratch));
        }

        ref int slot = ref CollectionsMarshal.GetValueRefOrAddDefault(map, value, out bool exists);
        int key;
        if (exists)
        {
            key = slot;
        }
        else
        {
            try
            {
                key = canonical.Intern(leaf.ToCanonical(value, position, ref scratch));
            }
            catch
            {
                // Do not keep an entry with no key behind a value that the leaf refused.
                map.Remove(value);
                throw;
            }

            slot = key;
            misses++;
        }

        if (++lookups == ProbeValues && misses * 2 > ProbeValues)
        {
            clrKeys = null;
        }

        return key;
    }

    /// <summary>Returns the buffers to the pool. The interner cannot be used after this.</summary>
    public void Dispose()
    {
        canonical.Dispose();
        if (scratch.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(scratch);
            scratch = Array.Empty<byte>();
        }

        clrKeys = null;
    }
}
