using System;
using System.Buffers;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The dictionary of a LowCardinality write over a byte-run leaf (<c>String</c>, <c>FixedString</c>,
/// <c>JSON</c>): it gives each distinct run of canonical bytes one key, in the order of first use. Two values share
/// an entry exactly when their bytes are equal.
/// </summary>
/// <remarks>
/// <para>
/// The first entries are reserved. Without NULL, entry 0 is the placeholder. With NULL, entry 0 is the NULL slot (it
/// holds the placeholder bytes and no lookup finds it) and entry 1 is the placeholder. A value equal to the
/// placeholder gets the key of the placeholder entry.
/// </para>
/// <para>
/// The entries are stored end to end in one buffer. The buffers come from <see cref="ArrayPool{T}.Shared"/> and
/// grow with the number of distinct values, not with the number of rows. One interner serves one write, on one
/// thread. Dispose it to return the buffers.
/// </para>
/// </remarks>
internal sealed class ByteInterner : IDisposable
{
    private const int InitialEntries = 64;
    private const int InitialBytes = 1024;

    private readonly int firstIndexed;

    private byte[] bytes;
    private int byteCount;

    // starts[i] is where entry i begins; starts[count] is byteCount, so entry i ends at starts[i + 1].
    private int[] starts;
    private int[] hashes;

    // Open addressing with linear probing: entry index + 1, or 0 for an empty bucket. bucketCount is a power of two,
    // and the rented array can be longer than it.
    private int[] buckets;
    private int bucketCount;

    private int count;

    /// <summary>Initializes an interner with the reserved entries.</summary>
    /// <param name="placeholder">The canonical placeholder bytes of the leaf.</param>
    /// <param name="nullable">Whether the dictionary also has the NULL slot (<c>LowCardinality(Nullable(X))</c>).</param>
    public ByteInterner(ReadOnlySpan<byte> placeholder, bool nullable)
    {
        bytes = ArrayPool<byte>.Shared.Rent(Math.Max(InitialBytes, placeholder.Length * 2));
        starts = ArrayPool<int>.Shared.Rent(InitialEntries + 1);
        hashes = ArrayPool<int>.Shared.Rent(InitialEntries);
        bucketCount = InitialEntries * 2;
        buckets = RentBuckets(bucketCount);
        starts[0] = 0;

        if (nullable)
        {
            Append(placeholder, Hash(placeholder));
            firstIndexed = 1;
        }

        Intern(placeholder);
    }

    /// <summary>The number of entries, the reserved ones included.</summary>
    public int Count => count;

    /// <summary>The bytes of one entry. The span is valid only until the next call to <see cref="Intern"/>.</summary>
    /// <param name="index">The key of the entry.</param>
    /// <returns>The canonical bytes.</returns>
    public ReadOnlySpan<byte> Entry(int index)
    {
        if ((uint)index >= (uint)count)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index, $"The interner has {count} entries.");
        }

        return bytes.AsSpan(starts[index], starts[index + 1] - starts[index]);
    }

    /// <summary>The key of <paramref name="value"/>: the key of an equal entry, or a new key at the end.</summary>
    /// <param name="value">The canonical bytes. The interner copies them.</param>
    /// <returns>The key.</returns>
    public int Intern(ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(bytes is null, this);
        int hash = Hash(value);
        int mask = bucketCount - 1;
        for (int bucket = hash & mask; ; bucket = (bucket + 1) & mask)
        {
            int slot = buckets[bucket];
            if (slot == 0)
            {
                int index = Append(value, hash);
                buckets[bucket] = index + 1;

                // At most half of the buckets are used, so a probe ends soon.
                if ((count - firstIndexed) * 2 > bucketCount)
                {
                    Rehash();
                }

                return index;
            }

            int candidate = slot - 1;
            if (hashes[candidate] == hash
                && bytes.AsSpan(starts[candidate], starts[candidate + 1] - starts[candidate]).SequenceEqual(value))
            {
                return candidate;
            }
        }
    }

    /// <summary>Returns the buffers to the pool. The interner cannot be used after this.</summary>
    public void Dispose()
    {
        if (bytes is null)
        {
            return;
        }

        ArrayPool<byte>.Shared.Return(bytes);
        ArrayPool<int>.Shared.Return(starts);
        ArrayPool<int>.Shared.Return(hashes);
        ArrayPool<int>.Shared.Return(buckets);
        bytes = null;
        starts = null;
        hashes = null;
        buckets = null;
    }

    private static int Hash(ReadOnlySpan<byte> value)
    {
        var hash = default(HashCode);
        hash.AddBytes(value);
        return hash.ToHashCode();
    }

    private static int[] RentBuckets(int length)
    {
        int[] rented = ArrayPool<int>.Shared.Rent(length);
        Array.Clear(rented, 0, length);
        return rented;
    }

    private int Append(ReadOnlySpan<byte> value, int hash)
    {
        if (count + 1 >= starts.Length)
        {
            starts = Grow(starts, count + 1, (count + 2) * 2L);
        }

        if (count >= hashes.Length)
        {
            hashes = Grow(hashes, count, (count + 1) * 2L);
        }

        long end = (long)byteCount + value.Length;
        if (end > Array.MaxLength)
        {
            throw new InvalidOperationException(
                $"The distinct values of a LowCardinality column exceed {Array.MaxLength} bytes, the most that this client can buffer.");
        }

        if (end > bytes.Length)
        {
            bytes = Grow(bytes, byteCount, Math.Max(bytes.Length * 2L, end));
        }

        value.CopyTo(bytes.AsSpan(byteCount));
        hashes[count] = hash;
        byteCount = (int)end;
        count++;
        starts[count] = byteCount;
        return count - 1;
    }

    private void Rehash()
    {
        int newCount = checked(bucketCount * 2);
        int[] newBuckets = RentBuckets(newCount);
        int mask = newCount - 1;
        for (int i = firstIndexed; i < count; i++)
        {
            int bucket = hashes[i] & mask;
            while (newBuckets[bucket] != 0)
            {
                bucket = (bucket + 1) & mask;
            }

            newBuckets[bucket] = i + 1;
        }

        ArrayPool<int>.Shared.Return(buckets);
        buckets = newBuckets;
        bucketCount = newCount;
    }

    // Rents a larger array, copies the used part, and returns the old array to the pool.
    private static T[] Grow<T>(T[] array, int used, long minLength)
    {
        T[] larger = ArrayPool<T>.Shared.Rent((int)Math.Min(minLength, Array.MaxLength));
        Array.Copy(array, larger, used);
        ArrayPool<T>.Shared.Return(array);
        return larger;
    }
}
