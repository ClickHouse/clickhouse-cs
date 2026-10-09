// SPIKE for ClickHouse/integrations#801. Not production code.
//
// Write side of the candidate structure. A writer converts each value to its canonical value just before it
// encodes it. The wire layout of a composite is a sequence of streams that each cover ALL rows (Array: offsets,
// then the whole child; Nullable: the whole null map, then the whole inner), so a combinator cannot call its
// inner writer once per row. Writers therefore take a list of segments: Each passes its rows' arrays as the
// child's segments, with no flat copy.
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace WireConverterSpike;

internal abstract class ColumnWriter<T>
{
    public virtual void WritePrefix(ClickHouseBinaryWriter writer)
    {
    }

    public abstract void Write(ClickHouseBinaryWriter writer, ReadOnlySpan<ReadOnlyMemory<T>> segments);

    public void Write(ClickHouseBinaryWriter writer, ReadOnlyMemory<T> values)
        => Write(writer, MemoryMarshal.CreateReadOnlySpan(ref values, 1));

    internal static int Count(ReadOnlySpan<ReadOnlyMemory<T>> segments)
    {
        int total = 0;
        foreach (ReadOnlyMemory<T> segment in segments)
        {
            total += segment.Length;
        }

        return total;
    }
}

/// <summary>A leaf writer that a combinator can ask for the canonical placeholder.</summary>
internal abstract class LeafWriter<T> : ColumnWriter<T>
{
    public abstract void WritePlaceholders(ClickHouseBinaryWriter writer, int count);

    public abstract void WriteOne(ClickHouseBinaryWriter writer, T value);
}

/// <summary>A leaf whose canonical value is a byte run, so LowCardinality can intern it.</summary>
internal abstract class BytesLeafWriter<T> : LeafWriter<T>
{
    /// <summary>The canonical bytes of <paramref name="value"/>; may point into <paramref name="scratch"/>.</summary>
    public abstract ReadOnlySpan<byte> Canonical(T value, ref byte[] scratch);

    public abstract ReadOnlySpan<byte> Placeholder { get; }

    public abstract void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<byte> canonical);

    /// <summary>
    /// Whether equal CLR values (by <see cref="EqualityComparer{T}.Default"/>) always have equal canonical bytes.
    /// When true, LowCardinality can look the CLR value up first and convert only on a miss. The converse is
    /// not needed: two unequal CLR values with the same canonical bytes still share one entry, because the
    /// miss path interns the bytes.
    /// </summary>
    public virtual bool ClrEqualityImpliesCanonicalEquality => false;

    public override void WritePlaceholders(ClickHouseBinaryWriter writer, int count)
    {
        for (int i = 0; i < count; i++)
        {
            Encode(writer, Placeholder);
        }
    }
}

// ---------------------------------------------------------------- leaves

/// <summary>T to canonical, for a fixed-width leaf. Throws when the value cannot be stored.</summary>
internal interface IFixedLeafWrite<T, TCanon>
{
    TCanon Write(T value);
}

internal interface IFixedEncoder<TCanon>
{
    void Encode(ClickHouseBinaryWriter writer, TCanon value);
}

internal readonly struct OffsetToDateTime : IFixedLeafWrite<DateTimeOffset, uint>
{
    public uint Write(DateTimeOffset value)
    {
        long seconds = (value.UtcDateTime - DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerSecond;
        return seconds is < 0 or > uint.MaxValue
            ? throw new ArgumentOutOfRangeException(nameof(value), value, "outside the DateTime range")
            : (uint)seconds;
    }
}

internal readonly struct UInt32Encoder : IFixedEncoder<uint>
{
    public void Encode(ClickHouseBinaryWriter writer, uint value) => writer.WriteUInt32(value);
}

internal sealed class FixedLeafWriter<T, TCanon, TConv, TEnc> : LeafWriter<T>
    where TConv : struct, IFixedLeafWrite<T, TCanon>
    where TEnc : struct, IFixedEncoder<TCanon>
{
    private readonly TCanon placeholder;

    public FixedLeafWriter(TCanon placeholder) => this.placeholder = placeholder;

    public override void Write(ClickHouseBinaryWriter writer, ReadOnlySpan<ReadOnlyMemory<T>> segments)
    {
        TConv conv = default;
        TEnc enc = default;
        foreach (ReadOnlyMemory<T> segment in segments)
        {
            foreach (T value in segment.Span)
            {
                enc.Encode(writer, conv.Write(value));
            }
        }
    }

    public override void WriteOne(ClickHouseBinaryWriter writer, T value) => default(TEnc).Encode(writer, default(TConv).Write(value));

    public override void WritePlaceholders(ClickHouseBinaryWriter writer, int count)
    {
        for (int i = 0; i < count; i++)
        {
            default(TEnc).Encode(writer, placeholder);
        }
    }
}

/// <summary><c>String</c> from text. Canonical bytes are UTF-8, so a value is encoded before it is hashed.</summary>
internal sealed class StringFromText : BytesLeafWriter<string>
{
    public static readonly StringFromText Instance = new();

    public override ReadOnlySpan<byte> Placeholder => ReadOnlySpan<byte>.Empty;

    // Equal strings have equal UTF-8. Different invalid UTF-16 strings can share UTF-8 (both lone surrogates
    // become EF BF BD); the miss path interns those to one entry.
    public override bool ClrEqualityImpliesCanonicalEquality => true;

    public override ReadOnlySpan<byte> Canonical(string value, ref byte[] scratch)
    {
        int max = Encoding.UTF8.GetMaxByteCount(value.Length);
        if (scratch.Length < max)
        {
            scratch = new byte[Math.Max(max, scratch.Length * 2)];
        }

        return scratch.AsSpan(0, Encoding.UTF8.GetBytes(value, scratch));
    }

    public override void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<byte> canonical) => writer.WriteString(canonical);

    // The direct path skips the canonical step: WriteString(string) encodes into the output buffer.
    public override void WriteOne(ClickHouseBinaryWriter writer, string value) => writer.WriteString(value);

    public override void Write(ClickHouseBinaryWriter writer, ReadOnlySpan<ReadOnlyMemory<string>> segments)
    {
        foreach (ReadOnlyMemory<string> segment in segments)
        {
            foreach (string value in segment.Span)
            {
                writer.WriteString(value ?? throw new ArgumentException("String cannot hold NULL"));
            }
        }
    }
}

/// <summary><c>String</c> or <c>FixedString(N)</c> from raw bytes.</summary>
internal sealed class BytesFromArray : BytesLeafWriter<byte[]>
{
    private readonly int fixedSize; // 0 = String
    private readonly byte[] placeholder;

    public BytesFromArray(int fixedSize)
    {
        this.fixedSize = fixedSize;
        placeholder = new byte[fixedSize];
    }

    public override ReadOnlySpan<byte> Placeholder => placeholder;

    public override ReadOnlySpan<byte> Canonical(byte[] value, ref byte[] scratch)
    {
        if (fixedSize == 0 || value.Length == fixedSize)
        {
            return value;
        }

        if (value.Length > fixedSize)
        {
            throw new ArgumentException($"{value.Length} bytes do not fit FixedString({fixedSize})");
        }

        // Shorter values are zero-padded, so the canonical value is the padded one: "ab" and "ab\0" intern alike.
        if (scratch.Length < fixedSize)
        {
            scratch = new byte[fixedSize];
        }

        Span<byte> padded = scratch.AsSpan(0, fixedSize);
        value.CopyTo(padded);
        padded[value.Length..].Clear();
        return padded;
    }

    public override void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<byte> canonical)
    {
        if (fixedSize == 0)
        {
            writer.WriteString(canonical);
        }
        else
        {
            writer.WriteBytes(canonical);
        }
    }

    public override void WriteOne(ClickHouseBinaryWriter writer, byte[] value)
    {
        byte[] scratch = Array.Empty<byte>();
        Encode(writer, Canonical(value, ref scratch));
    }

    public override void Write(ClickHouseBinaryWriter writer, ReadOnlySpan<ReadOnlyMemory<byte[]>> segments)
    {
        byte[] scratch = Array.Empty<byte>();
        foreach (ReadOnlyMemory<byte[]> segment in segments)
        {
            foreach (byte[] value in segment.Span)
            {
                Encode(writer, Canonical(value, ref scratch));
            }
        }
    }
}

/// <summary><c>FixedString(N)</c> from text: UTF-8, zero-padded. The current client has no such write.</summary>
internal sealed class FixedStringFromText : BytesLeafWriter<string>
{
    private readonly int size;
    private readonly byte[] placeholder;

    public FixedStringFromText(int size)
    {
        this.size = size;
        placeholder = new byte[size];
    }

    public override ReadOnlySpan<byte> Placeholder => placeholder;

    public override bool ClrEqualityImpliesCanonicalEquality => true;

    public override ReadOnlySpan<byte> Canonical(string value, ref byte[] scratch)
    {
        if (scratch.Length < size)
        {
            scratch = new byte[size];
        }

        Span<byte> padded = scratch.AsSpan(0, size);
        if (!Encoding.UTF8.TryGetBytes(value, padded, out int written))
        {
            throw new ArgumentException($"'{value}' does not fit FixedString({size})");
        }

        padded[written..].Clear();
        return padded;
    }

    public override void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<byte> canonical) => writer.WriteBytes(canonical);

    public override void WriteOne(ClickHouseBinaryWriter writer, string value)
    {
        byte[] scratch = Array.Empty<byte>();
        Encode(writer, Canonical(value, ref scratch));
    }

    public override void Write(ClickHouseBinaryWriter writer, ReadOnlySpan<ReadOnlyMemory<string>> segments)
    {
        byte[] scratch = new byte[size];
        foreach (ReadOnlyMemory<string> segment in segments)
        {
            foreach (string value in segment.Span)
            {
                Encode(writer, Canonical(value, ref scratch));
            }
        }
    }
}

// ---------------------------------------------------------------- combinators

/// <summary><c>Nullable(X)</c> from <c>T?</c>: the null map, then the inner values with the canonical placeholder.</summary>
internal sealed class LiftValueWriter<T> : ColumnWriter<T?>
    where T : struct
{
    private readonly LeafWriter<T> inner;

    public LiftValueWriter(LeafWriter<T> inner) => this.inner = inner;

    public override void WritePrefix(ClickHouseBinaryWriter writer) => inner.WritePrefix(writer);

    public override void Write(ClickHouseBinaryWriter writer, ReadOnlySpan<ReadOnlyMemory<T?>> segments)
    {
        foreach (ReadOnlyMemory<T?> segment in segments)
        {
            foreach (T? value in segment.Span)
            {
                writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
            }
        }

        // Per value: a virtual call into the leaf. Passing a run of T would need a copy out of T?.
        foreach (ReadOnlyMemory<T?> segment in segments)
        {
            foreach (T? value in segment.Span)
            {
                if (value.HasValue)
                {
                    inner.WriteOne(writer, value.GetValueOrDefault());
                }
                else
                {
                    inner.WritePlaceholders(writer, 1);
                }
            }
        }
    }
}

/// <summary><c>Array(X)</c> from <c>T[]</c>: offsets, then the child over the rows' arrays as segments.</summary>
internal sealed class EachWriter<T> : ColumnWriter<T[]>
{
    private readonly ColumnWriter<T> inner;

    public EachWriter(ColumnWriter<T> inner) => this.inner = inner;

    public override void WritePrefix(ClickHouseBinaryWriter writer) => inner.WritePrefix(writer);

    public override void Write(ClickHouseBinaryWriter writer, ReadOnlySpan<ReadOnlyMemory<T[]>> segments)
    {
        int rows = Count(segments);
        ReadOnlyMemory<T>[] children = ArrayPool<ReadOnlyMemory<T>>.Shared.Rent(rows);
        try
        {
            ulong offset = 0;
            int r = 0;
            foreach (ReadOnlyMemory<T[]> segment in segments)
            {
                foreach (T[] row in segment.Span)
                {
                    offset += (ulong)row.Length;
                    writer.WriteUInt64(offset);
                    children[r++] = row;
                }
            }

            inner.Write(writer, children.AsSpan(0, rows));
        }
        finally
        {
            ArrayPool<ReadOnlyMemory<T>>.Shared.Return(children, clearArray: true);
        }
    }
}

/// <summary>
/// <c>LowCardinality(X)</c> over a byte-run leaf: convert each value, then intern its canonical bytes. One
/// table for every write type, since the key is the canonical value and not the CLR value.
/// </summary>
internal sealed class DictionaryWriter<T> : ColumnWriter<T>
{
    // Experiment switch: SPIKE_LCKEY=canonical turns the CLR-key fast path off.
    private static readonly bool CanonicalOnly = Environment.GetEnvironmentVariable("SPIKE_LCKEY") == "canonical";

    // Experiment switch: SPIKE_LCKEY=always keeps the CLR map for every row (no probe).
    private static readonly bool AlwaysClr = Environment.GetEnvironmentVariable("SPIKE_LCKEY") == "always";

    private const int ProbeRows = 1024;

    private readonly BytesLeafWriter<T> inner;
    private readonly bool nullable;

    public DictionaryWriter(BytesLeafWriter<T> inner, bool nullable)
    {
        this.inner = inner;
        this.nullable = nullable;
    }

    public override void WritePrefix(ClickHouseBinaryWriter writer) => writer.WriteInt64(1);

    public override void Write(ClickHouseBinaryWriter writer, ReadOnlySpan<ReadOnlyMemory<T>> segments)
    {
        int rows = Count(segments);
        var table = new ByteInterner();
        // Slot 0 of a nullable dictionary is NULL and is never looked up; the last reserved slot holds the
        // placeholder, and a real value equal to it reuses that slot.
        if (nullable)
        {
            table.AddUnindexed(inner.Placeholder);
        }

        table.Intern(inner.Placeholder);

        int[] keys = ArrayPool<int>.Shared.Rent(rows);
        byte[] scratch = new byte[64];
        try
        {
            // Fast path: look the CLR value up first, and convert and intern the canonical bytes only on a miss.
            // The canonical table stays the source of truth, so the CLR map can be dropped at any row. It is
            // dropped after a probe when most rows miss, because then it only adds a hash and an insert per row.
            Dictionary<T, int> seen = inner.ClrEqualityImpliesCanonicalEquality && !CanonicalOnly ? new Dictionary<T, int>() : null;
            int misses = 0;
            int r = 0;
            foreach (ReadOnlyMemory<T> segment in segments)
            {
                foreach (T value in segment.Span)
                {
                    if (value is null && nullable)
                    {
                        keys[r++] = 0;
                        continue;
                    }

                    if (seen is null)
                    {
                        keys[r++] = table.Intern(inner.Canonical(value, ref scratch));
                        continue;
                    }

                    ref int key = ref CollectionsMarshal.GetValueRefOrAddDefault(seen, value, out bool exists);
                    if (!exists)
                    {
                        key = table.Intern(inner.Canonical(value, ref scratch));
                        misses++;
                    }

                    keys[r++] = key;
                    if (r == ProbeRows && misses * 2 > ProbeRows && !AlwaysClr)
                    {
                        seen = null;
                    }
                }
            }

            int size = table.Count;
            int code = LowCardinalityWire.SelectKeyWidthCode(size);
            writer.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)code);
            writer.WriteUInt64((ulong)size);
            for (int i = 0; i < size; i++)
            {
                inner.Encode(writer, table.Entry(i));
            }

            writer.WriteUInt64((ulong)rows);
            for (int i = 0; i < rows; i++)
            {
                LowCardinalityWire.WriteKey(writer, code, keys[i]);
            }
        }
        finally
        {
            ArrayPool<int>.Shared.Return(keys);
        }
    }
}

/// <summary>An open-addressing set of byte runs, stored end to end in one blob.</summary>
internal sealed class ByteInterner
{
    private byte[] blob = new byte[1024];
    private int blobLength;
    private int[] starts = new int[64];
    private int[] hashes = new int[64];
    private int[] buckets; // entry index + 1, 0 = empty
    private int count;

    // Starts small and grows with the distinct count. Sizing it from the row count put a 512 KB bucket array on
    // the large-object heap for every write, even for 100 distinct values.
    public ByteInterner()
    {
        buckets = new int[64];
    }

    public int Count => count;

    public ReadOnlySpan<byte> Entry(int index) => blob.AsSpan(starts[index], End(index) - starts[index]);

    private int firstIndexed;

    public void AddUnindexed(ReadOnlySpan<byte> value)
    {
        Append(value, Hash(value), insert: false);
        firstIndexed = count;
    }

    public int Intern(ReadOnlySpan<byte> value)
    {
        int hash = Hash(value);
        int mask = buckets.Length - 1;
        for (int b = hash & mask; ; b = (b + 1) & mask)
        {
            int slot = buckets[b];
            if (slot == 0)
            {
                return Append(value, hash, insert: true, bucket: b);
            }

            int index = slot - 1;
            if (hashes[index] == hash && Entry(index).SequenceEqual(value))
            {
                return index;
            }
        }
    }

    private int End(int index) => index + 1 < count ? starts[index + 1] : blobLength;

    private int Append(ReadOnlySpan<byte> value, int hash, bool insert, int bucket = -1)
    {
        if (count == starts.Length)
        {
            Array.Resize(ref starts, count * 2);
            Array.Resize(ref hashes, count * 2);
        }

        if (blobLength + value.Length > blob.Length)
        {
            Array.Resize(ref blob, Math.Max(blob.Length * 2, blobLength + value.Length));
        }

        value.CopyTo(blob.AsSpan(blobLength));
        starts[count] = blobLength;
        hashes[count] = hash;
        blobLength += value.Length;
        int index = count++;

        if (insert)
        {
            buckets[bucket] = index + 1;
            if (count * 2 > buckets.Length)
            {
                Rehash();
            }
        }

        return index;
    }

    private void Rehash()
    {
        buckets = new int[buckets.Length * 2];
        int mask = buckets.Length - 1;
        for (int i = firstIndexed; i < count; i++)
        {
            int b = hashes[i] & mask;
            while (buckets[b] != 0)
            {
                b = (b + 1) & mask;
            }

            buckets[b] = i + 1;
        }
    }

    private static int Hash(ReadOnlySpan<byte> value)
    {
        var hash = default(HashCode);
        hash.AddBytes(value);
        return hash.ToHashCode();
    }
}

// ---------------------------------------------------------------- derivation

internal static class WriteDerivation
{
    private static readonly ConcurrentDictionary<(string, Type), object> Cache = new();

    private static readonly Dictionary<(string, Type), Func<TypeNode, object>> Leaves = new()
    {
        [("DateTime", typeof(DateTimeOffset))] = static _ => new FixedLeafWriter<DateTimeOffset, uint, OffsetToDateTime, UInt32Encoder>(0),
        [("String", typeof(string))] = static _ => StringFromText.Instance,
        [("String", typeof(byte[]))] = static _ => new BytesFromArray(0),
        [("FixedString", typeof(byte[]))] = static node => new BytesFromArray(int.Parse(node.Arguments[0].Name)),
        [("FixedString", typeof(string))] = static node => new FixedStringFromText(int.Parse(node.Arguments[0].Name)),
    };

    public static ColumnWriter<T> Derive<T>(string type)
        => (ColumnWriter<T>)Cache.GetOrAdd((type, typeof(T)), key => Derive(TypeParser.Parse(key.Item1), key.Item2)
            ?? throw new InvalidCastException($"spike: no write of {key.Item2} into '{key.Item1}'"));

    private static object Derive(TypeNode node, Type source)
    {
        switch (node.Name)
        {
            case "Nullable":
            {
                Type underlying = Nullable.GetUnderlyingType(source);
                return underlying is null
                    ? null // spike: reference-type Nullable writes not implemented
                    : Wrap(typeof(LiftValueWriter<>), underlying, Derive(node.Arguments[0], underlying));
            }

            case "Array":
                return source.IsArray
                    ? Wrap(typeof(EachWriter<>), source.GetElementType(), Derive(node.Arguments[0], source.GetElementType()))
                    : null;

            case "LowCardinality":
            {
                TypeNode innerNode = node.Arguments[0];
                bool nullable = innerNode.Name == "Nullable";
                if (nullable)
                {
                    innerNode = innerNode.Arguments[0];
                }

                object inner = Derive(innerNode, source);
                return inner is null
                    ? null
                    : Activator.CreateInstance(typeof(DictionaryWriter<>).MakeGenericType(source), inner, nullable);
            }

            default:
                return Leaves.TryGetValue((node.Name, source), out var make) ? make(node) : null;
        }
    }

    private static object Wrap(Type combinator, Type argument, object inner)
        => inner is null ? null : Activator.CreateInstance(combinator.MakeGenericType(argument), inner);
}
