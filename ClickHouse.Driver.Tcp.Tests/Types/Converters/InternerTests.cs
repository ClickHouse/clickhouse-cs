using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Covers the LowCardinality interners: the reserved slots, the float bit patterns, growth, the lone-surrogate case
/// and the CLR-key fast path with its probe. A dictionary that an interner builds and a leaf encodes gives the bytes
/// of the current LowCardinality write.
/// </summary>
[TestFixture]
public class InternerTests
{
    private static ColumnWriter<T> Leaf<T>(string type) => ConverterDerivation.Default.Writer<T>(type, ConverterHarness.Context);

    [Test]
    public void FixedInterner_SignedZeros_GetTwoEntries()
    {
        var floats = (FixedLeafWriter<double, ulong>)Leaf<double>("Float64");
        using var interner = new FixedInterner<ulong>(floats.Placeholder, nullable: false);

        int positive = interner.Intern(floats.ToCanonical(0.0, 0));
        int negative = interner.Intern(floats.ToCanonical(-0.0, 1));

        Assert.Multiple(() =>
        {
            Assert.That(positive, Is.EqualTo(0), "+0 is the placeholder");
            Assert.That(negative, Is.EqualTo(1));
            Assert.That(interner.Count, Is.EqualTo(2));
        });
    }

    [Test]
    public void FixedInterner_NaNPayloads_GetAnEntryEach()
    {
        var floats = (FixedLeafWriter<float, uint>)Leaf<float>("Float32");
        using var interner = new FixedInterner<uint>(floats.Placeholder, nullable: false);
        float quiet = BitConverter.UInt32BitsToSingle(0x7FC0_0000);
        float payload = BitConverter.UInt32BitsToSingle(0x7FC0_0001);
        float negative = BitConverter.UInt32BitsToSingle(0xFFC0_0003);

        int[] keys = new[] { quiet, payload, negative, payload, quiet }.Select((value, i) => interner.Intern(floats.ToCanonical(value, i))).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(new[] { 1, 2, 3, 2, 1 }), "an equal bit pattern shares an entry; another payload does not");
            Assert.That(interner.Entries.ToArray(), Is.EqualTo(new uint[] { 0, 0x7FC0_0000, 0x7FC0_0001, 0xFFC0_0003 }));
        });
    }

    [Test]
    public void FixedInterner_NotNullable_ReservesThePlaceholderAtSlotZero()
    {
        using var interner = new FixedInterner<int>(7, nullable: false);

        Assert.Multiple(() =>
        {
            Assert.That(interner.Entries.ToArray(), Is.EqualTo(new[] { 7 }));
            Assert.That(interner.Intern(7), Is.EqualTo(0));
            Assert.That(interner.Intern(8), Is.EqualTo(1));
        });
    }

    [Test]
    public void FixedInterner_Nullable_ReservesTheNullSlotAndThePlaceholder()
    {
        using var interner = new FixedInterner<int>(7, nullable: true);

        Assert.Multiple(() =>
        {
            Assert.That(interner.Entries.ToArray(), Is.EqualTo(new[] { 7, 7 }));
            Assert.That(interner.Intern(7), Is.EqualTo(1), "a value equal to the placeholder takes the placeholder slot, not the NULL slot");
            Assert.That(interner.Intern(8), Is.EqualTo(2));
        });
    }

    [Test]
    public void FixedInterner_ManyDistinctValues_KeepsTheOrderOfFirstUse()
    {
        using var interner = new FixedInterner<long>(0, nullable: false);
        int[] keys = Enumerable.Range(1, 10_000).Select(i => interner.Intern(i * 3L)).ToArray();
        int[] again = Enumerable.Range(1, 10_000).Select(i => interner.Intern(i * 3L)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(Enumerable.Range(1, 10_000)));
            Assert.That(again, Is.EqualTo(keys));
            Assert.That(interner.Entries.ToArray(), Is.EqualTo(new[] { 0L }.Concat(Enumerable.Range(1, 10_000).Select(i => i * 3L))));
        });
    }

    [Test]
    public void FixedInterner_Disposed_RefusesToIntern()
    {
        var interner = new FixedInterner<int>(0, nullable: false);
        interner.Dispose();
        interner.Dispose();

        Assert.Throws<ObjectDisposedException>(() => interner.Intern(1));
    }

    [Test]
    public void ByteInterner_ReservedSlots_AreAsForTheFixedInterner()
    {
        using var plain = new ByteInterner("p"u8, nullable: false);
        using var nullable = new ByteInterner("p"u8, nullable: true);

        Assert.Multiple(() =>
        {
            Assert.That(plain.Count, Is.EqualTo(1));
            Assert.That(plain.Intern("p"u8), Is.EqualTo(0));
            Assert.That(nullable.Count, Is.EqualTo(2));
            Assert.That(nullable.Entry(0).ToArray(), Is.EqualTo("p"u8.ToArray()));
            Assert.That(nullable.Intern("p"u8), Is.EqualTo(1));
            Assert.That(nullable.Intern("q"u8), Is.EqualTo(2));
        });
    }

    /// <summary>Enough distinct values to grow every buffer and to rehash many times. The keys stay in first-use order.</summary>
    [Test]
    public void ByteInterner_ManyDistinctValues_GrowsAndKeepsEveryEntry()
    {
        using var interner = new ByteInterner(ReadOnlySpan<byte>.Empty, nullable: false);
        byte[][] values = Enumerable.Range(0, 100_000).Select(i => Encoding.UTF8.GetBytes($"value-{i}-{new string('x', i % 50)}")).ToArray();

        int[] keys = values.Select(value => interner.Intern(value)).ToArray();
        int[] again = values.Select(value => interner.Intern(value)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(Enumerable.Range(1, values.Length)));
            Assert.That(again, Is.EqualTo(keys));
            Assert.That(interner.Count, Is.EqualTo(values.Length + 1));
            Assert.That(interner.Intern(ReadOnlySpan<byte>.Empty), Is.EqualTo(0), "the empty value is the placeholder");
            Assert.That(Enumerable.Range(0, values.Length).All(i => interner.Entry(i + 1).SequenceEqual(values[i])), Is.True);
        });
    }

    [Test]
    public void ByteInterner_EntryOutOfRange_Throws()
    {
        using var interner = new ByteInterner(ReadOnlySpan<byte>.Empty, nullable: false);

        Assert.Throws<ArgumentOutOfRangeException>(() => interner.Entry(1));
    }

    [Test]
    public void ByteInterner_Disposed_RefusesToIntern()
    {
        var interner = new ByteInterner(ReadOnlySpan<byte>.Empty, nullable: false);
        interner.Dispose();
        interner.Dispose();

        Assert.Throws<ObjectDisposedException>(() => interner.Intern("a"u8));
    }

    /// <summary>
    /// Each lone surrogate encodes as EF BF BD, so two different strings have the same canonical bytes and share one
    /// entry. The current writer keys on the string and gives them two entries. Both dictionaries read back the same.
    /// </summary>
    [Test]
    public async Task ClrKeyedByteInterner_TwoLoneSurrogates_ShareOneEntry()
    {
        string[] values = { "\uD800", "\uDBFF", "\uD800" };
        using var interner = new ClrKeyedByteInterner<string>((BytesLeafWriter<string>)Leaf<string>("String"), nullable: false);

        int[] keys = values.Select((value, i) => interner.Intern(value, i)).ToArray();
        byte[] current = await ConverterHarness.WriteOldAsync("LowCardinality(String)", values, 0, values.Length);

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(new[] { 1, 1, 1 }));
            Assert.That(interner.Entries.Count, Is.EqualTo(2));
            Assert.That(interner.Entries.Entry(1).ToArray(), Is.EqualTo(new byte[] { 0xEF, 0xBF, 0xBD }));
            Assert.That(DictionarySize(current), Is.EqualTo(3), "the current writer keeps an entry for each string");
        });
    }

    /// <summary>When the first values do not repeat, the probe turns the CLR lookup off. The keys stay correct after it.</summary>
    [Test]
    public void ClrKeyedByteInterner_DistinctValues_TurnsTheClrLookupOffAfterTheProbe()
    {
        using var interner = new ClrKeyedByteInterner<string>((BytesLeafWriter<string>)Leaf<string>("String"), nullable: false);
        int probe = ClrKeyedByteInterner<string>.ProbeValues;

        int[] first = Enumerable.Range(0, probe - 1).Select(i => interner.Intern($"v{i}", i)).ToArray();
        bool onBefore = interner.UsesClrKeys;
        int last = interner.Intern($"v{probe - 1}", probe - 1);
        bool onAfter = interner.UsesClrKeys;
        int repeated = interner.Intern("v7", probe);

        Assert.Multiple(() =>
        {
            Assert.That(onBefore, Is.True);
            Assert.That(onAfter, Is.False);
            Assert.That(first.Append(last), Is.EqualTo(Enumerable.Range(1, probe)));
            Assert.That(repeated, Is.EqualTo(8), "the byte interner still finds a value that the CLR lookup knew");
        });
    }

    [Test]
    public void ClrKeyedByteInterner_RepeatingValues_KeepsTheClrLookup()
    {
        using var interner = new ClrKeyedByteInterner<string>((BytesLeafWriter<string>)Leaf<string>("String"), nullable: false);
        int[] keys = Enumerable.Range(0, 5_000).Select(i => interner.Intern($"v{i % 100}", i)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(interner.UsesClrKeys, Is.True);
            Assert.That(keys.Take(100), Is.EqualTo(Enumerable.Range(1, 100)));
            Assert.That(keys.Skip(100), Is.EqualTo(Enumerable.Range(0, 4_900).Select(i => (i % 100) + 1)));
        });
    }

    /// <summary>Exactly half misses is not more than half, so the lookup stays on.</summary>
    [Test]
    public void ClrKeyedByteInterner_HalfOfTheProbeMisses_KeepsTheClrLookup()
    {
        using var interner = new ClrKeyedByteInterner<string>((BytesLeafWriter<string>)Leaf<string>("String"), nullable: false);
        int half = ClrKeyedByteInterner<string>.ProbeValues / 2;
        for (int i = 0; i < ClrKeyedByteInterner<string>.ProbeValues; i++)
        {
            interner.Intern($"v{i % half}", i);
        }

        Assert.That(interner.UsesClrKeys, Is.True);
    }

    /// <summary>A byte[] has no CLR equality that implies equal bytes, so only the byte interner keys it.</summary>
    [Test]
    public void ClrKeyedByteInterner_ByteArrays_KeyOnTheBytesOnly()
    {
        using var interner = new ClrKeyedByteInterner<byte[]>((BytesLeafWriter<byte[]>)Leaf<byte[]>("String"), nullable: false);

        int first = interner.Intern(new byte[] { 1, 2 }, 0);
        int second = interner.Intern(new byte[] { 1, 2 }, 1);

        Assert.Multiple(() =>
        {
            Assert.That(interner.UsesClrKeys, Is.False);
            Assert.That(second, Is.EqualTo(first), "equal bytes in two arrays share an entry");
        });
    }

    /// <summary>A value that the leaf refuses leaves no key behind, so the next lookup of it fails again.</summary>
    [Test]
    public void ClrKeyedByteInterner_RefusedValue_LeavesNoClrKey()
    {
        using var interner = new ClrKeyedByteInterner<string>(new RefusingLeaf("bad"), nullable: false);

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentException>(() => interner.Intern("bad", 0));
            Assert.Throws<ArgumentException>(() => interner.Intern("bad", 1));
            Assert.That(interner.Intern("good", 2), Is.EqualTo(1));
            Assert.That(interner.UsesClrKeys, Is.True);
        });
    }

    [Test]
    public void ClrKeyedByteInterner_NullString_ThrowsAsTheStringWriteDoes()
    {
        using var interner = new ClrKeyedByteInterner<string>((BytesLeafWriter<string>)Leaf<string>("String"), nullable: false);

        var thrown = Assert.Throws<ArgumentNullException>(() => interner.Intern(null, 0));
        Assert.That(thrown.ParamName, Is.EqualTo("value"));
    }

    // The dictionary that an interner builds and a leaf encodes gives the bytes of the current LowCardinality write:
    // the same entries, in the same order, with the same key width.
    [Test]
    public async Task Dictionary_StringsFromText_GiveTheCurrentLowCardinalityBytes()
    {
        string[] values = Enumerable.Range(0, 600).Select(i => i % 7 == 0 ? string.Empty : $"value {i % 300}").ToArray();
        var leaf = (BytesLeafWriter<string>)Leaf<string>("String");
        using var interner = new ClrKeyedByteInterner<string>(leaf, nullable: false);
        int[] keys = values.Select((value, i) => interner.Intern(value, i)).ToArray();

        byte[] expected = await ConverterHarness.WriteOldAsync("LowCardinality(String)", values, 0, values.Length);
        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteBytesDictionary(w, leaf, interner.Entries, keys));

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task Dictionary_NullableStrings_GiveTheCurrentLowCardinalityBytes()
    {
        string[] values = { "a", null, string.Empty, "a", null, "b" };
        var leaf = (BytesLeafWriter<string>)Leaf<string>("String");
        using var interner = new ClrKeyedByteInterner<string>(leaf, nullable: true);
        int[] keys = values.Select((value, i) => value is null ? 0 : interner.Intern(value, i)).ToArray();

        byte[] expected = await ConverterHarness.WriteOldAsync("LowCardinality(Nullable(String))", values, 0, values.Length);
        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteBytesDictionary(w, leaf, interner.Entries, keys));

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task Dictionary_FixedStringBytes_GiveTheCurrentLowCardinalityBytes()
    {
        byte[][] values = { "ab"u8.ToArray(), new byte[2], "ab"u8.ToArray(), "cd"u8.ToArray() };
        var leaf = (BytesLeafWriter<byte[]>)Leaf<byte[]>("FixedString(2)");
        using var interner = new ClrKeyedByteInterner<byte[]>(leaf, nullable: false);
        int[] keys = values.Select((value, i) => interner.Intern(value, i)).ToArray();

        byte[] expected = await ConverterHarness.WriteOldAsync("LowCardinality(FixedString(2))", values, 0, values.Length);
        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteBytesDictionary(w, leaf, interner.Entries, keys));

        Assert.That(actual, Is.EqualTo(expected));
    }

    /// <summary>
    /// <c>FixedString</c> from text keys on the padded bytes, with the CLR lookup: the bytes of the current write of
    /// the padded bytes.
    /// </summary>
    [Test]
    public async Task Dictionary_FixedStringFromText_GivesTheBytesOfThePaddedBytes()
    {
        string[] values = { "ab", string.Empty, "ab", "é", "\uD800", "\uDBFF", "é" };
        var leaf = (BytesLeafWriter<string>)Leaf<string>("FixedString(3)");
        using var interner = new ClrKeyedByteInterner<string>(leaf, nullable: false);
        int[] keys = values.Select((value, i) => interner.Intern(value, i)).ToArray();

        byte[] expected = await ConverterHarness.WriteOldAsync(
            "LowCardinality(FixedString(3))",
            values.Select(text =>
            {
                var bytes = new byte[3];
                Encoding.UTF8.GetBytes(text, bytes);
                return bytes;
            }).ToArray(),
            0,
            values.Length);
        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteBytesDictionary(w, leaf, interner.Entries, keys));

        Assert.Multiple(() =>
        {
            Assert.That(interner.UsesClrKeys, Is.True);
            Assert.That(actual, Is.EqualTo(expected));
        });
    }

    [Test]
    public Task Dictionary_FloatsWithSignedZerosAndNaNPayloads_GiveTheCurrentLowCardinalityBytes()
        => AssertFixedDictionaryAsync(
            "Float64",
            new[] { 0.0, -0.0, double.NaN, BitConverter.UInt64BitsToDouble(0x7FF8_0000_0000_0001), -0.0, double.NaN, 1.5 });

    [Test]
    public Task Dictionary_InstantsConvertedToSeconds_GiveTheCurrentLowCardinalityBytes()
        => AssertFixedDictionaryAsync(
            "DateTime('UTC')",
            new[] { DateTimeOffset.UnixEpoch, new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(2)), new DateTimeOffset(2024, 1, 15, 8, 30, 0, TimeSpan.Zero) });

    /// <summary>A dictionary of 300 entries needs two-byte keys, as the current writer chooses.</summary>
    [Test]
    public Task Dictionary_ManyEntries_GiveTheCurrentKeyWidth()
        => AssertFixedDictionaryAsync("UInt32", Enumerable.Range(0, 300).Select(i => (uint)(i * 7)).ToArray());

    [Test]
    public async Task Dictionary_NullableBytes_GiveTheCurrentLowCardinalityBytes()
    {
        byte?[] values = { 3, null, 0, 3, null, 9 };
        var leaf = (FixedLeafWriter<byte, byte>)Leaf<byte>("UInt8");
        using var interner = new FixedInterner<byte>(leaf.Placeholder, nullable: true);
        int[] keys = values.Select((value, i) => value is null ? 0 : interner.Intern(leaf.ToCanonical(value.Value, i))).ToArray();

        byte[] expected = await ConverterHarness.WriteOldAsync("LowCardinality(Nullable(UInt8))", values, 0, values.Length);
        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteFixedDictionary<byte, byte>(w, interner, keys));

        Assert.That(actual, Is.EqualTo(expected));
    }

    private static Task AssertFixedDictionaryAsync<T>(string type, T[] values)
        => (Task)ConverterHarness.InvokeGeneric(
            typeof(InternerTests),
            nameof(AssertFixedDictionaryCoreAsync),
            new[] { typeof(T), Leaf<T>(type).GetType().BaseType.GenericTypeArguments[1] },
            type,
            values);

    private static async Task AssertFixedDictionaryCoreAsync<T, TCanon>(string type, T[] values)
        where TCanon : unmanaged, IEquatable<TCanon>
    {
        var leaf = (FixedLeafWriter<T, TCanon>)Leaf<T>(type);
        using var interner = new FixedInterner<TCanon>(leaf.Placeholder, nullable: false);
        var canonical = new TCanon[values.Length];
        leaf.ToCanonical(values, canonical, 0);
        int[] keys = canonical.Select(interner.Intern).ToArray();

        byte[] expected = await ConverterHarness.WriteOldAsync($"LowCardinality({type})", values, 0, values.Length);
        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteFixedDictionary<T, TCanon>(w, interner, keys));

        Assert.That(actual, Is.EqualTo(expected));
    }

    // The LowCardinality prefix and body around a dictionary: the version, the metadata word, the entries, the keys.
    private static void WriteFixedDictionary<T, TCanon>(ClickHouseBinaryWriter writer, FixedInterner<TCanon> interner, int[] keys)
        where TCanon : unmanaged, IEquatable<TCanon>
    {
        int code = WriteHeader(writer, interner.Count);
        FixedLeafWriter<T, TCanon>.Encode(writer, interner.Entries);
        WriteKeys(writer, code, keys);
    }

    private static void WriteBytesDictionary<T>(ClickHouseBinaryWriter writer, BytesLeafWriter<T> leaf, ByteInterner interner, int[] keys)
    {
        int code = WriteHeader(writer, interner.Count);
        for (int i = 0; i < interner.Count; i++)
        {
            leaf.Encode(writer, interner.Entry(i));
        }

        WriteKeys(writer, code, keys);
    }

    private static int WriteHeader(ClickHouseBinaryWriter writer, int size)
    {
        int code = LowCardinalityWire.SelectKeyWidthCode(size);
        writer.WriteInt64(LowCardinalityWire.StatePrefixVersion);
        writer.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)code);
        writer.WriteUInt64((ulong)size);
        return code;
    }

    private static void WriteKeys(ClickHouseBinaryWriter writer, int code, int[] keys)
    {
        writer.WriteUInt64((ulong)keys.Length);
        foreach (int key in keys)
        {
            LowCardinalityWire.WriteKey(writer, code, key);
        }
    }

    // The dictionary size of a LowCardinality write: after the version (8 bytes) and the metadata word (8 bytes).
    private static long DictionarySize(byte[] bytes) => BitConverter.ToInt64(bytes, 16);

    // A leaf that refuses one value, to show that the interner keeps no key for a refused value.
    private sealed class RefusingLeaf : BytesLeafWriter<string>
    {
        private readonly string refused;

        public RefusingLeaf(string refused) => this.refused = refused;

        public override ReadOnlySpan<byte> Placeholder => ReadOnlySpan<byte>.Empty;

        public override bool ClrEqualityImpliesCanonicalEquality => true;

        public override ReadOnlySpan<byte> ToCanonical(string value, int position, ref byte[] scratch)
            => value == refused ? throw new ArgumentException($"'{value}' is refused.") : Encoding.UTF8.GetBytes(value);

        public override void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<byte> canonical) => writer.WriteString(canonical);

        public override void Write(ClickHouseBinaryWriter writer, ValueSource<string> values, IColumnWriteState state)
            => throw new NotSupportedException();
    }
}
