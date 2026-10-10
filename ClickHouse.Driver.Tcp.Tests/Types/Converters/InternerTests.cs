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
/// and the CLR-key fast path with its probe. A dictionary that an interner builds and a leaf encodes gives pinned bytes:
/// the entries in the order of first use, and the key width of the dictionary size.
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
    /// entry.
    /// </summary>
    [Test]
    public void ClrKeyedByteInterner_TwoLoneSurrogates_ShareOneEntry()
    {
        string[] values = { "\uD800", "\uDBFF", "\uD800" };
        using var interner = new ClrKeyedByteInterner<string>((BytesLeafWriter<string>)Leaf<string>("String"), nullable: false);

        int[] keys = values.Select(value => interner.Intern(value)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(new[] { 1, 1, 1 }));
            Assert.That(interner.Entries.Count, Is.EqualTo(2));
            Assert.That(interner.Entries.Entry(1).ToArray(), Is.EqualTo(new byte[] { 0xEF, 0xBF, 0xBD }));
        });
    }

    /// <summary>When the first values do not repeat, the probe turns the CLR lookup off. The keys stay correct after it.</summary>
    /// <summary>
    /// Distinct values: the CLR lookup stops at the miss that passes half of the probe, as no later lookup of the probe can
    /// keep it, and the keys go on in order.
    /// </summary>
    [Test]
    public void ClrKeyedByteInterner_DistinctValues_TurnsTheClrLookupOffAtTheMissThatPassesHalfOfTheProbe()
    {
        using var interner = new ClrKeyedByteInterner<string>((BytesLeafWriter<string>)Leaf<string>("String"), nullable: false);
        int half = ClrKeyedByteInterner<string>.ProbeValues / 2;

        int[] first = Enumerable.Range(0, half).Select(i => interner.Intern($"v{i}")).ToArray();
        bool onAtHalf = interner.UsesClrKeys;
        int next = interner.Intern($"v{half}");
        bool onAfter = interner.UsesClrKeys;
        int[] later = Enumerable.Range(half + 1, half).Select(i => interner.Intern($"v{i}")).ToArray();
        int repeated = interner.Intern("v7");

        Assert.Multiple(() =>
        {
            Assert.That(onAtHalf, Is.True);
            Assert.That(onAfter, Is.False);
            Assert.That(first.Append(next).Concat(later), Is.EqualTo(Enumerable.Range(1, (2 * half) + 1)));
            Assert.That(repeated, Is.EqualTo(8), "the byte interner still finds a value that the CLR lookup knew");
        });
    }

    [Test]
    public void ClrKeyedByteInterner_RepeatingValues_KeepsTheClrLookup()
    {
        using var interner = new ClrKeyedByteInterner<string>((BytesLeafWriter<string>)Leaf<string>("String"), nullable: false);
        int[] keys = Enumerable.Range(0, 5_000).Select(i => interner.Intern($"v{i % 100}")).ToArray();

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
            interner.Intern($"v{i % half}");
        }

        Assert.That(interner.UsesClrKeys, Is.True);
    }

    /// <summary>A byte[] has no CLR equality that implies equal bytes, so only the byte interner keys it.</summary>
    [Test]
    public void ClrKeyedByteInterner_ByteArrays_KeyOnTheBytesOnly()
    {
        using var interner = new ClrKeyedByteInterner<byte[]>((BytesLeafWriter<byte[]>)Leaf<byte[]>("String"), nullable: false);

        int first = interner.Intern(new byte[] { 1, 2 });
        int second = interner.Intern(new byte[] { 1, 2 });

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
            Assert.Throws<ArgumentException>(() => interner.Intern("bad"));
            Assert.Throws<ArgumentException>(() => interner.Intern("bad"));
            Assert.That(interner.Intern("good"), Is.EqualTo(1));
            Assert.That(interner.UsesClrKeys, Is.True);
        });
    }

    /// <summary>
    /// After a probe of repeating values the CLR lookup stays on. A value that the leaf refuses then leaves no key behind,
    /// a repeated value is found, and a new value takes the next key.
    /// </summary>
    [Test]
    public void ClrKeyedByteInterner_RefusedValueAfterTheProbe_LeavesNoClrKey()
    {
        using var interner = new ClrKeyedByteInterner<string>(new RefusingLeaf("bad"), nullable: false);
        for (int i = 0; i < ClrKeyedByteInterner<string>.ProbeValues; i++)
        {
            interner.Intern("good");
        }

        Assert.Multiple(() =>
        {
            Assert.That(interner.UsesClrKeys, Is.True);
            Assert.Throws<ArgumentException>(() => interner.Intern("bad"));
            Assert.Throws<ArgumentException>(() => interner.Intern("bad"));
            Assert.That(interner.Intern("good"), Is.EqualTo(1));
            Assert.That(interner.Intern("other"), Is.EqualTo(2));
            Assert.That(interner.Intern("other"), Is.EqualTo(2));
        });
    }

    [Test]
    public void ClrKeyedByteInterner_NullString_ThrowsAsTheStringWriteDoes()
    {
        using var interner = new ClrKeyedByteInterner<string>((BytesLeafWriter<string>)Leaf<string>("String"), nullable: false);

        var thrown = Assert.Throws<ArgumentNullException>(() => interner.Intern(null));
        Assert.That(thrown.ParamName, Is.EqualTo("value"));
    }

    /// <summary>
    /// A Guid is looked up by itself, and converted to its wire bytes once for each distinct value. The keys are the keys
    /// of the canonical interner, which also holds the entries.
    /// </summary>
    [Test]
    public void ClrKeyedFixedInterner_RepeatingGuids_KeepsTheClrLookupAndTheCanonicalEntries()
    {
        var leaf = (FixedLeafWriter<Guid, UInt128>)Leaf<Guid>("UUID");
        using var interner = new ClrKeyedFixedInterner<Guid, UInt128>(leaf, nullable: false);
        Guid[] distinct = Enumerable.Range(1, 100).Select(i => new Guid(i, 0, 0, new byte[8])).ToArray();

        int[] keys = Enumerable.Range(0, 5_000).Select(i => interner.Intern(distinct[i % 100])).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(interner.UsesClrKeys, Is.True);
            Assert.That(keys, Is.EqualTo(Enumerable.Range(0, 5_000).Select(i => (i % 100) + 1)));
            Assert.That(interner.Entries.Entries.ToArray(), Is.EqualTo(new[] { UInt128.Zero }.Concat(distinct.Select((g, i) => leaf.ToCanonical(g, i)))));
        });
    }

    /// <summary>When the first values do not repeat, the probe turns the CLR lookup off. The keys stay correct after it.</summary>
    [Test]
    public void ClrKeyedFixedInterner_DistinctValues_TurnsTheClrLookupOffAtTheMissThatPassesHalfOfTheProbe()
    {
        using var interner = new ClrKeyedFixedInterner<Guid, UInt128>((FixedLeafWriter<Guid, UInt128>)Leaf<Guid>("UUID"), nullable: false);
        int half = ClrKeyedFixedInterner<Guid, UInt128>.ProbeValues / 2;
        Guid[] values = Enumerable.Range(1, (2 * half) + 1).Select(i => new Guid(i, 0, 0, new byte[8])).ToArray();

        int[] first = values.Take(half).Select(interner.Intern).ToArray();
        bool onAtHalf = interner.UsesClrKeys;
        int next = interner.Intern(values[half]);
        bool onAfter = interner.UsesClrKeys;
        int[] later = values.Skip(half + 1).Select(interner.Intern).ToArray();
        int repeated = interner.Intern(values[7]);

        Assert.Multiple(() =>
        {
            Assert.That(onAtHalf, Is.True);
            Assert.That(onAfter, Is.False);
            Assert.That(first.Append(next).Concat(later), Is.EqualTo(Enumerable.Range(1, (2 * half) + 1)));
            Assert.That(repeated, Is.EqualTo(8), "the canonical interner still finds a value that the CLR lookup knew");
        });
    }

    /// <summary>
    /// A leaf whose equal values can have other canonical values (a DateTime of another Kind, a float of another sign),
    /// and a leaf whose CLR value is its canonical value, have no CLR lookup.
    /// </summary>
    [TestCase("DateTime('UTC')", typeof(DateTime), typeof(uint))]
    [TestCase("Float64", typeof(double), typeof(ulong))]
    [TestCase("Int32", typeof(int), typeof(int))]
    public void ClrKeyedFixedInterner_LeafWithNoClrLookup_KeysOnTheCanonicalValue(string type, Type clrType, Type canonical)
        => ConverterHarness.InvokeGeneric(typeof(InternerTests), nameof(AssertNoClrLookup), new[] { clrType, canonical }, type);

    /// <summary>A value that the leaf refuses leaves no CLR key behind, so the next lookup of it fails again.</summary>
    [Test]
    public void ClrKeyedFixedInterner_RefusedValue_LeavesNoClrKey()
    {
        using var interner = new ClrKeyedFixedInterner<decimal, int>((FixedLeafWriter<decimal, int>)Leaf<decimal>("Decimal(3, 2)"), nullable: false);

        Assert.Multiple(() =>
        {
            Assert.Throws<OverflowException>(() => interner.Intern(100m));
            Assert.Throws<OverflowException>(() => interner.Intern(100m));
            Assert.That(interner.Intern(1.25m), Is.EqualTo(1));
            Assert.That(interner.Intern(1.250m), Is.EqualTo(1), "equal decimals of other scales share an entry");
            Assert.That(interner.UsesClrKeys, Is.True);
        });
    }

    /// <summary>
    /// After a probe of repeating values the CLR lookup stays on. A value out of the range of the leaf then leaves no key
    /// behind, a repeated value is found, and a new value takes the next key.
    /// </summary>
    [Test]
    public void ClrKeyedFixedInterner_RefusedValueAfterTheProbe_LeavesNoClrKey()
    {
        using var interner = new ClrKeyedFixedInterner<decimal, int>((FixedLeafWriter<decimal, int>)Leaf<decimal>("Decimal(3, 2)"), nullable: false);
        for (int i = 0; i < ClrKeyedFixedInterner<decimal, int>.ProbeValues; i++)
        {
            interner.Intern(1.25m);
        }

        Assert.Multiple(() =>
        {
            Assert.That(interner.UsesClrKeys, Is.True);
            Assert.Throws<OverflowException>(() => interner.Intern(100m));
            Assert.Throws<OverflowException>(() => interner.Intern(100m));
            Assert.That(interner.Intern(1.25m), Is.EqualTo(1));
            Assert.That(interner.Intern(2.5m), Is.EqualTo(2));
            Assert.That(interner.Intern(2.50m), Is.EqualTo(2), "equal decimals of other scales share an entry");
        });
    }

    /// <summary>
    /// A 16-byte canonical value is keyed by its bits: values that differ in one half get an entry each, and equal values
    /// share one.
    /// </summary>
    [Test]
    public void FixedInterner_WideValues_KeyOnBothHalves()
    {
        using var interner = new FixedInterner<UInt128>(UInt128.Zero, nullable: false);
        var values = new[] { new UInt128(1, 2), new UInt128(2, 1), new UInt128(1, 2), new UInt128(0, 2), UInt128.Zero, UInt128.MaxValue };

        int[] keys = values.Select(interner.Intern).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(new[] { 1, 2, 1, 3, 0, 4 }));
            Assert.That(interner.Entries.ToArray(), Is.EqualTo(new[] { UInt128.Zero, new UInt128(1, 2), new UInt128(2, 1), new UInt128(0, 2), UInt128.MaxValue }));
        });
    }

    // The dictionary that an interner builds and a leaf encodes, as a LowCardinality write gives it: the version, the
    // metadata word with the key width, the entries in the order of first use after the reserved slots, and the keys.
    // The pinned bytes are those of the LowCardinality write of the same values.

    /// <summary>
    /// 600 strings of 300 distinct values and the placeholder: 301 entries, so the keys take two bytes. The entries are
    /// the placeholder, then each value at its first use; each key is the slot of its value.
    /// </summary>
    [Test]
    public async Task Dictionary_ManyStringsFromText_GiveTheEntriesInTheOrderOfFirstUse()
    {
        string[] values = Enumerable.Range(0, 600).Select(i => i % 7 == 0 ? string.Empty : $"value {i % 300}").ToArray();
        var leaf = (BytesLeafWriter<string>)Leaf<string>("String");
        using var interner = new ClrKeyedByteInterner<string>(leaf, nullable: false);
        int[] keys = values.Select(value => interner.Intern(value)).ToArray();
        (List<string> entries, int[] expectedKeys) = FirstUse(values, string.Empty);

        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteBytesDictionary(w, leaf, interner.Entries, keys));

        Assert.Multiple(() =>
        {
            Assert.That(Convert.ToHexString(actual, 0, 24), Is.EqualTo("0100000000000000" + "0106000000000000" + "2D01000000000000"), "version, two-byte keys, 301 entries");
            Assert.That(actual, Has.Length.EqualTo(4123));
            Assert.That(Enumerable.Range(0, interner.Entries.Count).Select(i => Encoding.UTF8.GetString(interner.Entries.Entry(i))), Is.EqualTo(entries));
            Assert.That(keys, Is.EqualTo(expectedKeys));
        });
    }

    /// <summary>
    /// 300 distinct numbers, the first of them the placeholder 0: 300 entries, so the keys take two bytes, in the order of
    /// first use.
    /// </summary>
    [Test]
    public async Task Dictionary_ManyNumbers_GiveTwoByteKeysAndTheEntriesInTheOrderOfFirstUse()
    {
        uint[] values = Enumerable.Range(0, 300).Select(i => (uint)(i * 7)).ToArray();
        var leaf = (FixedLeafWriter<uint, uint>)Leaf<uint>("UInt32");
        using var interner = new FixedInterner<uint>(leaf.Placeholder, nullable: false);
        int[] keys = values.Select(interner.Intern).ToArray();
        (List<uint> entries, int[] expectedKeys) = FirstUse(values, 0u);

        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteFixedDictionary<uint, uint>(w, interner, keys));

        Assert.Multiple(() =>
        {
            Assert.That(Convert.ToHexString(actual, 0, 24), Is.EqualTo("0100000000000000" + "0106000000000000" + "2C01000000000000"), "version, two-byte keys, 300 entries");
            Assert.That(actual, Has.Length.EqualTo(1832));
            Assert.That(interner.Entries.ToArray(), Is.EqualTo(entries));
            Assert.That(keys, Is.EqualTo(expectedKeys));
        });
    }

    [Test]
    public async Task Dictionary_NullableStrings_GiveThePinnedBytes()
    {
        string[] values = { "a", null, string.Empty, "a", null, "b" };
        var leaf = (BytesLeafWriter<string>)Leaf<string>("String");
        using var interner = new ClrKeyedByteInterner<string>(leaf, nullable: true);
        int[] keys = values.Select(value => value is null ? 0 : interner.Intern(value)).ToArray();

        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteBytesDictionary(w, leaf, interner.Entries, keys));

        // The NULL slot and the placeholder are empty strings.
        Assert.That(Convert.ToHexString(actual), Is.EqualTo("0100000000000000" + "0006000000000000" + "0400000000000000" + "00" + "00" + "0161" + "0162" + "0600000000000000" + "020001020003"));
    }

    [Test]
    public async Task Dictionary_FixedStringBytes_GiveThePinnedBytes()
    {
        byte[][] values = { "ab"u8.ToArray(), new byte[2], "ab"u8.ToArray(), "cd"u8.ToArray() };
        var leaf = (BytesLeafWriter<byte[]>)Leaf<byte[]>("FixedString(2)");
        using var interner = new ClrKeyedByteInterner<byte[]>(leaf, nullable: false);
        int[] keys = values.Select(value => interner.Intern(value)).ToArray();

        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteBytesDictionary(w, leaf, interner.Entries, keys));

        // Two zero bytes are the placeholder in slot 0.
        Assert.That(Convert.ToHexString(actual), Is.EqualTo("0100000000000000" + "0006000000000000" + "0300000000000000" + "0000" + "6162" + "6364" + "0400000000000000" + "01000102"));
    }

    /// <summary>
    /// <c>FixedString</c> from text keys on the padded bytes, with the CLR lookup: <c>""</c> is the placeholder, two lone
    /// surrogates share the entry EF BF BD.
    /// </summary>
    [Test]
    public async Task Dictionary_FixedStringFromText_KeysOnThePaddedBytes()
    {
        string[] values = { "ab", string.Empty, "ab", "é", "\uD800", "\uDBFF", "é" };
        var leaf = (BytesLeafWriter<string>)Leaf<string>("FixedString(3)");
        using var interner = new ClrKeyedByteInterner<string>(leaf, nullable: false);
        int[] keys = values.Select(value => interner.Intern(value)).ToArray();

        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteBytesDictionary(w, leaf, interner.Entries, keys));

        Assert.Multiple(() =>
        {
            Assert.That(interner.UsesClrKeys, Is.True);
            Assert.That(
                Convert.ToHexString(actual),
                Is.EqualTo("0100000000000000" + "0006000000000000" + "0400000000000000" + "000000" + "616200" + "C3A900" + "EFBFBD" + "0700000000000000" + "01000102030302"));
        });
    }

    /// <summary>
    /// The placeholder of a wide FixedString is made for the write: the NULL slot and the placeholder are N zero bytes,
    /// then each text padded to N.
    /// </summary>
    [Test]
    public async Task Dictionary_WideFixedStringFromText_GivesThePaddedEntries()
    {
        const int size = 5_000;
        string[] values = { "a", new string('y', size), "a", string.Empty };
        var leaf = (BytesLeafWriter<string>)Leaf<string>($"FixedString({size})");
        using var interner = new ClrKeyedByteInterner<string>(leaf, nullable: true);
        int[] keys = values.Select(value => interner.Intern(value)).ToArray();
        byte[] expected = Header(4, code: 0)
            .Concat(new byte[size]).Concat(new byte[size]).Concat(Padded("a", size)).Concat(Encoding.UTF8.GetBytes(new string('y', size)))
            .Concat(BitConverter.GetBytes(4UL)).Concat(new byte[] { 2, 3, 2, 1 })
            .ToArray();

        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteBytesDictionary(w, leaf, interner.Entries, keys));

        Assert.Multiple(() =>
        {
            Assert.That(interner.Entries.Count, Is.EqualTo(4), "the NULL slot, the placeholder, \"a\" and the wide value");
            Assert.That(actual, Is.EqualTo(expected));
        });
    }

    /// <summary>
    /// Floats key on their bits: 0 (the placeholder), -0, the default NaN, a NaN with another payload and 1.5 are five
    /// entries.
    /// </summary>
    [Test]
    public Task Dictionary_FloatsWithSignedZerosAndNaNPayloads_GiveThePinnedBytes()
        => AssertFixedDictionaryAsync(
            "Float64",
            new[] { 0.0, -0.0, double.NaN, BitConverter.UInt64BitsToDouble(0x7FF8_0000_0000_0001), -0.0, double.NaN, 1.5 },
            "0100000000000000" + "0006000000000000" + "0500000000000000"
            + "0000000000000000" + "0000000000000080" + "000000000000F8FF" + "010000000000F87F" + "000000000000F83F"
            + "0700000000000000" + "00010203010204");

    /// <summary>Two instants of the same second share an entry: the keys are on the seconds, not on the offsets.</summary>
    [Test]
    public Task Dictionary_InstantsConvertedToSeconds_GiveThePinnedBytes()
        => AssertFixedDictionaryAsync(
            "DateTime('UTC')",
            new[] { DateTimeOffset.UnixEpoch, new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(2)), new DateTimeOffset(2024, 1, 15, 8, 30, 0, TimeSpan.Zero) },
            "0100000000000000" + "0006000000000000" + "0200000000000000" + "00000000" + "08EDA465" + "0300000000000000" + "000101");

    [Test]
    public async Task Dictionary_NullableBytes_GiveThePinnedBytes()
    {
        byte?[] values = { 3, null, 0, 3, null, 9 };
        var leaf = (FixedLeafWriter<byte, byte>)Leaf<byte>("UInt8");
        using var interner = new FixedInterner<byte>(leaf.Placeholder, nullable: true);
        int[] keys = values.Select((value, i) => value is null ? 0 : interner.Intern(leaf.ToCanonical(value.Value, i))).ToArray();

        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteFixedDictionary<byte, byte>(w, interner, keys));

        // The NULL slot and the placeholder are zero bytes.
        Assert.That(Convert.ToHexString(actual), Is.EqualTo("0100000000000000" + "0006000000000000" + "0400000000000000" + "00" + "00" + "03" + "09" + "0600000000000000" + "020001020003"));
    }

    private static Task AssertFixedDictionaryAsync<T>(string type, T[] values, string expected)
        => (Task)ConverterHarness.InvokeGeneric(
            typeof(InternerTests),
            nameof(AssertFixedDictionaryCoreAsync),
            new[] { typeof(T), Leaf<T>(type).GetType().BaseType.GenericTypeArguments[1] },
            type,
            values,
            expected);

    private static async Task AssertFixedDictionaryCoreAsync<T, TCanon>(string type, T[] values, string expected)
        where TCanon : unmanaged, IEquatable<TCanon>
    {
        var leaf = (FixedLeafWriter<T, TCanon>)Leaf<T>(type);
        using var interner = new FixedInterner<TCanon>(leaf.Placeholder, nullable: false);
        var canonical = new TCanon[values.Length];
        leaf.ToCanonical(values, canonical, 0);
        int[] keys = canonical.Select(interner.Intern).ToArray();

        byte[] actual = await CodecTestHarness.WriteAsync(w => WriteFixedDictionary<T, TCanon>(w, interner, keys));

        Assert.That(Convert.ToHexString(actual), Is.EqualTo(expected));
    }

    // The entries of a dictionary without NULL in the order of first use, the placeholder first, and the slot of each value.
    private static (List<T> Entries, int[] Keys) FirstUse<T>(T[] values, T placeholder)
    {
        var entries = new List<T> { placeholder };
        int[] keys = values.Select(value =>
        {
            int slot = entries.IndexOf(value);
            if (slot < 0)
            {
                entries.Add(value);
                slot = entries.Count - 1;
            }

            return slot;
        }).ToArray();
        return (entries, keys);
    }

    // The version, the metadata word with the key width code, and the dictionary size.
    private static byte[] Header(int size, int code)
        => BitConverter.GetBytes(1L).Concat(BitConverter.GetBytes(0x0600UL | (ulong)code)).Concat(BitConverter.GetBytes((ulong)size)).ToArray();

    private static byte[] Padded(string text, int size)
    {
        var bytes = new byte[size];
        Encoding.UTF8.GetBytes(text, bytes);
        return bytes;
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

    // A leaf that refuses one value, to show that the interner keeps no key for a refused value.
    private static void AssertNoClrLookup<T, TCanon>(string type)
        where TCanon : unmanaged, IEquatable<TCanon>
    {
        using var interner = new ClrKeyedFixedInterner<T, TCanon>((FixedLeafWriter<T, TCanon>)Leaf<T>(type), nullable: false);
        Assert.That(interner.UsesClrKeys, Is.False);
    }

    private sealed class RefusingLeaf : BytesLeafWriter<string>
    {
        private readonly string refused;

        public RefusingLeaf(string refused) => this.refused = refused;

        public override ReadOnlySpan<byte> GetPlaceholder(ref byte[] scratch) => ReadOnlySpan<byte>.Empty;

        public override void WritePlaceholder(ClickHouseBinaryWriter writer) => writer.WriteString(ReadOnlySpan<byte>.Empty);

        public override bool ClrEqualityImpliesCanonicalEquality => true;

        public override ReadOnlySpan<byte> ToCanonical(string value, int position, ref byte[] scratch)
            => value == refused ? throw new ArgumentException($"'{value}' is refused.") : Encoding.UTF8.GetBytes(value);

        public override void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<byte> canonical) => writer.WriteString(canonical);

        public override void Write(ClickHouseBinaryWriter writer, ValueSource<string> values, IColumnWriteState state)
            => throw new NotSupportedException();
    }
}
