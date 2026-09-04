using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class VariantColumnCodecTests
{
    private const string StringUInt64 = "Variant(String, UInt64)";

    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    // The example documented on VariantColumnCodec: Variant(String, UInt64) with [42, 'hi', NULL, 7, 'yo']. String
    // sorts before UInt64, so discriminator 0 = String, 1 = UInt64. Each run holds its own rows in row order, so a
    // multi-value run also pins that ordering. Verified against a ClickHouse 26.6 SELECT ... FORMAT Native.
    private static readonly byte[] DocumentedBytes =
    {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // state prefix: discriminators mode = 0 (BASIC)
        0x01, 0x00, 0xFF, 0x01, 0x00,                   // discriminators: UInt64, String, NULL, UInt64, String
        0x02, 0x68, 0x69,                               // String run, rows 1 and 4: len = 2, "hi"
        0x02, 0x79, 0x6F,                               //                          len = 2, "yo"
        0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // UInt64 run, rows 0 and 3: 42
        0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //                           7
    };

    [Test]
    public async Task WriteStatePrefixAndColumn_DocumentedExample_ProducesTheDocumentedBytes()
    {
        IColumnCodec codec = Resolve(StringUInt64);
        var column = new ArrayColumn<object>("v", StringUInt64, new object[] { 42UL, "hi", null, 7UL, "yo" });

        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, column);
            codec.WriteColumn(w, column);
        });

        CollectionAssert.AreEqual(DocumentedBytes, bytes);
    }

    [Test]
    public async Task ReadColumn_DocumentedBytes_ReconstructsValuesAndNull()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn column = await codec.ReadColumnAsync(reader, "v", StringUInt64, 5, CodecTestHarness.None);

        Assert.That(column.RowCount, Is.EqualTo(5));
        Assert.That(column.GetValue(0), Is.EqualTo(42UL));
        Assert.That(column.GetValue(1), Is.EqualTo("hi"));
        Assert.That(column.GetValue(2), Is.Null);

        // Rows past the NULL: each addresses the second value of its run, so these also pin the per-row local index.
        Assert.That(column.GetValue(3), Is.EqualTo(7UL));
        Assert.That(column.GetValue(4), Is.EqualTo("yo"));
    }

    [Test]
    public async Task WriteColumn_DenseColumnReadBack_RoundTripsToIdenticalBytes()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn dense = await codec.ReadColumnAsync(reader, "v", StringUInt64, 5, CodecTestHarness.None);

        // The read-back VariantColumn is the zero-copy write source: writing it must reproduce the exact bytes.
        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, dense);
            codec.WriteColumn(w, dense);
        });

        CollectionAssert.AreEqual(DocumentedBytes, bytes);
    }

    [Test]
    public void ReadStatePrefix_CompactDiscriminatorsMode_ThrowsProtocolException()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        // Mode 1 = COMPACT, which this client does not implement.
        byte[] compactPrefix = { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(compactPrefix);

        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () => await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None));
    }

    [Test]
    public void ReadColumn_DiscriminatorPastAlternativeCount_ThrowsProtocolException()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        // Discriminator 5 selects no declared alternative (only 0 and 1 exist).
        byte[] bytes = { 0x05 };
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);

        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () => await codec.ReadColumnAsync(reader, "v", StringUInt64, 1, CodecTestHarness.None));
    }

    [Test]
    public void Create_NullableAlternative_Throws()
        => Assert.Throws<FormatException>(() => Resolve("Variant(String, Nullable(UInt64))"));

    [Test]
    public void Create_NoArguments_Throws()
        => Assert.Throws<FormatException>(() => Resolve("Variant()"));

    [Test]
    public void Create_DynamicAlternative_Throws()
        => Assert.Throws<FormatException>(() => Resolve("Variant(String, Dynamic)"));

    [Test]
    public void WriteColumn_ValueWithNoMatchingAlternative_Throws()
    {
        IColumnCodec codec = Resolve(StringUInt64);
        var column = new ArrayColumn<object>("v", StringUInt64, new object[] { 3.14 }); // double matches neither String nor UInt64

        Assert.ThrowsAsync<ArgumentException>(async () => await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column)));
    }

    // The refusal lists what the variant does take, so it must include a type a collision settles: IPv4 and IPv6
    // both surface IPAddress, and Variant(IPv4, IPv6, String) does write an address of either family. Listing only
    // the alternatives that own their CLR type outright would report IPAddress as unsupported.
    [Test]
    public void WriteColumn_ValueWithNoMatchingAlternative_NamesTheTypesACollisionSettles()
    {
        const string type = "Variant(IPv4, IPv6, String)";
        IColumnCodec codec = Resolve(type);
        var column = new ArrayColumn<object>("v", type, new object[] { 3.14 });

        ArgumentException refusal = Assert.ThrowsAsync<ArgumentException>(
            async () => await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column)));
        Assert.Multiple(() =>
        {
            Assert.That(refusal.Message, Does.Contain(typeof(IPAddress).ToString()));
            Assert.That(refusal.Message, Does.Contain(typeof(string).ToString()));
        });
    }

    // Several alternatives can share a CLR element type even though the server forbids duplicate alternative types
    // — they only have to surface the same one. JSON and String are both string; Int64, DateTime64 and Time64 are
    // all long; Geometry collides twice over (Ring/LineString, Polygon/MultiLineString). No alternative claims
    // such a value, and picking one would store it as the wrong type with no error. The message names the
    // alternatives it could not choose between, and prescribes nothing: there is no way for a caller to say which
    // one is meant.
    [TestCaseSource(nameof(AmbiguousAlternativeCases))]
    public void WriteColumn_ValueWhoseClrTypeSeveralAlternativesShare_ThrowsNamingThem(string type, object ambiguous, string[] expectedNames)
    {
        IColumnCodec codec = Resolve(type);
        var column = new ArrayColumn<object>("v", type, new[] { ambiguous });

        ArgumentException refusal = Assert.ThrowsAsync<ArgumentException>(
            async () => await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column)));
        Assert.Multiple(() =>
        {
            foreach (string name in expectedNames)
            {
                Assert.That(refusal.Message, Does.Contain($"'{name}'"));
            }

            Assert.That(refusal.Message, Does.Not.Contain("VariantColumn"));
        });
    }

    // A collision the value itself settles: IPv4 and IPv6 both surface IPAddress, but an address carries its
    // family, so exactly one alternative claims it and the write goes through. This is the only one of the four
    // collision families a value-level test can resolve — a string says nothing about JSON versus String, a long
    // nothing about Int64 versus DateTime64, and a Ring nothing about LineString.
    [TestCase("127.0.0.1", 0, TestName = "An IPv4 address goes to the IPv4 alternative")]
    [TestCase("::1", 1, TestName = "An IPv6 address goes to the IPv6 alternative")]
    public async Task WriteColumn_IpValueWhoseFamilyNamesOneAlternative_WritesThatDiscriminator(string address, byte expectedDiscriminator)
    {
        const string Type = "Variant(IPv4, IPv6, String)";
        IColumnCodec codec = Resolve(Type);
        var column = new ArrayColumn<object>("v", Type, new object[] { IPAddress.Parse(address) });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));
        Assert.That(bytes[0], Is.EqualTo(expectedDiscriminator));
    }

    // The refusal above is per value, not per column: an IColumn<object> says nothing about the runtime types it
    // holds, so refusing the whole column would also reject every unambiguous value in it. A string is unambiguous
    // in Variant(IPv4, IPv6, String) and a UInt64 is in Variant(JSON, String, UInt64), and both still write.
    [TestCaseSource(nameof(UnambiguousAlternativeCases))]
    public async Task WriteColumn_ValueWhoseClrTypeOneAlternativeHas_WritesEvenWhenOthersCollide(string type, object unambiguous)
    {
        IColumnCodec codec = Resolve(type);
        var column = new ArrayColumn<object>("v", type, new[] { unambiguous });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));
        Assert.That(bytes, Is.Not.Empty);
    }

    // The dense path pairs the column's alternative i with the codec's alternative i and reuses the column's own
    // discriminators, so it is only correct when the two lists are the same list. The gate compared the counts
    // alone, which let a column of one two-alternative Variant be written as another: every discriminator then
    // named the wrong type, and the failure came from inside the body, after the discriminators had gone out.
    [Test]
    public async Task WriteColumn_DenseColumnOfAnotherVariant_WritesThisCodecsDiscriminatorsAndNotTheColumnsOwn()
    {
        IColumnCodec codec = Resolve(StringUInt64);
        using var numbers = new ArrayColumn<ulong>("v", "UInt64", new ulong[] { 42 });
        using var text = new ArrayColumn<string>("v", "String", new[] { "hi" });

        // As a column read from a Variant(UInt64, String) arrives: its alternative 0 is UInt64, where this codec's
        // alternative 0 is String. Row 0 selected UInt64, row 1 String.
        using var dense = new VariantColumn(
            "v", "Variant(UInt64, String)", new byte[] { 0, 1 }, new IColumn[] { numbers, text },
            rowCount: 2, pooledDiscriminators: false, ownsColumns: false);

        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, dense);
            codec.WriteColumn(w, dense);
        });

        Assert.That(bytes, Is.EqualTo(new byte[]
        {
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // state prefix: discriminators mode = 0 (BASIC)
            0x01, 0x00,                                     // row 0 is the UInt64 (1 here), row 1 the String (0)
            0x02, 0x68, 0x69,                               // String run: len = 2, "hi"
            0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // UInt64 run: 42
        }));
    }

    // The other side of that gate: a column whose alternatives *are* this codec's must still take the dense path.
    // Where the two paths differ is a value whose CLR type several alternatives share — the dense column already
    // records which alternative each row selected, while scattering the same values by runtime type cannot choose.
    // A check that rejected a column it should accept would silently downgrade every such insert to a refusal.
    [Test]
    public void WriteColumn_DenseColumnOfThisVariant_WritesWhereScatteringTheSameValuesCouldNotChoose()
    {
        const string type = "Variant(JSON, String, UInt64)";
        IColumnCodec codec = Resolve(type);
        using var json = new ArrayColumn<string>("v", "JSON", Array.Empty<string>());
        using var text = new ArrayColumn<string>("v", "String", new[] { "hi" });
        using var numbers = new ArrayColumn<ulong>("v", "UInt64", Array.Empty<ulong>());
        using var dense = new VariantColumn(
            "v", type, new byte[] { 1 }, new IColumn[] { json, text, numbers },
            rowCount: 1, pooledDiscriminators: false, ownsColumns: false);

        var boxed = new ArrayColumn<object>("v", type, new object[] { "hi" });

        Assert.Multiple(() =>
        {
            Assert.DoesNotThrowAsync(async () => await CodecTestHarness.WriteAsync(w =>
            {
                codec.WriteStatePrefix(w, dense);
                codec.WriteColumn(w, dense);
            }));

            Assert.ThrowsAsync<ArgumentException>(
                async () => await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, boxed)),
                "the same value boxed is ambiguous between JSON and String, which is what makes the dense path observable");
        });
    }

    private static IEnumerable<TestCaseData> AmbiguousAlternativeCases()
    {
        yield return new TestCaseData("Variant(JSON, String, UInt64)", (object)"{}", new[] { "JSON", "String" })
            .SetName("JSON and String are both string");

        // Three, not two. Striking a colliding type from the map must not free the key for the next alternative to
        // claim: an odd-sized collision group would then resolve to its last member and write every such value as
        // that alternative. All three of these surface the raw long the wire carries.
        yield return new TestCaseData("Variant(DateTime64(3), Int64, Time64(3))", (object)5L, new[] { "DateTime64(3)", "Int64", "Time64(3)" })
            .SetName("Int64, DateTime64 and Time64 are all long");

        // The structural pairs inside Geometry, which no value-level test can separate.
        yield return new TestCaseData("Geometry", (object)new[] { (0d, 0d), (1d, 1d) }, new[] { "LineString", "Ring" })
            .SetName("LineString and Ring are both Array(Point)");
    }

    private static IEnumerable<TestCaseData> UnambiguousAlternativeCases()
    {
        yield return new TestCaseData("Variant(IPv4, IPv6, String)", (object)"abc").SetName("String beside the colliding IP pair");
        yield return new TestCaseData("Variant(JSON, String, UInt64)", (object)7UL).SetName("UInt64 beside the colliding text pair");
        yield return new TestCaseData("Variant(DateTime64(3), Int64, String, Time64(3))", (object)"abc").SetName("String beside the colliding long trio");
    }

    [Test]
    public async Task WriteColumn_DenseColumnSlice_WritesOnlyTheSlicedRowsAndTheirValues()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn dense = await codec.ReadColumnAsync(reader, "v", StringUInt64, 5, CodecTestHarness.None);

        // Slice rows [1, 3): "hi" (String) and NULL. The discriminators are 00 FF; the String run is cut to just
        // the in-slice value ("hi", leaving out "yo" at row 4), and the UInt64 run is empty because both its rows
        // (0 and 3) fall outside the slice.
        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, dense, 1, 2));

        byte[] expected = { 0x00, 0xFF, 0x02, 0x68, 0x69 };
        CollectionAssert.AreEqual(expected, bytes);
    }

    [Test]
    public async Task WriteColumn_DenseColumnSliceAfterEarlierValues_StartsEachRunAtItsSliceOffset()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn dense = await codec.ReadColumnAsync(reader, "v", StringUInt64, 5, CodecTestHarness.None);

        // Slice rows [3, 5): 7 (UInt64) and "yo" (String). Both rows are the *second* value of their run, so each
        // run must be written from offset 1 — the case a slice starting at row 0 cannot catch.
        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, dense, 3, 2));

        byte[] expected =
        {
            0x01, 0x00,                                     // discriminators: UInt64, String
            0x02, 0x79, 0x6F,                               // String run from offset 1: "yo"
            0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // UInt64 run from offset 1: 7
        };
        CollectionAssert.AreEqual(expected, bytes);
    }

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumn()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        // A zero-row block carries no prefix and no body, so read straight from an empty buffer.
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(Array.Empty<byte>());
        using IColumn column = await codec.ReadColumnAsync(reader, "v", StringUInt64, 0, CodecTestHarness.None);

        Assert.That(column.RowCount, Is.Zero);
    }

    [Test]
    public void Indexer_RowPastRowCount_ThrowsRatherThanReadingStaleDiscriminators()
    {
        // The discriminator buffer is normally a pooled array longer than the column, so a row past RowCount read a
        // stale byte from its tail — and the outcome depended on the leftover value: a stale 255 (the NULL
        // discriminator) reported the row as an existing NULL, while any other value fell through to the
        // exactly-sized local-index array and threw. Both spellings must be a bounds failure.
        using var alternative = new ArrayColumn<uint>("v", "UInt32", new uint[] { 7 });
        var discriminators = new byte[] { 0, IVariantColumn.NullDiscriminator, 0 };
        using var variant = new VariantColumn(
            "v", "Variant(UInt32)", discriminators, new IColumn[] { alternative }, rowCount: 1, pooledDiscriminators: false, ownsColumns: false);

        Assert.Multiple(() =>
        {
            Assert.That(variant[0], Is.EqualTo(7u));
            Assert.That(() => variant[1], Throws.InstanceOf<IndexOutOfRangeException>(), "a stale NULL discriminator must not read as an existing NULL row");
            Assert.That(() => variant[2], Throws.InstanceOf<IndexOutOfRangeException>());
        });
    }

    [Test]
    public void Constructor_DiscriminatorsShorterThanRowCount_Throws()
    {
        // rowCount is load-bearing here — each child holds only the rows that selected it, and a NULL row takes a slot
        // in none of them — so it is validated rather than derived.
        using var alternative = new ArrayColumn<uint>("v", "UInt32", new uint[] { 7 });

        Assert.That(
            () => new VariantColumn("v", "Variant(UInt32)", new byte[] { 0 }, new IColumn[] { alternative }, rowCount: 2, pooledDiscriminators: false, ownsColumns: false),
            Throws.ArgumentException.With.Message.Contains("fewer than"));
    }

    [Test]
    public void RestrictOwnership_DisposesOnlyFlaggedTypeColumns()
    {
        // The mechanism the partial densify rebuild relies on: after RestrictOwnership, Dispose frees exactly the
        // alternative columns flagged true (the freshly built ones) and leaves the rest (borrowed) untouched.
        var owned = new DisposeSpyColumn<uint>("v", "UInt32", new uint[] { 1 });
        var borrowed = new DisposeSpyColumn<int>("v", "Int32", new[] { 2 });
        var variant = new VariantColumn("v", "Variant(UInt32, Int32)", new byte[] { 0, 1 }, new IColumn[] { owned, borrowed }, rowCount: 2, pooledDiscriminators: false, ownsColumns: false);

        variant.RestrictOwnership(new[] { true, false });
        variant.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(owned.DisposeCount, Is.EqualTo(1), "a flagged (freshly built) alternative column must be disposed exactly once");
            Assert.That(borrowed.DisposeCount, Is.EqualTo(0), "an unflagged (borrowed) alternative column must not be disposed");
        });
    }
}
