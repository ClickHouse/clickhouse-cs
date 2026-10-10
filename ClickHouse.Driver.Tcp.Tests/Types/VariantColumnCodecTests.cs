using System;
using System.Collections.Generic;
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
        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, dense, 0, 5, prefix: true);

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

    // Two or more alternatives can take the same CLR type: JSON and String both take a string, and DateTime64, Int64 and
    // Time64 all take a long. No alternative claims such a value, so the insert refuses it. The message names the
    // alternatives and does not tell the caller what to use, because the caller cannot say which alternative is meant.
    [TestCaseSource(nameof(AmbiguousAlternativeCases))]
    public void WriteColumn_ValueWhoseClrTypeSeveralAlternativesShare_ThrowsNamingThem(string type, object ambiguous, string[] expectedNames)
    {
        IColumnCodec codec = Resolve(type);
        var column = new ArrayColumn<object>("v", type, new[] { ambiguous });

        ArgumentException refusal = Assert.ThrowsAsync<ArgumentException>(
            async () => await CodecTestHarness.WriteSliceAsync(codec, column, 0, column.RowCount));
        Assert.Multiple(() =>
        {
            foreach (string name in expectedNames)
            {
                Assert.That(refusal.Message, Does.Contain($"'{name}'"));
            }

            Assert.That(refusal.Message, Does.Not.Contain("VariantColumn"));
        });
    }

    // The refusal above is per value, not per column: a column of object values does not tell the runtime types that it
    // holds, so a refusal of the column would also refuse each value that only one alternative takes. A UInt64 in
    // Variant(JSON, String, UInt64) and a string in Variant(DateTime64(3), Int64, String, Time64(3)) are written.
    [TestCaseSource(nameof(UnambiguousAlternativeCases))]
    public async Task WriteColumn_ValueWhoseClrTypeOneAlternativeHas_WritesEvenWhenOthersCollide(string type, object unambiguous)
    {
        IColumnCodec codec = Resolve(type);
        var column = new ArrayColumn<object>("v", type, new[] { unambiguous });

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, column, 0, column.RowCount);
        Assert.That(bytes, Is.Not.Empty);
    }

    // The codec writes a dense column only when its discriminators name the same alternatives in the same order, and
    // the codec of each alternative writes the child column from its storage. An insert gives each other column to the
    // converter layer.
    [Test]
    public void CanWrite_DenseColumn_IsTrueOnlyForTheSameAlternativesWithStoredChildren()
    {
        IColumnCodec codec = Resolve(StringUInt64);
        using IColumn text = DecodedColumns.Of("v", "String", "hi");
        using IColumn numbers = DecodedColumns.Of("v", "UInt64", 42UL);
        using var callerNumbers = new ArrayColumn<ulong>("v", "UInt64", new ulong[] { 42 });

        using var same = new VariantColumn(
            "v", StringUInt64, new byte[] { 1, 0 }, new[] { text, numbers },
            rowCount: 2, pooledDiscriminators: false, ownsColumns: false);
        using var otherOrder = new VariantColumn(
            "v", "Variant(UInt64, String)", new byte[] { 0, 1 }, new[] { numbers, text },
            rowCount: 2, pooledDiscriminators: false, ownsColumns: false);
        using var callerChild = new VariantColumn(
            "v", StringUInt64, new byte[] { 1, 0 }, new IColumn[] { text, callerNumbers },
            rowCount: 2, pooledDiscriminators: false, ownsColumns: false);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(same), Is.True, "the decoded children in the order of the type");
            Assert.That(codec.CanWrite(otherOrder), Is.False, "the same alternatives in another order");
            Assert.That(codec.CanWrite(callerChild), Is.False, "a child column that the caller built");
        });
    }

    // A dense column of the same alternatives keeps its discriminators, so the codec writes a value that the converter
    // layer cannot place by its CLR type: JSON and String both take a string.
    [Test]
    public async Task WriteColumn_DenseColumnOfThisVariant_WritesWhereScatteringTheSameValuesCouldNotChoose()
    {
        const string type = "Variant(JSON, String, UInt64)";
        IColumnCodec codec = Resolve(type);
        using IColumn json = DecodedColumns.Of("v", "JSON", Array.Empty<string>());
        using IColumn text = DecodedColumns.Of("v", "String", "hi");
        using IColumn numbers = DecodedColumns.Of("v", "UInt64", Array.Empty<ulong>());
        using var dense = new VariantColumn(
            "v", type, new byte[] { 1 }, new[] { json, text, numbers },
            rowCount: 1, pooledDiscriminators: false, ownsColumns: false);

        var boxed = new ArrayColumn<object>("v", type, new object[] { "hi" });

        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, dense, 0, 1, prefix: true);

        Assert.Multiple(() =>
        {
            // The discriminators mode, the JSON version, the discriminator of String (1), the String run "hi".
            Assert.That(Convert.ToHexString(bytes), Is.EqualTo("0000000000000000" + "0100000000000000" + "01" + "026869"));

            Assert.ThrowsAsync<ArgumentException>(
                async () => await CodecTestHarness.WriteSliceAsync(codec, boxed, 0, boxed.RowCount),
                "the same value boxed is ambiguous between JSON and String, which is what makes the dense path observable");
        });
    }

    private static IEnumerable<TestCaseData> AmbiguousAlternativeCases()
    {
        yield return new TestCaseData("Variant(JSON, String, UInt64)", (object)"{}", new[] { "JSON", "String" })
            .SetName("JSON and String are both string");

        // Three alternatives, not two: an odd number of alternatives that take the same CLR type is also a refusal, and
        // the value does not go to the last of them.
        yield return new TestCaseData("Variant(DateTime64(3), Int64, Time64(3))", (object)5L, new[] { "DateTime64(3)", "Int64", "Time64(3)" })
            .SetName("Int64, DateTime64 and Time64 are all long");
    }

    private static IEnumerable<TestCaseData> UnambiguousAlternativeCases()
    {
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

        // Slice rows [1, 3): "hi" (String) and NULL. The discriminators are 00 FF. The String run holds only the value
        // in the slice ("hi", not "yo" of row 4). The UInt64 run is empty, because its rows (0 and 3) are outside the
        // slice.
        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, dense, 1, 2);

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

        // Slice rows [3, 5): 7 (UInt64) and "yo" (String). Each row is the second value of its run, so each run starts
        // at offset 1. A slice that starts at row 0 cannot show this.
        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, dense, 3, 2);

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
