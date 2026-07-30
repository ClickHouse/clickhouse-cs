using System;
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

    // The canonical example from the native-format notes: Variant(String, UInt64) with [42, 'hi', NULL]. String
    // sorts before UInt64, so discriminator 0 = String, 1 = UInt64.
    private static readonly byte[] DocumentedBytes =
    {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // state prefix: discriminators mode = 0 (BASIC)
        0x01, 0x00, 0xFF,                               // discriminators: 1 (UInt64), 0 (String), 255 (NULL)
        0x02, 0x68, 0x69,                               // String run (1 value): len = 2, "hi"
        0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // UInt64 run (1 value): 42
    };

    [Test]
    public async Task WriteStatePrefixAndColumn_DocumentedExample_ProducesTheDocumentedBytes()
    {
        IColumnCodec codec = Resolve(StringUInt64);
        var column = new ArrayColumn<object>("v", StringUInt64, new object[] { 42UL, "hi", null });

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
        using IColumn column = await codec.ReadColumnAsync(reader, "v", StringUInt64, 3, CodecTestHarness.None);

        Assert.That(column.RowCount, Is.EqualTo(3));
        Assert.That(column.GetValue(0), Is.EqualTo(42UL));
        Assert.That(column.GetValue(1), Is.EqualTo("hi"));
        Assert.That(column.GetValue(2), Is.Null);
    }

    [Test]
    public async Task WriteColumn_DenseColumnReadBack_RoundTripsToIdenticalBytes()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn dense = await codec.ReadColumnAsync(reader, "v", StringUInt64, 3, CodecTestHarness.None);

        // The read-back VariantColumn is the zero-copy write source: writing it must reproduce the exact bytes.
        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, dense);
            codec.WriteColumn(w, dense);
        });

        CollectionAssert.AreEqual(DocumentedBytes, bytes);
    }

    [Test]
    public void ReadStatePrefix_CompactDiscriminatorsMode_Throws()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        // Mode 1 = COMPACT, which this client does not implement.
        byte[] compactPrefix = { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(compactPrefix);

        Assert.ThrowsAsync<NotSupportedException>(async () => await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None));
    }

    [Test]
    public void ReadColumn_DiscriminatorPastAlternativeCount_Throws()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        // Discriminator 5 selects no declared alternative (only 0 and 1 exist).
        byte[] bytes = { 0x05 };
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);

        Assert.ThrowsAsync<FormatException>(async () => await codec.ReadColumnAsync(reader, "v", StringUInt64, 1, CodecTestHarness.None));
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

    [Test]
    public async Task WriteColumn_DenseColumnSlice_WritesOnlyTheSlicedRowsAndTheirValues()
    {
        IColumnCodec codec = Resolve(StringUInt64);

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn dense = await codec.ReadColumnAsync(reader, "v", StringUInt64, 3, CodecTestHarness.None);

        // Slice rows [1, 3): "hi" (String) and NULL. The discriminators are 00 FF; only the String run carries a
        // value ("hi"), and the UInt64 run is empty because row 0 (the only UInt64) is before the slice.
        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, dense, 1, 2));

        byte[] expected = { 0x00, 0xFF, 0x02, 0x68, 0x69 };
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
