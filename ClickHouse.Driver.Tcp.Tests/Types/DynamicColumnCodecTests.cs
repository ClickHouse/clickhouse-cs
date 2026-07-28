using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class DynamicColumnCodecTests
{
    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    // Captured verbatim from a ClickHouse server (FORMAT Native, flattened serialization) for a Dynamic column
    // holding [42::UInt64, 'hi'::String, NULL]. String sorts before UInt64, so discriminator 0 = String, 1 =
    // UInt64, and NULL is the discriminator equal to the type count (2). The version (3) and type list are the
    // state prefix; the discriminators and per-type runs are the body.
    private static readonly byte[] DocumentedBytes =
    {
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // state prefix: serialization version = 3 (flattened)
        0x02,                                           // num_types = 2
        0x06, 0x53, 0x74, 0x72, 0x69, 0x6E, 0x67,       // type[0] = "String"
        0x06, 0x55, 0x49, 0x6E, 0x74, 0x36, 0x34,       // type[1] = "UInt64"
        0x01, 0x00, 0x02,                               // discriminators: 1 (UInt64), 0 (String), 2 (NULL)
        0x02, 0x68, 0x69,                               // String run (1 value): len = 2, "hi"
        0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // UInt64 run (1 value): 42
    };

    [Test]
    public async Task WriteFull_ErgonomicColumn_ProducesTheDocumentedBytes()
    {
        IColumnCodec codec = Resolve("Dynamic");
        var column = new ArrayColumn<object>("d", "Dynamic", new object[] { 42UL, "hi", null });

        // The inferred type list is name-sorted (String before UInt64), matching the server's canonicalization.
        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteFull(w, column));

        CollectionAssert.AreEqual(DocumentedBytes, bytes);
    }

    [Test]
    public async Task WriteFull_DenseColumnReadBack_RoundTripsToIdenticalBytes()
    {
        IColumnCodec codec = Resolve("Dynamic");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn dense = await codec.ReadColumnAsync(reader, "d", "Dynamic", 3, CodecTestHarness.None);

        // The read-back DynamicColumn is the zero-copy write source: writing it reproduces the exact bytes.
        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteFull(w, dense));

        CollectionAssert.AreEqual(DocumentedBytes, bytes);
    }

    [Test]
    public async Task WriteStatePrefixThenColumn_SeparateStateFreeCalls_ProducesTheDocumentedBytes()
    {
        IColumnCodec codec = Resolve("Dynamic");
        var column = new ArrayColumn<object>("d", "Dynamic", new object[] { 42UL, "hi", null });

        // The state-free prefix and body calls each recompute the (deterministic) type list independently; the
        // combined output must still match the shared-state path.
        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            codec.WriteStatePrefix(w, column);
            codec.WriteColumn(w, column);
        });

        CollectionAssert.AreEqual(DocumentedBytes, bytes);
    }

    [Test]
    public async Task ReadColumn_DocumentedBytes_ReconstructsValuesAndSurfacesTheRuntimeTypeList()
    {
        // Decoding the golden vector: the type list and discriminators are the dynamic-structure surface no
        // integration test can see (it only calls GetValue), and keeping the value assertions alongside them
        // guards the decoder against drifting even if a future server emits a different type ordering. The
        // values on their own are covered by the "Dynamic [scalars + null]" case in InsertRoundTripCase.
        IColumnCodec codec = Resolve("Dynamic");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn column = await codec.ReadColumnAsync(reader, "d", "Dynamic", 3, CodecTestHarness.None);

        var dynamic = (IDynamicColumn)column;
        Assert.Multiple(() =>
        {
            Assert.That(dynamic.TypeCount, Is.EqualTo(2));
            Assert.That(dynamic.TypeNames, Is.EqualTo(new[] { "String", "UInt64" }));
            Assert.That(dynamic.Discriminators.ToArray(), Is.EqualTo(new[] { 1, 0, 2 }));
            Assert.That(column.RowCount, Is.EqualTo(3));
            Assert.That(column.GetValue(0), Is.EqualTo(42UL));
            Assert.That(column.GetValue(1), Is.EqualTo("hi"));
            Assert.That(column.GetValue(2), Is.Null);
        });
    }

    [Test]
    public void ReadStatePrefix_VersionNotFlattened_Throws()
    {
        IColumnCodec codec = Resolve("Dynamic");

        // Version 2 is the non-flat native default, which this client does not decode.
        byte[] bytes = { 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None));
    }

    [Test]
    public async Task ReadColumn_DiscriminatorPastTypeCount_Throws()
    {
        IColumnCodec codec = Resolve("Dynamic");

        // Prefix declares two types; a discriminator of 5 selects neither a type (0, 1) nor NULL (2).
        byte[] prefix =
        {
            0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02,
            0x06, 0x53, 0x74, 0x72, 0x69, 0x6E, 0x67,
            0x06, 0x55, 0x49, 0x6E, 0x74, 0x36, 0x34,
            0x05, // discriminator out of range
        };
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(prefix);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);

        Assert.ThrowsAsync<FormatException>(async () => await codec.ReadColumnAsync(reader, "d", "Dynamic", 1, CodecTestHarness.None));
    }

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumn()
    {
        IColumnCodec codec = Resolve("Dynamic");

        // A zero-row block carries no prefix and no body, so read straight from an empty buffer.
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(Array.Empty<byte>());
        using IColumn column = await codec.ReadColumnAsync(reader, "d", "Dynamic", 0, CodecTestHarness.None);

        Assert.That(column.RowCount, Is.Zero);
    }

    [Test]
    public void Create_MaxTypesArgument_IsAccepted()
    {
        IColumnCodec codec = Resolve("Dynamic(max_types=5)");
        Assert.That(codec.TypeName, Is.EqualTo("Dynamic(max_types=5)"));
    }

    [Test]
    public void Indexer_RowPastRowCount_ThrowsRatherThanReadingStaleDiscriminators()
    {
        // The discriminator buffer is normally a pooled array longer than the column, so a row past RowCount read a
        // stale value from its tail — and the outcome depended on the leftover: a stale NULL discriminator (which is
        // just typeColumns.Length, here 1) reported the row as an existing NULL, while any other value fell through to
        // the exactly-sized local-index array and threw. Both spellings must be a bounds failure.
        using var alternative = new ArrayColumn<long>("d", "Int64", new[] { 7L });
        var discriminators = new[] { 0, 1, 0 };
        using var column = new DynamicColumn(
            "d", "Dynamic", new[] { "Int64" }, discriminators, new IColumn[] { alternative }, rowCount: 1, pooledDiscriminators: false, ownsColumns: false);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(7L));
            Assert.That(() => column[1], Throws.InstanceOf<IndexOutOfRangeException>(), "a stale NULL discriminator must not read as an existing NULL row");
            Assert.That(() => column[2], Throws.InstanceOf<IndexOutOfRangeException>());
        });
    }

    [Test]
    public void Constructor_DiscriminatorsShorterThanRowCount_Throws()
    {
        // rowCount is load-bearing here — each child holds only the rows that selected it, and a NULL row takes a slot
        // in none of them — so it is validated rather than derived.
        using var alternative = new ArrayColumn<long>("d", "Int64", new[] { 7L });

        Assert.That(
            () => new DynamicColumn("d", "Dynamic", new[] { "Int64" }, new[] { 0 }, new IColumn[] { alternative }, rowCount: 2, pooledDiscriminators: false, ownsColumns: false),
            Throws.ArgumentException.With.Message.Contains("fewer than"));
    }

    [Test]
    public void Create_UnknownArgument_Throws()
        => Assert.Throws<FormatException>(() => Resolve("Dynamic(max_sizes=5)"));
}
