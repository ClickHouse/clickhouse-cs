using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class ArrayColumnCodecTests
{
    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    [Test]
    public async Task Values_AfterMaterializing_AgreesWithTheIndexer()
    {
        // Per-type row values are covered against a real server by the Array(T) cases in InsertRoundTripCase.
        // What those cannot reach is the warm-cache branch: ArrayValueColumn.Values materializes a cache, while
        // the indexer reads `cache is not null ? cache[row] : Materialize(row)`, and AssertColumnsEqual only ever
        // calls GetValue on a cold column — so the indexer's cached branch runs nowhere else. Reading Values
        // first and then the indexer pins the two against each other. TypeName is asserted here for the same
        // reason: the integration comparison never looks at it.
        IColumnCodec codec = Resolve("Array(UInt32)");
        var expected = new[] { new uint[] { 10, 20, 30 }, Array.Empty<uint>(), new uint[] { 40, 50 } };
        var column = new ArrayColumn<uint[]>("c", "Array(UInt32)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Array(UInt32)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(read.TypeName, Is.EqualTo("Array(UInt32)"));
            Assert.That(((IColumn<uint[]>)read).Values.ToArray(), Is.EqualTo(expected));
            for (int row = 0; row < expected.Length; row++)
            {
                Assert.That(read.GetValue(row), Is.EqualTo(expected[row]), $"row {row} through the warm cache");
            }
        });
    }

    [Test]
    public void Indexer_RowPastRowCount_ThrowsRatherThanReadingStaleOffsets()
    {
        // The offsets buffer is normally a pooled array longer than the column, and a previous, larger read leaves
        // monotonic offsets behind in the tail. Reading a row past RowCount through the raw buffer would therefore
        // hand back a real-looking slice of the inner column instead of failing — silent wrong data, not a crash.
        // Here rows 0 and 1 are the column; the 7 and 9 are the stale tail a longer read would have left.
        var inner = PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        var offsets = new[] { 0, 2, 5, 7, 9 };
        using var column = new ArrayValueColumn<uint>("c", "Array(UInt32)", inner, offsets, rowCount: 2, pooledOffsets: false);

        Assert.Multiple(() =>
        {
            Assert.That(column[1], Is.EqualTo(new uint[] { 3, 4, 5 }), "the last real row still materializes");
            Assert.That(() => column[2], Throws.InstanceOf<IndexOutOfRangeException>());
        });
    }

    [Test]
    public void Constructor_OffsetsShorterThanRowCountPlusOne_Throws()
    {
        // rowCount is load-bearing here — the inner column is flat and the offsets are pooled — so it is validated
        // rather than derived. One offset per row plus the leading zero is the minimum.
        var inner = PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 1, 2, 3 });

        Assert.That(
            () => new ArrayValueColumn<uint>("c", "Array(UInt32)", inner, new[] { 0, 3 }, rowCount: 2, pooledOffsets: false),
            Throws.ArgumentException.With.Message.Contains("fewer than"));
    }

    [Test]
    public async Task ReadColumn_EmptyColumn_ReadsZeroRowsWithoutConsumingBytes()
    {
        IColumnCodec codec = Resolve("Array(UInt32)");
        var column = new ArrayColumn<uint[]>("c", "Array(UInt32)", Array.Empty<uint[]>());

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, column, 0, 0);
        Assert.That(bytes, Is.Empty, "an empty array column writes no offsets and no values");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Array(UInt32)", 0, CodecTestHarness.None);
        Assert.That(read.RowCount, Is.Zero);
    }

    [Test]
    public void CanWrite_AcceptsOnlyMatchingArrayColumn()
    {
        IColumnCodec codec = Resolve("Array(UInt32)");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<uint[]>("c", "Array(UInt32)", new[] { new uint[] { 1 } })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<int[]>("c", "Array(Int32)", new[] { new[] { 1 } })), Is.False);
            Assert.That(codec.CanWrite(PrimitiveColumn<uint>.FromValues("c", "UInt32", new uint[] { 1 })), Is.False);
        });
    }

    [Test]
    public void CanWrite_NestedInnerWithoutDenseNamedFieldColumn_ReturnsFalse()
    {
        const string type = "Array(Nested(a UInt8))";
        const string nestedType = "Nested(a UInt8)";
        IColumnCodec codec = Resolve(type);

        // Flattening the ergonomic rows would leave only object[][] values, not Nested's named field columns.
        var ergonomic = new ArrayColumn<object[][][]>("c", type, Array.Empty<object[][][]>());
        var wrongDense = new ArrayValueColumn<object[][]>(
            "c",
            type,
            new ArrayColumn<object[][]>("c", nestedType, Array.Empty<object[][]>()),
            new[] { 0 },
            rowCount: 0,
            pooledOffsets: false);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(ergonomic), Is.False, "the row-oriented projection has lost the named fields");
            Assert.That(codec.CanWrite(wrongDense), Is.False, "a dense outer column still needs a real NestedColumn inner");
        });
    }

    [Test]
    public void ElementType_IsInnerElementTypeArray()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Resolve("Array(UInt32)").ElementType, Is.EqualTo(typeof(uint[])));
            Assert.That(Resolve("Array(String)").ElementType, Is.EqualTo(typeof(string[])));
            Assert.That(Resolve("Array(Nullable(UInt32))").ElementType, Is.EqualTo(typeof(uint?[])));
            Assert.That(Resolve("Array(Array(UInt8))").ElementType, Is.EqualTo(typeof(byte[][])));
        });
    }

    [Test]
    public void NullPlaceholder_IsEmptyInnerArray()
        => Assert.That(Resolve("Array(UInt32)").NullPlaceholder, Is.EqualTo(Array.Empty<uint>()));

    [Test]
    public async Task ReadColumn_NonMonotonicOffsets_ThrowsProtocol()
    {
        // Offsets must be non-decreasing; a decrease is corruption. Wire: two UInt64 offsets [2, 1].
        IColumnCodec codec = Resolve("Array(UInt32)");
        byte[] wire = new byte[16];
        BitConverter.TryWriteBytes(wire.AsSpan(0, 8), 2UL);
        BitConverter.TryWriteBytes(wire.AsSpan(8, 8), 1UL);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(wire);

        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () =>
            await codec.ReadColumnAsync(reader, "c", "Array(UInt32)", 2, CodecTestHarness.None));
    }

    [Test]
    public async Task ReadColumn_OffsetBeyondInt32_ThrowsProtocol()
    {
        // An offset larger than int.MaxValue cannot be addressed by this client and is rejected up front.
        IColumnCodec codec = Resolve("Array(UInt32)");
        byte[] wire = new byte[8];
        BitConverter.TryWriteBytes(wire.AsSpan(0, 8), (ulong)int.MaxValue + 1);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(wire);

        Assert.ThrowsAsync<ClickHouseTcpProtocolException>(async () =>
            await codec.ReadColumnAsync(reader, "c", "Array(UInt32)", 1, CodecTestHarness.None));
    }

    /// <summary>Null Array(T) rows are rejected for canonical and lifted element types.</summary>
    [TestCase("Array(UInt32)", false)]
    [TestCase("Array(DateTime('UTC'))", true)]
    public async Task WriteColumn_ErgonomicColumnWithANullRow_ThrowsNamingTheRowAndTheRemedy(string type, bool lifted)
    {
        IColumnCodec codec = Resolve(type);
        IColumn column = lifted
            ? new ArrayColumn<DateTime[]>("c", type, new[] { Array.Empty<DateTime>(), null })
            : new ArrayColumn<uint[]>("c", type, new[] { new uint[] { 1 }, null });

        ArgumentException thrown = null;
        await CodecTestHarness.WriteAsync(writer =>
            thrown = Assert.Throws<ArgumentException>(() => codec.WriteColumn(writer, column, 0, 2)));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain("null value at row 1").And.Contain("Array(Nullable(T))"));
            Assert.That(thrown.ParamName, Is.EqualTo("column"), "the argument at fault, not an internal local's name.");
        });
    }

    // The state-aware overloads take the state this codec's own BeginWrite returned, and nothing else. They used to
    // treat null as "the caller has none to share" and rebuild one, which meant a caller that lost or never opened
    // the state still got correct bytes, so the mistake could not be seen. Only the state-free overloads build
    // their own state now.
    [Test]
    public async Task WriteColumn_StateAwareOverloadGivenNoState_ThrowsArgument()
    {
        IColumnCodec codec = Resolve("Array(UInt32)");
        var column = new ArrayColumn<uint[]>("c", "Array(UInt32)", new[] { new uint[] { 1, 2 } });

        ArgumentException thrown = null;
        await CodecTestHarness.WriteAsync(writer =>
            thrown = Assert.Throws<ArgumentException>(() => codec.WriteColumn(writer, column, 0, 1, state: null)));

        Assert.That(thrown.Message, Does.Contain("Array(UInt32)"));
    }

    [Test]
    public async Task WriteStatePrefix_StateAwareOverloadGivenNoState_ThrowsArgument()
    {
        IColumnCodec codec = Resolve("Array(UInt32)");
        var column = new ArrayColumn<uint[]>("c", "Array(UInt32)", new[] { new uint[] { 1, 2 } });

        await CodecTestHarness.WriteAsync(writer =>
            Assert.Throws<ArgumentException>(() => codec.WriteStatePrefix(writer, column, 0, 1, state: null)));
    }

    [Test]
    public void Resolve_Array_StampsFullTypeName()
        => Assert.That(Resolve("Array(Nullable(String))").TypeName, Is.EqualTo("Array(Nullable(String))"));

    [TestCase("Array(Int32, Int32)")]
    [TestCase("Array()")]
    public void Resolve_WrongArgumentCount_ThrowsFormat(string type)
        => Assert.Throws<FormatException>(() => Resolve(type));

    [Test]
    public void Resolve_UnsupportedInner_ThrowsNotSupported()
        => Assert.Throws<NotSupportedException>(() => Resolve("Array(NoSuchType)"));
}
