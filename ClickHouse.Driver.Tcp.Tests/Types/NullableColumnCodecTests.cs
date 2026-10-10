using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class NullableColumnCodecTests
{
    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    [Test]
    public async Task Values_ValueAndReferenceInners_MaterializeEveryRowIncludingNulls()
    {
        // Per-type value/null round-tripping is covered against a real server by the Nullable(T) cases in
        // InsertRoundTripCase. What that cannot reach is this accessor: the integration comparison comes from
        // AssertColumnsEqual, which only ever calls GetValue(row), so the lazily-materialized ArrayPool-rented
        // Values cache on NullableValueColumn/NullableReferenceColumn is exercised nowhere else. Both the value
        // and reference shapes have their own copy of it, hence both here.
        IColumnCodec valueCodec = Resolve("Nullable(Int32)");
        var valueExpected = new int?[] { 7, null, -3, null, 0 };
        var valueColumn = new ArrayColumn<int?>("c", "Nullable(Int32)", valueExpected);

        IColumnCodec referenceCodec = Resolve("Nullable(String)");
        var referenceExpected = new[] { "hi", null, string.Empty, "world" };
        var referenceColumn = new ArrayColumn<string>("c", "Nullable(String)", referenceExpected);

        using IColumn valueRead = await CodecTestHarness.RoundTripAsync(valueCodec, valueColumn, "Nullable(Int32)", valueColumn.RowCount);
        using IColumn referenceRead = await CodecTestHarness.RoundTripAsync(referenceCodec, referenceColumn, "Nullable(String)", referenceColumn.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(((IColumn<int?>)valueRead).Values.ToArray(), Is.EqualTo(valueExpected));
            Assert.That(((IColumn<string>)referenceRead).Values.ToArray(), Is.EqualTo(referenceExpected));
        });
    }

    [Test]
    public async Task WriteColumn_DenseNullableColumn_RoundTripsWithoutRebuildingValues()
    {
        // A dense NullableValueColumn<T> (inner column + null-map, the wire's own layout) is the zero-copy write
        // path — the same shape a read produces and the row-materialization tier will build. Writing one and
        // reading it back must preserve the values and nulls.
        IColumnCodec codec = Resolve("Nullable(Int32)");
        var inner = PrimitiveColumn<int>.FromValues("c", "Int32", new[] { 7, 0, 9 });
        var dense = new NullableValueColumn<int>("c", "Nullable(Int32)", inner, new byte[] { 0, 1, 0 }, pooledMap: false);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, dense, "Nullable(Int32)", dense.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(dense), Is.True, "the codec writes the column from its storage");
            Assert.That(dense.RowCount, Is.EqualTo(3), "the row count comes from the inner column, not a separate argument");
            Assert.That(((IColumn<int?>)read).Values.ToArray(), Is.EqualTo(new int?[] { 7, null, 9 }));
        });
    }

    [Test]
    public void Constructor_NullMapShorterThanInner_Throws()
    {
        // The inner column sets the row count, so those two can no longer disagree; the null-map is the one input
        // that still can. A pooled map is routinely longer than the row count (and must stay accepted), so only a
        // short one is rejected — unchecked it would leave NullMap's slice and the indexer reading out of bounds.
        var inner = PrimitiveColumn<int>.FromValues("c", "Int32", new[] { 1, 2, 3 });
        var longer = new byte[8];
        var reference = new ArrayColumn<string>("c", "String", new[] { "a", "b", "c" });

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new NullableValueColumn<int>("c", "Nullable(Int32)", inner, new byte[] { 0, 1 }, pooledMap: false),
                Throws.ArgumentException.With.Message.Contains("shorter than"));
            Assert.That(
                () => new NullableReferenceColumn<string>("c", "Nullable(String)", reference, new byte[] { 0, 1 }, pooledMap: false),
                Throws.ArgumentException.With.Message.Contains("shorter than"));
            Assert.That(
                new NullableValueColumn<int>("c", "Nullable(Int32)", inner, longer, pooledMap: false).RowCount,
                Is.EqualTo(3),
                "an over-long pooled map is accepted and does not inflate the row count");
        });
    }

    [Test]
    public void ElementType_ReflectsInnerNullability()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Resolve("Nullable(Int32)").ElementType, Is.EqualTo(typeof(int?)));
            Assert.That(Resolve("Nullable(String)").ElementType, Is.EqualTo(typeof(string)));
        });
    }

    [Test]
    public async Task ReadColumn_NullableNothing_SurfacesEveryRowAsNull()
    {
        // Nullable(Nothing) is how a bare NULL literal is typed. Wire: null-map (all null) then one Nothing
        // placeholder byte per row. This is the read-only completion of the Nothing/Nullable(Nothing) pairing.
        IColumnCodec codec = Resolve("Nullable(Nothing)");
        byte[] wire = { 1, 1, 1, 0, 0, 0 };
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(wire);

        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Nullable(Nothing)", 3, CodecTestHarness.None);

        Assert.Multiple(() =>
        {
            Assert.That(read.RowCount, Is.EqualTo(3));
            Assert.That(read.GetValue(0), Is.Null);
            Assert.That(read.GetValue(2), Is.Null);
            Assert.That(codec.ElementType, Is.EqualTo(typeof(object)));
        });
    }

    [Test]
    public async Task CanWrite_NullableNothing_ReturnsFalse()
    {
        // The Nothing codec writes no column. Thus the Nullable(Nothing) codec does not write the column that it
        // reads.
        IColumnCodec codec = Resolve("Nullable(Nothing)");
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(new byte[] { 1, 1, 0, 0 });
        using IColumn decoded = await codec.ReadColumnAsync(reader, "c", "Nullable(Nothing)", 2, CodecTestHarness.None);

        Assert.That(codec.CanWrite(decoded), Is.False);
    }

    [Test]
    public void Resolve_NestedNullable_ThrowsFormat()
        => Assert.Throws<FormatException>(() => Resolve("Nullable(Nullable(Int32))"));

    [TestCase("Nullable(Int32, Int32)")]
    [TestCase("Nullable()")]
    public void Resolve_WrongArgumentCount_ThrowsFormat(string type)
        => Assert.Throws<FormatException>(() => Resolve(type));

    [Test]
    public void Resolve_UnsupportedInner_ThrowsNotSupported()
        => Assert.Throws<NotSupportedException>(() => Resolve("Nullable(NotAType)"));

    [Test]
    public void Resolve_Nullable_StampsFullTypeName()
        => Assert.That(Resolve("Nullable(UInt8)").TypeName, Is.EqualTo("Nullable(UInt8)"));
}
