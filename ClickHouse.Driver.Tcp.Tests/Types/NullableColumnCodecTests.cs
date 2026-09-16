using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Numerics;
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
    public void CanWrite_AcceptsOnlyMatchingNullableColumn()
    {
        IColumnCodec value = Resolve("Nullable(Int32)");
        IColumnCodec reference = Resolve("Nullable(String)");

        Assert.Multiple(() =>
        {
            Assert.That(value.CanWrite(new ArrayColumn<int?>("c", "Nullable(Int32)", new int?[] { 1 })), Is.True);
            Assert.That(value.CanWrite(new ArrayColumn<int>("c", "Int32", new[] { 1 })), Is.False);
            Assert.That(value.CanWrite(new ArrayColumn<long?>("c", "Nullable(Int64)", new long?[] { 1 })), Is.False);
            Assert.That(reference.CanWrite(new ArrayColumn<string>("c", "Nullable(String)", new[] { "x" })), Is.True);
        });
    }

    [Test]
    public async Task WriteColumn_NullableDateTimeAsDateTimeSpelling_RoundTripsAsCanonicalOffset()
    {
        // The bare DateTime codec accepts both DateTimeOffset and DateTime; Nullable(DateTime) re-offers both.
        // A DateTime? column (the inner's alternate spelling made nullable) must write, with the null row taking
        // a DateTime placeholder, and read back as the canonical raw epoch seconds (uint?).
        IColumnCodec codec = Resolve("Nullable(DateTime('UTC'))");
        var input = new DateTime?[] { DateTime.UnixEpoch.AddSeconds(1_700_000_000), null, DateTime.UnixEpoch };
        var column = new ArrayColumn<DateTime?>("c", "Nullable(DateTime('UTC'))", input);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Nullable(DateTime('UTC'))", column.RowCount);

        var expected = new uint?[] { 1_700_000_000u, null, 0u };
        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(typeof(uint?)));
            Assert.That(((IColumn<uint?>)read).Values.ToArray(), Is.EqualTo(expected));
            Assert.That(read.GetValue(1), Is.Null);
        });
    }

    [Test]
    public void WritableElementTypes_ListWhatCanWriteAccepts_CanonicalFirst()
    {
        // The list has to agree with CanWrite: a caller choosing a write type from it — a POCO insert picking the
        // spelling to gather a property into — never gets to probe with a column first.
        Assert.Multiple(() =>
        {
            Assert.That(
                Resolve("Nullable(DateTime('UTC'))").WritableElementTypes,
                Is.EqualTo(new[] { typeof(uint?), typeof(DateTimeOffset?), typeof(DateTime?) }));
            Assert.That(Resolve("Nullable(Int32)").WritableElementTypes, Is.EqualTo(new[] { typeof(int?) }));
            Assert.That(Resolve("Nullable(String)").WritableElementTypes, Is.EqualTo(new[] { typeof(string) }));
        });
    }

    [Test]
    public void NullPlaceholderAs_EveryWritableSpelling_IsNullAndAnythingElseThrows()
    {
        IColumnCodec codec = Resolve("Nullable(DateTime('UTC'))");

        Assert.Multiple(() =>
        {
            Assert.That(codec.NullPlaceholderAs(typeof(uint?)), Is.Null);
            Assert.That(codec.NullPlaceholderAs(typeof(DateTime?)), Is.Null);
            Assert.Throws<NotSupportedException>(() => codec.NullPlaceholderAs(typeof(DateTime)));
        });
    }

    [Test]
    public void CanWrite_NullableDateTime_AcceptsBothOffsetAndDateTimeSpellings()
    {
        IColumnCodec codec = Resolve("Nullable(DateTime('UTC'))");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<DateTimeOffset?>("c", "Nullable(DateTime('UTC'))", new DateTimeOffset?[] { DateTimeOffset.UnixEpoch })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<DateTime?>("c", "Nullable(DateTime('UTC'))", new DateTime?[] { DateTime.UnixEpoch })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<int?>("c", "Nullable(Int32)", new int?[] { 1 })), Is.False);
        });
    }

    [Test]
    public async Task WriteColumn_NullableDateTime64AsOffsetAndDateTimeSpellings_RoundTripsAsCanonicalNative()
    {
        // DateTime64's canonical read type is the raw Int64 count, but it accepts DateTimeOffset and DateTime on
        // write; Nullable(DateTime64) re-offers all three. Both alternate spellings must round-trip through the
        // raw read type, with a null placeholder at the null row.
        IColumnCodec codec = Resolve("Nullable(DateTime64(3, 'UTC'))");
        DateTimeOffset present = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123);
        const long presentCount = 1_700_000_000_123L; // scale 3: milliseconds since the epoch

        var asOffset = new ArrayColumn<DateTimeOffset?>("c", "Nullable(DateTime64(3, 'UTC'))", new DateTimeOffset?[] { present, null });
        var asDateTime = new ArrayColumn<DateTime?>("c", "Nullable(DateTime64(3, 'UTC'))", new DateTime?[] { present.UtcDateTime, null });

        using IColumn fromOffset = await CodecTestHarness.RoundTripAsync(codec, asOffset, "Nullable(DateTime64(3, 'UTC'))", 2);
        using IColumn fromDateTime = await CodecTestHarness.RoundTripAsync(codec, asDateTime, "Nullable(DateTime64(3, 'UTC'))", 2);

        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(typeof(long?)));

            var offsetRead = (IColumn<long?>)fromOffset;
            Assert.That(offsetRead[0].Value, Is.EqualTo(presentCount));
            Assert.That(fromOffset.GetValue(1), Is.Null);

            var dateTimeRead = (IColumn<long?>)fromDateTime;
            Assert.That(dateTimeRead[0].Value, Is.EqualTo(presentCount));
            Assert.That(fromDateTime.GetValue(1), Is.Null);
        });
    }

    [Test]
    public void CanWrite_NullableDateTime64_AcceptsNativeOffsetAndDateTimeSpellings()
    {
        IColumnCodec codec = Resolve("Nullable(DateTime64(3, 'UTC'))");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<long?>("c", "Nullable(DateTime64(3, 'UTC'))", new long?[] { 0L })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<DateTimeOffset?>("c", "Nullable(DateTime64(3, 'UTC'))", new DateTimeOffset?[] { DateTimeOffset.UnixEpoch })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<DateTime?>("c", "Nullable(DateTime64(3, 'UTC'))", new DateTime?[] { DateTime.UnixEpoch })), Is.True);
        });
    }

    [Test]
    public void WriteColumn_ColumnOfUnacceptedSpelling_ThrowsArgument()
    {
        // WriteColumn is normally guarded by CanWrite, but a direct call with a column whose CLR spelling none of
        // the inner's writable spellings match must fail with a clear error rather than a nested cast failure.
        IColumnCodec codec = Resolve("Nullable(DateTime('UTC'))");
        using var writer = new ClickHouseBinaryWriter(new System.IO.MemoryStream());
        var wrong = new ArrayColumn<long?>("c", "Nullable(Int64)", new long?[] { 1 });

        Assert.Throws<ArgumentException>(() => codec.WriteColumn(writer, wrong, 0, 1));
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
    public void CanWrite_NullableNothing_ReturnsFalse()
    {
        // The inner Nothing codec cannot write, so Nullable(Nothing) must report not-writable up front rather
        // than accept the column and fail mid-write. (Reading Nullable(Nothing) still works — see above.)
        IColumnCodec codec = Resolve("Nullable(Nothing)");
        Assert.That(codec.CanWrite(new ArrayColumn<object>("c", "Nullable(Nothing)", new object[] { null, null })), Is.False);
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
        => Assert.Throws<NotSupportedException>(() => Resolve("Nullable(Point)"));

    [Test]
    public void Resolve_Nullable_StampsFullTypeName()
        => Assert.That(Resolve("Nullable(UInt8)").TypeName, Is.EqualTo("Nullable(UInt8)"));
}
