using System;
using System.Net;
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
    public async Task ReadColumn_WriteThenRead_ValueInnerRoundTripsWithNulls()
    {
        IColumnCodec codec = Resolve("Nullable(Int32)");
        var expected = new int?[] { 7, null, -3, null, 0 };
        var column = new ArrayColumn<int?>("c", "Nullable(Int32)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Nullable(Int32)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(read.TypeName, Is.EqualTo("Nullable(Int32)"));
            Assert.That(read.RowCount, Is.EqualTo(5));
            Assert.That(((IColumn<int?>)read).Values.ToArray(), Is.EqualTo(expected));
            Assert.That(read.GetValue(0), Is.EqualTo(7));
            Assert.That(read.GetValue(1), Is.Null);
        });
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_ReferenceInnerRoundTripsWithNulls()
    {
        IColumnCodec codec = Resolve("Nullable(String)");
        var expected = new[] { "hi", null, string.Empty, "world" };
        var column = new ArrayColumn<string>("c", "Nullable(String)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Nullable(String)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(read.RowCount, Is.EqualTo(4));
            Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(expected));
            Assert.That(read.GetValue(1), Is.Null);
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
        var dense = new NullableValueColumn<int>("c", "Nullable(Int32)", inner, new byte[] { 0, 1, 0 }, rowCount: 3, pooledMap: false);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, dense, "Nullable(Int32)", dense.RowCount);

        Assert.That(((IColumn<int?>)read).Values.ToArray(), Is.EqualTo(new int?[] { 7, null, 9 }));
    }

    [Test]
    public async Task ReadColumn_EveryRowNull_RoundTripsAsAllNull()
    {
        IColumnCodec codec = Resolve("Nullable(Int32)");
        var column = new ArrayColumn<int?>("c", "Nullable(Int32)", new int?[] { null, null, null });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Nullable(Int32)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(read.RowCount, Is.EqualTo(3));
            Assert.That(read.GetValue(0), Is.Null);
            Assert.That(read.GetValue(2), Is.Null);
        });
    }

    [Test]
    public async Task ReadColumn_ValueInnerWhoseDefaultIsOutOfRange_WritesPlaceholderTheCodecAccepts()
    {
        // default(DateOnly) is 0001-01-01, which the Date codec rejects; the null rows must borrow a present
        // value as the placeholder instead, so writing does not throw.
        IColumnCodec codec = Resolve("Nullable(Date)");
        var expected = new DateOnly?[] { new DateOnly(2000, 1, 1), null, new DateOnly(1970, 1, 1) };
        var column = new ArrayColumn<DateOnly?>("c", "Nullable(Date)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Nullable(Date)", column.RowCount);

        Assert.That(((IColumn<DateOnly?>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_FixedWidthReferenceInnerWithNulls_WritesPlaceholderNotDereferenced()
    {
        // IPv4 is reference-typed (IPAddress) but fixed-width; the IP codec dereferences each value, so a null
        // row must be written as a placeholder rather than passed through. This would NRE if it were.
        IColumnCodec codec = Resolve("Nullable(IPv4)");
        var expected = new[] { IPAddress.Parse("127.0.0.1"), null, IPAddress.Parse("10.0.0.1") };
        var column = new ArrayColumn<IPAddress>("c", "Nullable(IPv4)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Nullable(IPv4)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(((IColumn<IPAddress>)read).Values.ToArray(), Is.EqualTo(expected));
            Assert.That(read.GetValue(1), Is.Null);
        });
    }

    [Test]
    public async Task ReadColumn_FixedWidthReferenceInnerAllNull_RoundTripsAsAllNull()
    {
        IColumnCodec codec = Resolve("Nullable(IPv6)");
        var column = new ArrayColumn<IPAddress>("c", "Nullable(IPv6)", new IPAddress[] { null, null });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Nullable(IPv6)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(read.GetValue(0), Is.Null);
            Assert.That(read.GetValue(1), Is.Null);
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
    public void FixedRowByteSize_IsNullMapBytePlusInner_OrNullForVariableInner()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Resolve("Nullable(Int32)").FixedRowByteSize, Is.EqualTo(5));
            Assert.That(Resolve("Nullable(String)").FixedRowByteSize, Is.Null);
        });
    }

    [Test]
    public void MeasureRowBytes_VariableInner_CountsNullMapBytePlusInnerValue()
    {
        IColumnCodec codec = Resolve("Nullable(String)");
        var column = new ArrayColumn<string>("c", "Nullable(String)", new[] { "hi", null });

        Assert.Multiple(() =>
        {
            Assert.That(codec.MeasureRowBytes(column, 0), Is.EqualTo(4)); // 1 null-map + 1 length prefix + 2 bytes
            Assert.That(codec.MeasureRowBytes(column, 1), Is.EqualTo(2)); // 1 null-map + empty-string placeholder
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
        // a DateTime placeholder — not the canonical DateTimeOffset one — and read back as the canonical offset.
        IColumnCodec codec = Resolve("Nullable(DateTime('UTC'))");
        var input = new DateTime?[] { DateTime.UnixEpoch.AddSeconds(1_700_000_000), null, DateTime.UnixEpoch };
        var column = new ArrayColumn<DateTime?>("c", "Nullable(DateTime('UTC'))", input);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Nullable(DateTime('UTC'))", column.RowCount);

        var expected = new DateTimeOffset?[]
        {
            DateTimeOffset.FromUnixTimeSeconds(1_700_000_000),
            null,
            DateTimeOffset.FromUnixTimeSeconds(0),
        };
        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(typeof(DateTimeOffset?)));
            Assert.That(((IColumn<DateTimeOffset?>)read).Values.ToArray(), Is.EqualTo(expected));
            Assert.That(read.GetValue(1), Is.Null);
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
        // DateTime64's canonical read type is ClickHouseDateTime64, but it accepts DateTimeOffset and DateTime on
        // write; Nullable(DateTime64) re-offers all three. Both alternate spellings must round-trip through the
        // native read type, each with its own-typed placeholder at the null row.
        IColumnCodec codec = Resolve("Nullable(DateTime64(3, 'UTC'))");
        DateTimeOffset present = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123);

        var asOffset = new ArrayColumn<DateTimeOffset?>("c", "Nullable(DateTime64(3, 'UTC'))", new DateTimeOffset?[] { present, null });
        var asDateTime = new ArrayColumn<DateTime?>("c", "Nullable(DateTime64(3, 'UTC'))", new DateTime?[] { present.UtcDateTime, null });

        using IColumn fromOffset = await CodecTestHarness.RoundTripAsync(codec, asOffset, "Nullable(DateTime64(3, 'UTC'))", 2);
        using IColumn fromDateTime = await CodecTestHarness.RoundTripAsync(codec, asDateTime, "Nullable(DateTime64(3, 'UTC'))", 2);

        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(typeof(ClickHouseDateTime64?)));

            var offsetRead = (IColumn<ClickHouseDateTime64?>)fromOffset;
            Assert.That(offsetRead[0].Value.ToDateTimeOffset(), Is.EqualTo(present));
            Assert.That(fromOffset.GetValue(1), Is.Null);

            var dateTimeRead = (IColumn<ClickHouseDateTime64?>)fromDateTime;
            Assert.That(dateTimeRead[0].Value.ToDateTimeOffset(), Is.EqualTo(present));
            Assert.That(fromDateTime.GetValue(1), Is.Null);
        });
    }

    [Test]
    public void CanWrite_NullableDateTime64_AcceptsNativeOffsetAndDateTimeSpellings()
    {
        IColumnCodec codec = Resolve("Nullable(DateTime64(3, 'UTC'))");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<ClickHouseDateTime64?>("c", "Nullable(DateTime64(3, 'UTC'))", new ClickHouseDateTime64?[] { new ClickHouseDateTime64(0, 3, TimeSpan.Zero) })), Is.True);
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
    public async Task TryDensify_ErgonomicValueColumn_ProducesDenseColumnThatRoundTrips()
    {
        // TryDensify splits the ergonomic T? column into a dense (inner column + null-map) NullableValueColumn: the
        // inner holds a real value at every row (the codec's placeholder at the null rows) and the null-map marks
        // the nulls. It must surface the same T? values and round-trip identically to the ergonomic form.
        IColumnCodec codec = Resolve("Nullable(Int32)");
        var expected = new int?[] { 7, null, -3, null, 0 };
        var ergonomic = new ArrayColumn<int?>("c", "Nullable(Int32)", expected);

        using IColumn densified = codec.TryDensify(ergonomic, out _);

        Assert.That(densified, Is.InstanceOf<NullableValueColumn<int>>());
        var dense = (NullableValueColumn<int>)densified;
        Assert.Multiple(() =>
        {
            Assert.That(dense.NullMap.ToArray(), Is.EqualTo(new byte[] { 0, 1, 0, 1, 0 }));
            Assert.That(dense.Inner.Values.ToArray(), Is.EqualTo(new[] { 7, 0, -3, 0, 0 })); // placeholder 0 at nulls
            Assert.That(((IColumn<int?>)densified).Values.ToArray(), Is.EqualTo(expected));
        });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, densified, "Nullable(Int32)", densified.RowCount);
        Assert.That(((IColumn<int?>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task TryDensify_ErgonomicReferenceColumn_ProducesDenseColumnThatRoundTrips()
    {
        IColumnCodec codec = Resolve("Nullable(String)");
        var expected = new[] { "hi", null, string.Empty, "world" };
        var ergonomic = new ArrayColumn<string>("c", "Nullable(String)", expected);

        using IColumn densified = codec.TryDensify(ergonomic, out _);

        Assert.That(densified, Is.InstanceOf<NullableReferenceColumn<string>>());
        var dense = (NullableReferenceColumn<string>)densified;
        Assert.Multiple(() =>
        {
            Assert.That(dense.NullMap.ToArray(), Is.EqualTo(new byte[] { 0, 1, 0, 0 }));
            Assert.That(((IColumn<string>)densified).Values.ToArray(), Is.EqualTo(expected));
        });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, densified, "Nullable(String)", densified.RowCount);
        Assert.That(((IColumn<string>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void TryDensify_NullRows_UseInnerPlaceholderNotClrDefault()
    {
        // default(DateOnly) (0001-01-01) is out of the Date codec's range; densify must fill the null rows with
        // the codec's own placeholder (the epoch) so the dense inner column is writable.
        IColumnCodec codec = Resolve("Nullable(Date)");
        var expected = new DateOnly?[] { new DateOnly(2000, 1, 1), null };
        var ergonomic = new ArrayColumn<DateOnly?>("c", "Nullable(Date)", expected);

        using var dense = (NullableValueColumn<DateOnly>)codec.TryDensify(ergonomic, out _);

        Assert.Multiple(() =>
        {
            Assert.That(dense.Inner[1], Is.EqualTo(new DateOnly(1970, 1, 1)));
            Assert.That(((IColumn<DateOnly?>)dense).Values.ToArray(), Is.EqualTo(expected));
        });
    }

    [Test]
    public void TryDensify_AlreadyDenseColumn_ReturnsSameInstanceNotBuilt()
    {
        // TryDensify is idempotent: a column already in dense form is returned by reference with built = false, so a
        // caller knows it is borrowed (not to be disposed) rather than freshly built.
        IColumnCodec codec = Resolve("Nullable(Int32)");
        var inner = PrimitiveColumn<int>.FromValues("c", "Int32", new[] { 7, 0, 9 });
        var dense = new NullableValueColumn<int>("c", "Nullable(Int32)", inner, new byte[] { 0, 1, 0 }, rowCount: 3, pooledMap: false);

        IColumn result = codec.TryDensify(dense, out bool built);
        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(dense));
            Assert.That(built, Is.False, "an already-dense column is borrowed, not rebuilt");
        });
    }

    [Test]
    public void TryDensify_LeafCodec_ReturnsColumnUnchangedNotBuilt()
    {
        // A leaf codec has no denser form, so the default hook returns the column by reference with built = false.
        IColumnCodec codec = Resolve("Int32");
        var column = new ArrayColumn<int>("c", "Int32", new[] { 1, 2, 3 });

        IColumn result = codec.TryDensify(column, out bool built);
        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(column));
            Assert.That(built, Is.False, "a leaf codec never builds a new column");
        });
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
        => Assert.Throws<NotSupportedException>(() => Resolve("Nullable(Array(Int32))"));

    [Test]
    public void Resolve_Nullable_StampsFullTypeName()
        => Assert.That(Resolve("Nullable(UInt8)").TypeName, Is.EqualTo("Nullable(UInt8)"));
}
