using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class TimeColumnCodecTests
{
    // Time/Time64 value round-trips run against a live server (InsertRoundTripCase, with the Time type flag
    // enabled). These unit tests cover the raw/TimeSpan surfaces and the write-side range validation and
    // precision truncation a server round-trip does not.

    private static Time64ColumnCodec Time64(string type) => Time64ColumnCodec.Create(TypeParser.Parse(type));

    [Test]
    public async Task ReadColumn_Time_ExposesRawSecondsAndTimeSpanView()
    {
        // The default surface is the raw Int32 seconds (zero-copy); TimeSpan is a projection.
        byte[] bytes = await WriteAsync(w =>
        {
            w.WriteInt32(45_296); // 12:34:56
            w.WriteInt32(-3_723); // -01:02:03
        });
        using var reader = ReaderOver(bytes);

        using var column = (TimeColumn)await TimeColumnCodec.Instance.ReadColumnAsync(reader, "c", "Time", 2, None);

        Assert.Multiple(() =>
        {
            CollectionAssert.AreEqual(new[] { 45_296, -3_723 }, column.Values.ToArray());
            Assert.That(column[0], Is.EqualTo(45_296));
            Assert.That(column.GetTimeSpan(0), Is.EqualTo(new TimeSpan(12, 34, 56)));
            Assert.That(column.GetTimeSpan(1), Is.EqualTo(new TimeSpan(-1, -2, -3)));
            CollectionAssert.AreEqual(new[] { new TimeSpan(12, 34, 56), new TimeSpan(-1, -2, -3) }, column.ToTimeSpans());
        });
    }

    [TestCase(0)]
    [TestCase(45_296)]
    [TestCase(-3_723)]
    public async Task WriteColumn_Time_RawSeconds_WrittenVerbatim(int seconds)
    {
        var column = new ArrayColumn<int>("c", "Time", new[] { seconds });
        byte[] bytes = await WriteAsync(w => TimeColumnCodec.Instance.WriteColumn(w, column));
        Assert.That(BitConverter.ToInt32(bytes), Is.EqualTo(seconds));
    }

    [Test]
    public async Task ReadColumn_Time64_ExposesRawCountsAndTimeSpanView()
    {
        // The default surface is the raw Int64 count at the column's scale (zero-copy); TimeSpan is a projection.
        const string type = "Time64(3)";
        byte[] bytes = await WriteAsync(w =>
        {
            w.WriteInt64(3_723_456); // 01:02:03.456
            w.WriteInt64(-3_723_456);
        });
        using var reader = ReaderOver(bytes);

        using var column = (Time64Column)await Time64(type).ReadColumnAsync(reader, "c", type, 2, None);

        Assert.Multiple(() =>
        {
            Assert.That(column.Scale, Is.EqualTo(3));
            CollectionAssert.AreEqual(new[] { 3_723_456L, -3_723_456L }, column.Values.ToArray());
            Assert.That(column[0], Is.EqualTo(3_723_456L));
            Assert.That(column.GetTimeSpan(0), Is.EqualTo(new TimeSpan(0, 1, 2, 3, 456)));
            CollectionAssert.AreEqual(
                new[] { new TimeSpan(0, 1, 2, 3, 456), new TimeSpan(0, -1, -2, -3, -456) },
                column.ToTimeSpans());
        });
    }

    [Test]
    public async Task ReadColumn_Time64_Scale9_PreservesExactCount()
    {
        // A nanosecond count with sub-100 ns digits that no TimeSpan could hold must survive verbatim in Values.
        const string type = "Time64(9)";
        byte[] bytes = await WriteAsync(w => w.WriteInt64(3_723_123_456_789L));
        using var reader = ReaderOver(bytes);

        using var column = (Time64Column)await Time64(type).ReadColumnAsync(reader, "c", type, 1, None);

        Assert.That(column[0], Is.EqualTo(3_723_123_456_789L));
    }

    [TestCase(0L)]
    [TestCase(3_723_456L)]
    [TestCase(-3_723_456L)]
    public async Task WriteColumn_Time64_RawCount_WrittenVerbatim(long count)
    {
        var column = new ArrayColumn<long>("c", "Time64(3)", new[] { count });
        byte[] bytes = await WriteAsync(w => Time64("Time64(3)").WriteColumn(w, column));
        Assert.That(BitConverter.ToInt64(bytes), Is.EqualTo(count));
    }

    [Test]
    public void CanWrite_AcceptsRawAndTimeSpan_RejectsOthers()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TimeColumnCodec.Instance.CanWrite(new ArrayColumn<int>("c", "Time", Array.Empty<int>())), Is.True);
            Assert.That(TimeColumnCodec.Instance.CanWrite(new ArrayColumn<TimeSpan>("c", "Time", Array.Empty<TimeSpan>())), Is.True);
            Assert.That(TimeColumnCodec.Instance.CanWrite(new ArrayColumn<string>("c", "Time", Array.Empty<string>())), Is.False);
            Assert.That(Time64("Time64(3)").CanWrite(new ArrayColumn<long>("c", "Time64(3)", Array.Empty<long>())), Is.True);
            Assert.That(Time64("Time64(3)").CanWrite(new ArrayColumn<TimeSpan>("c", "Time64(3)", Array.Empty<TimeSpan>())), Is.True);
            Assert.That(Time64("Time64(3)").CanWrite(new ArrayColumn<string>("c", "Time64(3)", Array.Empty<string>())), Is.False);
        });
    }

    [Test]
    public void Time_OutOfRange_Throws()
    {
        var column = new ArrayColumn<TimeSpan>("c", "Time", new[] { TimeSpan.FromHours(1000) });
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await WriteAsync(w => TimeColumnCodec.Instance.WriteColumn(w, column)));
    }

    [Test]
    public async Task Time_SubSecondPrecision_TruncatesTowardZero()
    {
        // Time holds whole seconds only; the 500 ms component is dropped rather than rejected — precision is the caller's call.
        var column = new ArrayColumn<TimeSpan>("c", "Time", new[] { new TimeSpan(0, 0, 0, 1, 500) });
        byte[] bytes = await WriteAsync(w => TimeColumnCodec.Instance.WriteColumn(w, column));
        Assert.That(BitConverter.ToInt32(bytes), Is.EqualTo(1));
    }

    [Test]
    public async Task Time_WholeSeconds_Writes()
    {
        var column = new ArrayColumn<TimeSpan>("c", "Time", new[] { new TimeSpan(1, 2, 3) });
        byte[] bytes = await WriteAsync(w => TimeColumnCodec.Instance.WriteColumn(w, column));
        Assert.That(bytes.Length, Is.EqualTo(sizeof(int)));
    }

    [Test]
    public async Task Time64_SubScalePrecision_TruncatesTowardZero()
    {
        // Time64(3) is milliseconds; sub-millisecond ticks are truncated toward zero rather than rejected.
        // 4_560_789 ticks = 456.0789 ms → 456 at scale 3.
        Time64ColumnCodec codec = Time64("Time64(3)");
        var column = new ArrayColumn<TimeSpan>("c", "Time64(3)", new[] { TimeSpan.FromTicks(4_560_789) });
        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));
        Assert.That(BitConverter.ToInt64(bytes), Is.EqualTo(456));
    }

    [Test]
    public async Task Time64_ExactScale_Writes()
    {
        // A whole-millisecond value is exactly representable at scale 3.
        Time64ColumnCodec codec = Time64("Time64(3)");
        var column = new ArrayColumn<TimeSpan>("c", "Time64(3)", new[] { new TimeSpan(0, 0, 0, 1, 456) });
        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));
        Assert.That(bytes.Length, Is.EqualTo(sizeof(long)));
    }

    [Test]
    public void Time64_OutOfRange_Throws()
    {
        Time64ColumnCodec codec = Time64("Time64(3)");
        var column = new ArrayColumn<TimeSpan>("c", "Time64(3)", new[] { TimeSpan.FromHours(1000) });
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await WriteAsync(w => codec.WriteColumn(w, column)));
    }

    [Test]
    public void Time64_MissingScale_Throws()
        => Assert.Throws<FormatException>(() => Time64ColumnCodec.Create(TypeParser.Parse("Time64")));

    [Test]
    public void Time64_ScaleOutOfRange_Throws()
        => Assert.Throws<FormatException>(() => Time64ColumnCodec.Create(TypeParser.Parse("Time64(10)")));
}
