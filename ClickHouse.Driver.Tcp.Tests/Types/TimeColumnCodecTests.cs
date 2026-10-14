using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class TimeColumnCodecTests
{
    // Time/Time64 values go to a live server in InsertRoundTripCase, with the Time type flag enabled. These unit
    // tests cover the raw count and TimeSpan views of a read, and the resolution of the Time64 scale.

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

    [Test]
    public void Time64_MissingScale_Throws()
        => Assert.Throws<FormatException>(() => Time64ColumnCodec.Create(TypeParser.Parse("Time64")));

    [Test]
    public void Time64_ScaleOutOfRange_Throws()
        => Assert.Throws<FormatException>(() => Time64ColumnCodec.Create(TypeParser.Parse("Time64(10)")));
}
