using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class DateTime64ColumnCodecTests
{
    private static DateTime64ColumnCodec Codec(string type, string tz = null) => DateTime64ColumnCodec.Create(TypeParser.Parse(type), tz);

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumn()
    {
        using var reader = ReaderOver(Array.Empty<byte>());
        using var column = (IColumn<long>)await Codec("DateTime64(3)", "UTC").ReadColumnAsync(reader, "c", "DateTime64(3)", 0, None);
        Assert.That(column.RowCount, Is.EqualTo(0));
    }

    // Scale 8 sets one digit finer than a .NET tick, scale 9 two. The raw count keeps them; the DateTimeOffset
    // view truncates toward zero, it does not round.
    [TestCase("DateTime64(8)", 170_000_000_012_345_678L)]
    [TestCase("DateTime64(9)", 1_700_000_000_123_456_789L)]
    public async Task RoundTrip_ScaleFinerThanDotNetTick_KeepsTheCountAndTruncatesTheOffsetView(string type, long count)
    {
        const long ExpectedDotNetTicks = 17_000_000_001_234_567L; // 1_700_000_000.1234567 s: the tick-aligned part.
        DateTime64ColumnCodec codec = Codec(type, "UTC");

        using var column = (DateTime64Column)await RoundTripAsync(
            codec, new ArrayColumn<long>("c", type, new[] { count }), type, 1);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(count), "the raw wire count must round-trip exactly");
            Assert.That(
                column.GetDateTimeOffset(0),
                Is.EqualTo(new DateTimeOffset(DateTime.UnixEpoch.AddTicks(ExpectedDotNetTicks), TimeSpan.Zero)),
                "the DateTimeOffset view truncates the sub-tick digits toward zero");
        });
    }

    [Test]
    public async Task ReadColumn_Scale3_ScalesMillisecondsToInstant()
    {
        // A DateTime64(3) count of 1500 = 1.5 seconds after the epoch.
        byte[] bytes = await WriteAsync(w => w.WriteInt64(1500));
        using var reader = ReaderOver(bytes);

        using var column = (DateTime64Column)await Codec("DateTime64(3)", "UTC").ReadColumnAsync(reader, "c", "DateTime64(3)", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(1500L));
            Assert.That(column.GetDateTimeOffset(0), Is.EqualTo(DateTimeOffset.FromUnixTimeMilliseconds(1500)));
        });
    }

    [Test]
    public async Task ReadColumn_ExposesRawCountsAndOffsetViews()
    {
        // The specialized column offers the raw counts (Values / indexer, zero-copy) and a DateTimeOffset
        // projection; scale and timezone are column-level.
        const string type = "DateTime64(3)";
        byte[] bytes = await WriteAsync(w =>
        {
            w.WriteInt64(1500);
            w.WriteInt64(-2500);
        });
        using var reader = ReaderOver(bytes);

        using var column = (DateTime64Column)await Codec(type, "UTC").ReadColumnAsync(reader, "c", type, 2, None);

        Assert.Multiple(() =>
        {
            Assert.That(column.Scale, Is.EqualTo(3));
            Assert.That(column.TimeZone, Is.EqualTo(TimeZoneInfo.Utc));
            CollectionAssert.AreEqual(new[] { 1500L, -2500L }, column.Values.ToArray());
            Assert.That(column[0], Is.EqualTo(1500L));
            Assert.That(column.GetDateTimeOffset(0), Is.EqualTo(DateTimeOffset.FromUnixTimeMilliseconds(1500)));
            Assert.That(column.GetDateTimeOffset(1), Is.EqualTo(DateTimeOffset.FromUnixTimeMilliseconds(-2500)));
            CollectionAssert.AreEqual(
                new[] { DateTimeOffset.FromUnixTimeMilliseconds(1500), DateTimeOffset.FromUnixTimeMilliseconds(-2500) },
                column.ToDateTimeOffsets());
        });
    }

    [Test]
    public async Task ReadColumn_NoExplicitAndNoServerTimezone_PresentsUtc()
    {
        byte[] bytes = await WriteAsync(w => w.WriteInt64(1_700_000_000_000)); // scale 3
        using var reader = ReaderOver(bytes);

        // No timezone in the type or the session, so values present in UTC, not the host's local zone.
        using var column = (DateTime64Column)await Codec("DateTime64(3)", tz: null)
            .ReadColumnAsync(reader, "c", "DateTime64(3)", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column.GetDateTimeOffset(0).Offset, Is.EqualTo(TimeSpan.Zero));
            Assert.That(column.GetDateTimeOffset(0), Is.EqualTo(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000)));
        });
    }

    [Test]
    public async Task ReadColumn_ExplicitTimezone_PresentsOffset()
    {
        byte[] bytes = await WriteAsync(w => w.WriteInt64(1_700_000_000_000)); // scale 3, winter instant
        using var reader = ReaderOver(bytes);

        using var column = (DateTime64Column)await Codec("DateTime64(3, 'Asia/Kolkata')").ReadColumnAsync(reader, "c", "DateTime64(3, 'Asia/Kolkata')", 1, None);

        Assert.That(column.GetDateTimeOffset(0).Offset, Is.EqualTo(new TimeSpan(5, 30, 0)));
    }

    [Test]
    public async Task ReadColumn_DaylightSavingZoneSummerInstant_PresentsDaylightOffset()
    {
        // A summer instant read as Europe/London presents +01:00 (British Summer Time); the offset is resolved
        // per instant, so a daylight-saving zone honors its transitions.
        byte[] bytes = await WriteAsync(w => w.WriteInt64(1_689_300_000_000)); // scale 3, 2023-07-14, summer
        using var reader = ReaderOver(bytes);

        using var column = (DateTime64Column)await Codec("DateTime64(3, 'Europe/London')").ReadColumnAsync(reader, "c", "DateTime64(3, 'Europe/London')", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(1_689_300_000_000L));
            Assert.That(column.GetDateTimeOffset(0).Offset, Is.EqualTo(TimeSpan.FromHours(1)));
        });
    }

    [Test]
    public async Task ReadColumn_FixedUtcOffsetTimeZoneInfoCannotHold_ReadsTheCountsAndReportsOnlyTheZone()
    {
        // Raw counts do not require the unrepresentable sub-minute timezone.
        const string type = "DateTime64(3, 'Fixed/UTC+05:30:15')";
        byte[] bytes = await WriteAsync(w => w.WriteInt64(1_700_000_000_123));
        using var reader = ReaderOver(bytes);

        using var column = (DateTime64Column)await Codec(type).ReadColumnAsync(reader, "c", type, 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(1_700_000_000_123L));
            Assert.That(Assert.Throws<FormatException>(() => _ = column.TimeZone).Message, Does.Contain("+05:30:15"));
            Assert.Throws<FormatException>(() => column.GetDateTimeOffset(0));
        });
    }

    // A UTC value already identifies an instant and does not require the column timezone.
    [Test]
    public async Task WriteColumn_UtcDateTimeIntoAZoneTimeZoneInfoCannotHold_WritesTheInstant()
    {
        const string type = "DateTime64(3, 'Fixed/UTC+19:00:00')";
        var value = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        byte[] bytes = await WriteSliceAsync(Codec(type), new ArrayColumn<DateTime>("c", type, new[] { value }), 0, 1);

        Assert.That(BitConverter.ToInt64(bytes, 0), Is.EqualTo(1_705_314_600_000L));
    }

    // The DateTime64 write of a DateTime applies the rule of the DateTime write (DateTimeColumnCodec.ToUtc).
    [Test]
    public void WriteColumn_UnspecifiedKindInDaylightSavingGap_ThrowsNamingTheZone()
    {
        // 2024-03-10 02:30 does not exist in New York, so it names no instant and is rejected.
        const string type = "DateTime64(3, 'America/New_York')";
        DateTime64ColumnCodec codec = Codec(type);
        var gap = new DateTime(2024, 3, 10, 2, 30, 0, DateTimeKind.Unspecified);

        ArgumentException thrown = Assert.ThrowsAsync<ArgumentException>(() =>
            WriteSliceAsync(codec, new ArrayColumn<DateTime>("c", type, new[] { gap }), 0, 1));

        Assert.That(thrown.Message, Does.Contain("America/New_York"));
    }

    [Test]
    public void Create_MissingScale_Throws()
        => Assert.Throws<FormatException>(() => Codec("DateTime64"));

    [Test]
    public void Create_ScaleOutOfRange_Throws()
        => Assert.Throws<FormatException>(() => Codec("DateTime64(10)"));
}
