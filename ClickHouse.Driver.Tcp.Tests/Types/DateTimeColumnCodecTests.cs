using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class DateTimeColumnCodecTests
{
    private static DateTimeColumnCodec Codec(string type, string serverTimezone = null)
        => DateTimeColumnCodec.Create(TypeParser.Parse(type), serverTimezone);

    [Test]
    public async Task ReadColumn_SingleValue_DecodesRawUnixSeconds()
    {
        // 1 row: the little-endian UInt32 1000 = 1970-01-01T00:16:40Z.
        byte[] bytes = await WriteAsync(w => w.WriteUInt32(1000));
        using var reader = ReaderOver(bytes);

        using var column = (IColumn<uint>)await Codec("DateTime").ReadColumnAsync(reader, "c", "DateTime", 1, None);

        Assert.That(column[0], Is.EqualTo(1000u));
    }

    [Test]
    public async Task ReadColumn_ExplicitTimezone_PresentsThatOffset()
    {
        // A winter instant (no UK daylight-saving) read as Europe/London presents a +00:00 offset. Either way the
        // instant (the raw seconds) is unchanged.
        byte[] bytes = await WriteAsync(w => w.WriteUInt32(1_700_000_000)); // 2023-11-14, winter
        using var reader = ReaderOver(bytes);

        using var column = (DateTimeColumn)await Codec("DateTime('Europe/London')").ReadColumnAsync(reader, "c", "DateTime('Europe/London')", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(1_700_000_000u));
            Assert.That(column.GetDateTimeOffset(0).Offset, Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    public async Task ReadColumn_DaylightSavingZoneSummerInstant_PresentsDaylightOffset()
    {
        // A summer instant read as Europe/London presents +01:00 (British Summer Time); the offset is resolved
        // per instant, so a daylight-saving zone honors its transitions.
        byte[] bytes = await WriteAsync(w => w.WriteUInt32(1_689_300_000)); // 2023-07-14, summer
        using var reader = ReaderOver(bytes);

        using var column = (DateTimeColumn)await Codec("DateTime('Europe/London')").ReadColumnAsync(reader, "c", "DateTime('Europe/London')", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(1_689_300_000u));
            Assert.That(column.GetDateTimeOffset(0).Offset, Is.EqualTo(TimeSpan.FromHours(1)));
        });
    }

    [Test]
    public async Task ReadColumn_NoExplicitTimezone_FallsBackToServerTimezone()
    {
        byte[] bytes = await WriteAsync(w => w.WriteUInt32(1_700_000_000));
        using var reader = ReaderOver(bytes);

        // No timezone in the type string; the codec resolves against the session timezone instead.
        using var column = (DateTimeColumn)await Codec("DateTime", serverTimezone: "Asia/Kolkata").ReadColumnAsync(reader, "c", "DateTime", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(1_700_000_000u));
            Assert.That(column.GetDateTimeOffset(0).Offset, Is.EqualTo(new TimeSpan(5, 30, 0)));
        });
    }

    [Test]
    public async Task ReadColumn_NoExplicitAndNoServerTimezone_PresentsUtc()
    {
        byte[] bytes = await WriteAsync(w => w.WriteUInt32(1_700_000_000));
        using var reader = ReaderOver(bytes);

        // No timezone in the type or the session, so values present in UTC, not the host's local zone.
        using var column = (DateTimeColumn)await Codec("DateTime", serverTimezone: null).ReadColumnAsync(reader, "c", "DateTime", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(1_700_000_000u));
            Assert.That(column.GetDateTimeOffset(0).Offset, Is.EqualTo(TimeSpan.Zero));
            Assert.That(column.GetDateTimeOffset(0), Is.EqualTo(DateTimeOffset.FromUnixTimeSeconds(1_700_000_000)));
        });
    }

    [Test]
    public async Task ReadColumn_ExposesRawSecondsAndOffsetViews()
    {
        // The specialized column offers the raw epoch seconds (Values / indexer, zero-copy) and a DateTimeOffset
        // projection over the same rows; the timezone is column-level.
        byte[] bytes = await WriteAsync(w =>
        {
            w.WriteUInt32(1000);
            w.WriteUInt32(1_700_000_000);
        });
        using var reader = ReaderOver(bytes);

        using var column = (DateTimeColumn)await Codec("DateTime", serverTimezone: "UTC").ReadColumnAsync(reader, "c", "DateTime", 2, None);

        Assert.Multiple(() =>
        {
            Assert.That(column.TimeZone, Is.EqualTo(TimeZoneInfo.Utc));
            CollectionAssert.AreEqual(new uint[] { 1000, 1_700_000_000 }, column.Values.ToArray());
            Assert.That(column[1], Is.EqualTo(1_700_000_000u));
            Assert.That(column.GetDateTimeOffset(0), Is.EqualTo(DateTimeOffset.FromUnixTimeSeconds(1000)));
            CollectionAssert.AreEqual(
                new[] { DateTimeOffset.FromUnixTimeSeconds(1000), DateTimeOffset.FromUnixTimeSeconds(1_700_000_000) },
                column.ToDateTimeOffsets());
        });
    }

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumn()
    {
        using var reader = ReaderOver(Array.Empty<byte>());
        using var column = (IColumn<uint>)await Codec("DateTime").ReadColumnAsync(reader, "c", "DateTime", 0, None);
        Assert.That(column.RowCount, Is.EqualTo(0));
    }

    [Test]
    public async Task WriteColumn_UnspecifiedKindTimezonelessColumn_TreatedAsUtcNotMachineLocal()
    {
        // A timezone-less column with no session timezone resolves to UTC. So an insert writes a Kind=Unspecified wall
        // clock as UTC, and the wire bytes do not depend on the timezone of the host machine.
        DateTimeColumnCodec codec = Codec("DateTime");
        var unspecified = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Unspecified);
        var utc = DateTime.SpecifyKind(unspecified, DateTimeKind.Utc);

        byte[] fromUnspecified = await WriteSliceAsync(codec, new ArrayColumn<DateTime>("c", "DateTime", new[] { unspecified }), 0, 1);
        byte[] fromUtc = await WriteSliceAsync(codec, new ArrayColumn<DateTime>("c", "DateTime", new[] { utc }), 0, 1);

        CollectionAssert.AreEqual(fromUtc, fromUnspecified);
    }

    [TestCase("DateTime('Not/AZone')", "Not/AZone")]
    [TestCase("DateTime('Fixed/UTC+19:00:00')", "+19:00:00")]
    public async Task ReadColumn_TimezoneThisPlatformCannotResolve_ReadsTheSecondsAndReportsOnlyTheZone(string type, string named)
    {
        // Raw seconds do not require a representable timezone; calendar projections do.
        byte[] bytes = await WriteAsync(w => w.WriteUInt32(1_700_000_000));
        using var reader = ReaderOver(bytes);

        using var column = (DateTimeColumn)await Codec(type).ReadColumnAsync(reader, "c", type, 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(1_700_000_000u));
            Assert.That(Assert.Throws<FormatException>(() => _ = column.TimeZone).Message, Does.Contain(named));
            Assert.Throws<FormatException>(() => column.GetDateTimeOffset(0));
            Assert.Throws<FormatException>(() => column.ToDateTimeOffsets());
        });
    }

    [TestCase("Fixed/UTC+05:30:00", 5, 30)]
    [TestCase("Fixed/UTC-08:00:00", -8, 0)]
    public async Task ReadColumn_FixedUtcOffsetTimezone_PresentsThatOffset(string zone, int offsetHours, int offsetMinutes)
    {
        // ClickHouse emits synthetic "Fixed/UTC±HH:MM:SS" names for numeric offsets; the instant is unchanged
        // and the value is presented with the fixed offset the name encodes.
        string type = $"DateTime('{zone}')";
        byte[] bytes = await WriteAsync(w => w.WriteUInt32(1_700_000_000));
        using var reader = ReaderOver(bytes);

        using var column = (DateTimeColumn)await Codec(type).ReadColumnAsync(reader, "c", type, 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0], Is.EqualTo(1_700_000_000u));
            Assert.That(column.GetDateTimeOffset(0).Offset, Is.EqualTo(new TimeSpan(offsetHours, offsetMinutes, 0)));
        });
    }

    [Test]
    public async Task ReadColumn_FixedUtcOffsetAsServerTimezone_PresentsThatOffset()
    {
        // The fallback session timezone can itself be a synthetic fixed-offset name.
        byte[] bytes = await WriteAsync(w => w.WriteUInt32(1_700_000_000));
        using var reader = ReaderOver(bytes);

        using var column = (DateTimeColumn)await Codec("DateTime", serverTimezone: "Fixed/UTC+03:00:00")
            .ReadColumnAsync(reader, "c", "DateTime", 1, None);

        Assert.That(column.GetDateTimeOffset(0).Offset, Is.EqualTo(TimeSpan.FromHours(3)));
    }

    [Test]
    public void WriteColumn_DateTimeIntoAZoneTimeZoneInfoCannotHold_ThrowsNamingTheZone()
    {
        // An Unspecified DateTime is a wall clock, so writing one needs the zone the raw seconds did not.
        DateTimeColumnCodec codec = Codec("DateTime('Fixed/UTC+19:00:00')");
        var values = new ArrayColumn<DateTime>("c", "DateTime", new[] { new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Unspecified) });

        FormatException thrown = Assert.ThrowsAsync<FormatException>(() => WriteSliceAsync(codec, values, 0, values.RowCount));
        Assert.That(thrown.Message, Does.Contain("Fixed/UTC+19:00:00"));
    }

    // A UTC value already identifies an instant and does not require the column timezone.
    [Test]
    public async Task WriteColumn_UtcDateTimeIntoAZoneTimeZoneInfoCannotHold_WritesTheInstant()
    {
        var value = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        DateTimeColumnCodec codec = Codec("DateTime('Fixed/UTC+19:00:00')");

        byte[] bytes = await WriteSliceAsync(
            codec,
            new ArrayColumn<DateTime>("c", "DateTime('Fixed/UTC+19:00:00')", new[] { value }),
            0,
            1);

        Assert.That(BitConverter.ToUInt32(bytes, 0), Is.EqualTo(1_705_314_600U));
    }

    // Compare Local using the host conversion while verifying that the column timezone is ignored.
    [Test]
    public async Task WriteColumn_LocalDateTimeIntoAZoneTimeZoneInfoCannotHold_WritesTheInstant()
    {
        var value = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Local);
        DateTimeColumnCodec codec = Codec("DateTime('Fixed/UTC+19:00:00')");

        byte[] bytes = await WriteSliceAsync(
            codec,
            new ArrayColumn<DateTime>("c", "DateTime('Fixed/UTC+19:00:00')", new[] { value }),
            0,
            1);

        Assert.That(
            BitConverter.ToUInt32(bytes, 0),
            Is.EqualTo((uint)new DateTimeOffset(value.ToUniversalTime(), TimeSpan.Zero).ToUnixTimeSeconds()));
    }
}
