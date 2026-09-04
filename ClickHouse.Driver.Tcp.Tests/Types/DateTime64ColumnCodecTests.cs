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

    // Scales 8 and 9 exceed a .NET tick (100 ns), but a write scales up, so it stays exact. The lossy direction is
    // the read, pinned below.
    [TestCase("DateTime64(0)")]
    [TestCase("DateTime64(3)")]
    [TestCase("DateTime64(6)")]
    [TestCase("DateTime64(7)")]
    [TestCase("DateTime64(8)")]
    [TestCase("DateTime64(9)")]
    public async Task RoundTrip_PreservesInstantAtScale(string type)
    {
        DateTime64ColumnCodec codec = Codec(type, "UTC");
        var values = new[]
        {
            DateTimeOffset.FromUnixTimeSeconds(0),
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000),
            DateTimeOffset.FromUnixTimeSeconds(-1_000_000),
        };

        using var column = (DateTime64Column)await RoundTripAsync(codec, new ArrayColumn<DateTimeOffset>("c", type, values), type, values.Length);

        Assert.Multiple(() =>
        {
            for (int i = 0; i < values.Length; i++)
            {
                Assert.That(column.GetDateTimeOffset(i), Is.EqualTo(values[i]));
            }
        });
    }

    [TestCase(0L)]
    [TestCase(1500L)]
    [TestCase(-2500L)]
    public async Task WriteColumn_RawCount_WrittenVerbatim(long count)
    {
        // A raw long is assumed already at the column's scale, so it is written to the wire unchanged.
        DateTime64ColumnCodec codec = Codec("DateTime64(3)", "UTC");
        var column = new ArrayColumn<long>("c", "DateTime64(3)", new[] { count });

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));

        Assert.That(BitConverter.ToInt64(bytes), Is.EqualTo(count));
    }

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumn()
    {
        using var reader = ReaderOver(Array.Empty<byte>());
        using var column = (IColumn<long>)await Codec("DateTime64(3)", "UTC").ReadColumnAsync(reader, "c", "DateTime64(3)", 0, None);
        Assert.That(column.RowCount, Is.EqualTo(0));
    }

    [Test]
    public void WriteColumn_DateTimeOffsetSubScalePrecision_Throws()
    {
        // A DateTimeOffset (100 ns ticks) whose instant is not a whole millisecond cannot be written to a
        // scale-3 column without dropping non-zero digits.
        DateTime64ColumnCodec codec = Codec("DateTime64(3)", "UTC");
        DateTimeOffset value = DateTimeOffset.FromUnixTimeMilliseconds(1500).AddTicks(1234);
        var column = new ArrayColumn<DateTimeOffset>("c", "DateTime64(3)", new[] { value });

        Assert.ThrowsAsync<ArgumentException>(async () => await WriteAsync(w => codec.WriteColumn(w, column)));
    }

    [Test]
    public void WriteColumn_DateTimeOffsetPastTheScalesRange_ThrowsNamingTheValueAndTheColumn()
    {
        // A fine scale reaches a nearer instant than .NET does: at scale 9 the Int64 nanosecond count stops at
        // 2262-04-11, well inside DateTimeOffset's range, so a 2300 instant has to be refused. The message has to
        // carry the value and the type, which is what an OverflowException out of the multiply does not.
        const string type = "DateTime64(9)";
        DateTime64ColumnCodec codec = Codec(type, "UTC");
        var column = new ArrayColumn<DateTimeOffset>("c", type, new[] { new DateTimeOffset(2300, 1, 1, 0, 0, 0, TimeSpan.Zero) });

        var thrown = Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await WriteAsync(w => codec.WriteColumn(w, column)));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain("2300-01-01"));
            Assert.That(thrown.Message, Does.Contain(type));
        });
    }

    // Scale 8 sets one digit finer than a .NET tick, scale 9 two. The raw count keeps them; the DateTimeOffset
    // view truncates toward zero, it does not round.
    [TestCase("DateTime64(8)", 170_000_000_012_345_678L)]
    [TestCase("DateTime64(9)", 1_700_000_000_123_456_789L)]
    public async Task RoundTrip_ScaleFinerThanDotNetTick_KeepsTheCountAndTruncatesTheOffsetView(string type, long count)
    {
        const long ExpectedDotNetTicks = 17_000_000_001_234_567L; // 1_700_000_000.1234567 s — the tick-aligned part.
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
    public async Task WriteColumn_DateTimeInput_EncodesSameCountAsEquivalentDateTimeOffset()
    {
        const string type = "DateTime64(3)";
        DateTime64ColumnCodec codec = Codec(type, "UTC");
        var utc = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        byte[] fromDateTime = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTime>("c", type, new[] { utc })));
        byte[] fromOffset = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTimeOffset>("c", type, new[] { new DateTimeOffset(utc) })));

        CollectionAssert.AreEqual(fromOffset, fromDateTime);
    }

    [Test]
    public async Task WriteColumn_UnspecifiedKind_InterpretedInColumnTimezone()
    {
        // Consistency with the HTTP client (and the DateTime codec): a Kind=Unspecified wall-clock is interpreted
        // in the column's timezone, not UTC. 2024-01-15 10:30:00 in a +05:00 column is 2024-01-15 05:30:00Z.
        const string type = "DateTime64(3, 'Fixed/UTC+05:00:00')";
        DateTime64ColumnCodec codec = Codec(type);
        var unspecified = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Unspecified);

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTime>("c", type, new[] { unspecified })));

        long expectedMillis = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)).ToUnixTimeMilliseconds();
        Assert.That(BitConverter.ToInt64(bytes), Is.EqualTo(expectedMillis));
    }

    [Test]
    public async Task ReadColumn_FixedUtcOffsetTimeZoneInfoCannotHold_ReadsTheCountsAndReportsOnlyTheZone()
    {
        // The counts are the wire value and need no zone, so a zone TimeZoneInfo cannot hold (26.6 applies
        // Fixed/UTC+05:30:15, which is not a whole number of minutes) must not fail the read. DateTime64 carries
        // its own zone field and projection, so it is not covered by the DateTime codec's case.
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

    // The write side of the same rule. A Utc DateTime names an instant, so the zone the column declares is not
    // needed and must not be resolved: this codec's own scaling is what has to run.
    [Test]
    public async Task WriteColumn_UtcDateTimeIntoAZoneTimeZoneInfoCannotHold_WritesTheInstant()
    {
        const string type = "DateTime64(3, 'Fixed/UTC+19:00:00')";
        var value = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        byte[] bytes = await WriteAsync(w => Codec(type).WriteColumn(w, new ArrayColumn<DateTime>("c", type, new[] { value })));

        Assert.That(BitConverter.ToInt64(bytes, 0), Is.EqualTo(1_705_314_600_000L));
    }

    // DateTime64 shares DateTimeColumnCodec.ToUtc; covered here too because a refactor could separate them.
    [Test]
    public void WriteColumn_UnspecifiedKindInDaylightSavingGap_ThrowsNamingTheZone()
    {
        // 2024-03-10 02:30 does not exist in New York, so it names no instant and is rejected.
        const string type = "DateTime64(3, 'America/New_York')";
        DateTime64ColumnCodec codec = Codec(type);
        var gap = new DateTime(2024, 3, 10, 2, 30, 0, DateTimeKind.Unspecified);

        ArgumentException thrown = Assert.ThrowsAsync<ArgumentException>(async () =>
            await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTime>("c", type, new[] { gap }))));

        Assert.That(thrown.Message, Does.Contain("America/New_York"));
    }

    [Test]
    public async Task WriteColumn_UnspecifiedKindInAmbiguousHour_EncodesTheEarlierOccurrence()
    {
        // 2024-11-03 01:30 happens twice in New York; the earlier is 01:30 EDT = 05:30Z.
        const string type = "DateTime64(3, 'America/New_York')";
        DateTime64ColumnCodec codec = Codec(type);
        var ambiguous = new DateTime(2024, 11, 3, 1, 30, 0, DateTimeKind.Unspecified);

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTime>("c", type, new[] { ambiguous })));

        long expected = new DateTimeOffset(2024, 11, 3, 5, 30, 0, TimeSpan.Zero).ToUnixTimeMilliseconds();
        Assert.That(BitConverter.ToInt64(bytes), Is.EqualTo(expected));
    }

    [Test]
    public void CanWrite_AcceptsTemporalColumns_RejectsOthers()
    {
        DateTime64ColumnCodec codec = Codec("DateTime64(3)", "UTC");
        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<long>("c", "DateTime64(3)", Array.Empty<long>())), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<DateTimeOffset>("c", "DateTime64(3)", Array.Empty<DateTimeOffset>())), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<DateTime>("c", "DateTime64(3)", Array.Empty<DateTime>())), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<string>("c", "DateTime64(3)", Array.Empty<string>())), Is.False);
        });
    }

    [Test]
    public void WriteColumn_WrongElementType_Throws()
    {
        DateTime64ColumnCodec codec = Codec("DateTime64(3)", "UTC");
        var column = new ArrayColumn<string>("c", "DateTime64(3)", new[] { "x" });

        Assert.ThrowsAsync<ArgumentException>(async () => await WriteAsync(w => codec.WriteColumn(w, column)));
    }

    [Test]
    public void Create_MissingScale_Throws()
        => Assert.Throws<FormatException>(() => Codec("DateTime64"));

    [Test]
    public void Create_ScaleOutOfRange_Throws()
        => Assert.Throws<FormatException>(() => Codec("DateTime64(10)"));

    [Test]
    public void WritableElementTypes_ListsLongThenOffsetThenDateTime()
        => Assert.That(
            Codec("DateTime64(3)").WritableElementTypes,
            Is.EqualTo(new[] { typeof(long), typeof(DateTimeOffset), typeof(DateTime) }));

    [Test]
    public void NullPlaceholderAs_ReturnsEpochInRequestedSpelling_ThrowsForOthers()
    {
        DateTime64ColumnCodec codec = Codec("DateTime64(3)");
        Assert.Multiple(() =>
        {
            Assert.That(codec.NullPlaceholderAs(typeof(long)), Is.EqualTo(0L));
            Assert.That(codec.NullPlaceholderAs(typeof(DateTimeOffset)), Is.EqualTo(DateTimeOffset.UnixEpoch));
            Assert.That(codec.NullPlaceholderAs(typeof(DateTime)), Is.EqualTo(DateTime.UnixEpoch));
            Assert.Throws<NotSupportedException>(() => codec.NullPlaceholderAs(typeof(string)));
        });
    }
}
