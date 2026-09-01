using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class DateTimeColumnCodecTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static DateTimeColumnCodec Codec(string type, string serverTimezone = null)
        => DateTimeColumnCodec.Create(TypeParser.Parse(type), serverTimezone);

    [Test]
    public async Task RoundTrip_UtcWholeSeconds_PreservedAsOffset()
    {
        var values = new[]
        {
            DateTimeOffset.FromUnixTimeSeconds(0),
            DateTimeOffset.FromUnixTimeSeconds(1_700_000_000),
        };
        DateTimeColumnCodec codec = Codec("DateTime('UTC')");

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTimeOffset>("c", "DateTime('UTC')", values)));
        using var reader = ReaderOver(bytes);
        using var column = (DateTimeColumn)await codec.ReadColumnAsync(reader, "c", "DateTime('UTC')", values.Length, None);

        Assert.Multiple(() =>
        {
            CollectionAssert.AreEqual(values, column.ToDateTimeOffsets());
            Assert.That(column.GetDateTimeOffset(0).Offset, Is.EqualTo(TimeSpan.Zero));
            Assert.That(column.TypeName, Is.EqualTo("DateTime('UTC')"));
        });
    }

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

    [TestCase(0u)]
    [TestCase(1_700_000_000u)]
    public async Task WriteColumn_RawSeconds_WrittenVerbatim(uint seconds)
    {
        // Raw epoch seconds are the wire representation, so they are written unchanged.
        DateTimeColumnCodec codec = Codec("DateTime");
        var column = new ArrayColumn<uint>("c", "DateTime", new[] { seconds });

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));

        Assert.That(BitConverter.ToUInt32(bytes), Is.EqualTo(seconds));
    }

    [Test]
    public async Task WriteColumn_DateTimeOffset_EncodesSameUnixSecondsAsEquivalentDateTime()
    {
        // The same instant expressed as a DateTimeOffset (with a non-zero offset) and as a UTC DateTime must
        // produce identical column bodies — both are epoch seconds of the UTC instant.
        DateTimeColumnCodec codec = Codec("DateTime");
        DateTime utc = DateTime.UnixEpoch.AddSeconds(1_700_000_000);
        var offset = new DateTimeOffset(utc, TimeSpan.Zero).ToOffset(TimeSpan.FromHours(5));

        byte[] fromDateTime = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTime>("c", "DateTime", new[] { utc })));
        byte[] fromOffset = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTimeOffset>("c", "DateTime", new[] { offset })));

        CollectionAssert.AreEqual(fromDateTime, fromOffset);
    }

    [Test]
    public void WriteColumn_DateTimeOutsideClickHouseRange_Throws()
    {
        DateTimeColumnCodec codec = Codec("DateTime");
        using var ms = new MemoryStream();
        using var writer = new ClickHouseBinaryWriter(ms);
        var beforeEpoch = new ArrayColumn<DateTime>("c", "DateTime", new[] { new DateTime(1969, 12, 31, 23, 59, 59, DateTimeKind.Utc) });
        var pastMax = new ArrayColumn<DateTime>("c", "DateTime", new[] { new DateTime(2200, 1, 1, 0, 0, 0, DateTimeKind.Utc) });

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => codec.WriteColumn(writer, beforeEpoch));
            Assert.Throws<ArgumentOutOfRangeException>(() => codec.WriteColumn(writer, pastMax));
        });
    }

    [Test]
    public async Task WriteColumn_UnspecifiedKindTimezonelessColumn_TreatedAsUtcNotMachineLocal()
    {
        // A timezone-less column resolves to UTC, so a Kind=Unspecified wall-clock encodes as UTC — and the wire
        // bytes never depend on the host machine's timezone.
        DateTimeColumnCodec codec = Codec("DateTime");
        var unspecified = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Unspecified);
        var utc = DateTime.SpecifyKind(unspecified, DateTimeKind.Utc);

        byte[] fromUnspecified = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTime>("c", "DateTime", new[] { unspecified })));
        byte[] fromUtc = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTime>("c", "DateTime", new[] { utc })));

        CollectionAssert.AreEqual(fromUtc, fromUnspecified);
    }

    [Test]
    public async Task WriteColumn_UnspecifiedKindTimezoneBearingColumn_InterpretedInColumnTimezone()
    {
        // Consistency with the HTTP client: a Kind=Unspecified wall-clock is interpreted in the column's
        // timezone, not UTC. 2024-01-15 10:30:00 in a +05:00 column is the instant 2024-01-15 05:30:00Z.
        DateTimeColumnCodec codec = Codec("DateTime('Fixed/UTC+05:00:00')");
        var unspecified = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Unspecified);

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTime>("c", "DateTime('Fixed/UTC+05:00:00')", new[] { unspecified })));

        long expected = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)).ToUnixTimeSeconds();
        Assert.That(BitConverter.ToUInt32(bytes), Is.EqualTo((uint)expected));
    }

    [Test]
    public void WriteColumn_UnspecifiedKindInDaylightSavingGap_ThrowsNamingTheZone()
    {
        // 2024-03-10 02:30 does not exist in New York (02:00 jumps to 03:00), so it names no instant and is
        // rejected rather than shifted to one of the two neighbouring offsets.
        const string type = "DateTime('America/New_York')";
        DateTimeColumnCodec codec = Codec(type);
        var gap = new DateTime(2024, 3, 10, 2, 30, 0, DateTimeKind.Unspecified);

        ArgumentException thrown = Assert.ThrowsAsync<ArgumentException>(async () =>
            await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTime>("c", type, new[] { gap }))));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain("America/New_York"));
            Assert.That(thrown.Message, Does.Contain("2024-03-10 02:30:00"));
            Assert.That(thrown.Message, Does.Contain("DateTimeOffset"));
        });
    }

    [Test]
    public async Task WriteColumn_UnspecifiedKindInAmbiguousHour_EncodesTheEarlierOccurrence()
    {
        // 2024-11-03 01:30 happens twice in New York. The lenient rule takes the earlier, 01:30 EDT = 05:30Z, as
        // HTTP does; TimeZoneInfo alone picks standard time (06:30Z) and reports nothing — a silent hour apart.
        const string type = "DateTime('America/New_York')";
        DateTimeColumnCodec codec = Codec(type);
        var ambiguous = new DateTime(2024, 11, 3, 1, 30, 0, DateTimeKind.Unspecified);

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, new ArrayColumn<DateTime>("c", type, new[] { ambiguous })));

        long expected = new DateTimeOffset(2024, 11, 3, 5, 30, 0, TimeSpan.Zero).ToUnixTimeSeconds();
        Assert.That(BitConverter.ToUInt32(bytes), Is.EqualTo((uint)expected));
    }

    [TestCase("DateTime('Not/AZone')", "Not/AZone")]
    [TestCase("DateTime('Fixed/UTC+19:00:00')", "+19:00:00")]
    public async Task ReadColumn_TimezoneThisPlatformCannotResolve_ReadsTheSecondsAndReportsOnlyTheZone(string type, string named)
    {
        // Neither a zone with no tzdata here nor an offset TimeZoneInfo cannot hold (26.6 accepts and applies
        // Fixed/UTC+19:00:00, past its ±14 hours) may fail the read: the seconds are the wire value and need no
        // zone. Only a calendar value does, so only a calendar value reports it.
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

        FormatException thrown = Assert.ThrowsAsync<FormatException>(() => WriteAsync(w => codec.WriteColumn(w, values)));
        Assert.That(thrown.Message, Does.Contain("Fixed/UTC+19:00:00"));
    }

    [Test]
    public void CanWrite_AcceptsRawSecondsDateTimeAndDateTimeOffset_RejectsOthers()
    {
        DateTimeColumnCodec codec = Codec("DateTime");
        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<uint>("c", "DateTime", Array.Empty<uint>())), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<DateTime>("c", "DateTime", Array.Empty<DateTime>())), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<DateTimeOffset>("c", "DateTime", Array.Empty<DateTimeOffset>())), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<string>("c", "DateTime", Array.Empty<string>())), Is.False);
        });
    }

    [Test]
    public void WritableElementTypes_ListsUIntThenOffsetThenDateTime()
        => Assert.That(Codec("DateTime").WritableElementTypes, Is.EqualTo(new[] { typeof(uint), typeof(DateTimeOffset), typeof(DateTime) }));

    [Test]
    public void NullPlaceholderAs_ReturnsEpochInRequestedSpelling_ThrowsForOthers()
    {
        DateTimeColumnCodec codec = Codec("DateTime");
        Assert.Multiple(() =>
        {
            Assert.That(codec.NullPlaceholderAs(typeof(uint)), Is.EqualTo(0u));
            Assert.That(codec.NullPlaceholderAs(typeof(DateTimeOffset)), Is.EqualTo(DateTimeOffset.UnixEpoch));
            Assert.That(codec.NullPlaceholderAs(typeof(DateTime)), Is.EqualTo(DateTime.UnixEpoch));
            Assert.Throws<NotSupportedException>(() => codec.NullPlaceholderAs(typeof(string)));
        });
    }

    private static async Task<byte[]> WriteAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));
}
