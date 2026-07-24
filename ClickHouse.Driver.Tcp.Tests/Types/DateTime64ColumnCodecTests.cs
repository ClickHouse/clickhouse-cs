using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class DateTime64ColumnCodecTests
{
    private static DateTime64ColumnCodec Codec(string type, string tz = null) => DateTime64ColumnCodec.Create(TypeParser.Parse(type), tz);

    [TestCase("DateTime64(0)")]
    [TestCase("DateTime64(3)")]
    [TestCase("DateTime64(6)")]
    [TestCase("DateTime64(7)")]
    public async Task RoundTrip_PreservesInstantAtScale(string type)
    {
        DateTime64ColumnCodec codec = Codec(type, "UTC");
        var values = new[]
        {
            DateTimeOffset.FromUnixTimeSeconds(0),
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000),
            DateTimeOffset.FromUnixTimeSeconds(-1_000_000),
        };

        using var column = (IColumn<ClickHouseDateTime64>)await RoundTripAsync(codec, new ArrayColumn<DateTimeOffset>("c", type, values), type, values.Length);

        Assert.Multiple(() =>
        {
            for (int i = 0; i < values.Length; i++)
            {
                Assert.That(column[i].ToDateTimeOffset(), Is.EqualTo(values[i]));
            }
        });
    }

    [Test]
    public async Task RoundTrip_Scale9_PreservesNanosecondPrecision()
    {
        // A nanosecond count with sub-100 ns digits that no DateTimeOffset could hold must survive verbatim.
        const string type = "DateTime64(9)";
        DateTime64ColumnCodec codec = Codec(type, "UTC");
        var values = new[]
        {
            new ClickHouseDateTime64(1_700_000_000_123_456_789L, 9, TimeSpan.Zero),
            new ClickHouseDateTime64(-1_000_000_001L, 9, TimeSpan.Zero),
        };

        using var column = (IColumn<ClickHouseDateTime64>)await RoundTripAsync(codec, new ArrayColumn<ClickHouseDateTime64>("c", type, values), type, values.Length);

        Assert.Multiple(() =>
        {
            Assert.That(column[0].Count, Is.EqualTo(1_700_000_000_123_456_789L));
            Assert.That(column[0].Scale, Is.EqualTo(9));
            Assert.That(column[1].Count, Is.EqualTo(-1_000_000_001L));
        });
    }

    [TestCase(5L, 0, "DateTime64(3)", 5000L)]  // widening: 5 s written to a ms column -> 5000
    [TestCase(5000L, 3, "DateTime64(0)", 5L)]   // exact narrowing: 5000 ms written to a second column -> 5
    public async Task WriteColumn_RescalesNativeValueToColumnScale(long count, int scale, string type, long expectedWire)
    {
        DateTime64ColumnCodec codec = Codec(type, "UTC");
        var column = new ArrayColumn<ClickHouseDateTime64>("c", type, new[] { new ClickHouseDateTime64(count, scale, TimeSpan.Zero) });

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));

        Assert.That(BitConverter.ToInt64(bytes), Is.EqualTo(expectedWire));
    }

    [Test]
    public async Task ReadColumn_ValuesSpan_MaterializesEveryRowAsStruct()
    {
        const string type = "DateTime64(9)";
        var inserted = new[]
        {
            new ClickHouseDateTime64(0L, 9, TimeSpan.Zero),
            new ClickHouseDateTime64(1_700_000_000_123_456_789L, 9, TimeSpan.Zero),
        };
        DateTime64ColumnCodec codec = Codec(type, "UTC");

        using var column = (IColumn<ClickHouseDateTime64>)await RoundTripAsync(codec, new ArrayColumn<ClickHouseDateTime64>("c", type, inserted), type, inserted.Length);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(2));
            CollectionAssert.AreEqual(inserted, column.Values.ToArray());
            Assert.That(column.GetValue(1), Is.EqualTo(inserted[1]));
        });
    }

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumn()
    {
        using var reader = ReaderOver(Array.Empty<byte>());
        using var column = (IColumn<ClickHouseDateTime64>)await Codec("DateTime64(3)", "UTC").ReadColumnAsync(reader, "c", "DateTime64(3)", 0, None);
        Assert.That(column.RowCount, Is.EqualTo(0));
    }

    [Test]
    public void WriteColumn_NarrowingScaleLosesPrecision_Throws()
    {
        // A scale-9 value with sub-millisecond digits cannot be written to a scale-3 column without loss.
        DateTime64ColumnCodec codec = Codec("DateTime64(3)", "UTC");
        var column = new ArrayColumn<ClickHouseDateTime64>("c", "DateTime64(3)", new[] { new ClickHouseDateTime64(1_234_567L, 9, TimeSpan.Zero) });

        Assert.ThrowsAsync<ArgumentException>(async () => await WriteAsync(w => codec.WriteColumn(w, column)));
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
    public async Task ReadColumn_Scale3_ScalesMillisecondsToInstant()
    {
        // A DateTime64(3) count of 1500 = 1.5 seconds after the epoch.
        byte[] bytes = await WriteAsync(w => w.WriteInt64(1500));
        using var reader = ReaderOver(bytes);

        using var column = (IColumn<ClickHouseDateTime64>)await Codec("DateTime64(3)", "UTC").ReadColumnAsync(reader, "c", "DateTime64(3)", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0].Count, Is.EqualTo(1500));
            Assert.That(column[0].ToDateTimeOffset(), Is.EqualTo(DateTimeOffset.FromUnixTimeMilliseconds(1500)));
        });
    }

    [Test]
    public async Task ReadColumn_ExposesRawCountsStructAndOffsetViews()
    {
        // The specialized column offers three views over the same rows: raw counts, the ClickHouseDateTime64
        // struct (default), and a DateTimeOffset. Scale and timezone are column-level.
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
            CollectionAssert.AreEqual(new[] { 1500L, -2500L }, column.Counts.ToArray());
            Assert.That(column.GetCount(0), Is.EqualTo(1500L));
            Assert.That(column[0].Count, Is.EqualTo(1500L)); // the struct view
            Assert.That(column.GetDateTimeOffset(0), Is.EqualTo(DateTimeOffset.FromUnixTimeMilliseconds(1500)));
            Assert.That(column.GetDateTimeOffset(1), Is.EqualTo(DateTimeOffset.FromUnixTimeMilliseconds(-2500)));
        });
    }

    [Test]
    public async Task ReadColumn_ExplicitTimezone_PresentsOffset()
    {
        byte[] bytes = await WriteAsync(w => w.WriteInt64(1_700_000_000_000)); // scale 3, winter instant
        using var reader = ReaderOver(bytes);

        using var column = (IColumn<ClickHouseDateTime64>)await Codec("DateTime64(3, 'Asia/Kolkata')").ReadColumnAsync(reader, "c", "DateTime64(3, 'Asia/Kolkata')", 1, None);

        Assert.That(column[0].Offset, Is.EqualTo(new TimeSpan(5, 30, 0)));
    }

    [Test]
    public async Task ReadColumn_DaylightSavingZoneSummerInstant_PresentsDaylightOffset()
    {
        // A summer instant read as Europe/London presents +01:00 (British Summer Time); the offset is resolved
        // per instant, so a daylight-saving zone cannot use the fixed-offset fast path a zone like UTC does.
        byte[] bytes = await WriteAsync(w => w.WriteInt64(1_689_300_000_000)); // scale 3, 2023-07-14, summer
        using var reader = ReaderOver(bytes);

        using var column = (IColumn<ClickHouseDateTime64>)await Codec("DateTime64(3, 'Europe/London')").ReadColumnAsync(reader, "c", "DateTime64(3, 'Europe/London')", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column[0].Count, Is.EqualTo(1_689_300_000_000L));
            Assert.That(column[0].Offset, Is.EqualTo(TimeSpan.FromHours(1)));
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
    public void CanWrite_AcceptsTemporalColumns_RejectsOthers()
    {
        DateTime64ColumnCodec codec = Codec("DateTime64(3)", "UTC");
        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<ClickHouseDateTime64>("c", "DateTime64(3)", Array.Empty<ClickHouseDateTime64>())), Is.True);
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
    public void WritableElementTypes_ListsNativeThenOffsetThenDateTime()
        => Assert.That(
            Codec("DateTime64(3)").WritableElementTypes,
            Is.EqualTo(new[] { typeof(ClickHouseDateTime64), typeof(DateTimeOffset), typeof(DateTime) }));

    [Test]
    public void NullPlaceholderAs_ReturnsEpochInRequestedSpelling_ThrowsForOthers()
    {
        DateTime64ColumnCodec codec = Codec("DateTime64(3)");
        Assert.Multiple(() =>
        {
            Assert.That(((ClickHouseDateTime64)codec.NullPlaceholderAs(typeof(ClickHouseDateTime64))).Count, Is.EqualTo(0));
            Assert.That(codec.NullPlaceholderAs(typeof(DateTimeOffset)), Is.EqualTo(DateTimeOffset.UnixEpoch));
            Assert.That(codec.NullPlaceholderAs(typeof(DateTime)), Is.EqualTo(DateTime.UnixEpoch));
            Assert.Throws<NotSupportedException>(() => codec.NullPlaceholderAs(typeof(string)));
        });
    }
}
