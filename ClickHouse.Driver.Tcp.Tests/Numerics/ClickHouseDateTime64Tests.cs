using System;
using ClickHouse.Driver.Tcp.Numerics;

namespace ClickHouse.Driver.Tcp.Tests.Numerics;

[TestFixture]
public class ClickHouseDateTime64Tests
{
    [Test]
    public void Constructor_ScaleOutOfRange_Throws()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new ClickHouseDateTime64(0, -1, TimeSpan.Zero));
            Assert.Throws<ArgumentOutOfRangeException>(() => new ClickHouseDateTime64(0, 10, TimeSpan.Zero));
        });
    }

    [TestCase(1_500L, 3, 1_500_000_000L)]        // 1.5 s at ms scale -> 1.5e9 ns
    [TestCase(1L, 9, 1L)]                          // 1 ns
    [TestCase(-1_000_000_001L, 9, -1_000_000_001L)] // 1 ns before -1 s
    [TestCase(5L, 0, 5_000_000_000L)]              // 5 s
    public void ToUnixTimeNanoseconds_IsExactAtEveryScale(long count, int scale, long expectedNanos)
    {
        var value = new ClickHouseDateTime64(count, scale, TimeSpan.Zero);
        Assert.That(value.ToUnixTimeNanoseconds(), Is.EqualTo((Int128)expectedNanos));
    }

    [Test]
    public void ToDateTimeOffset_Scale7_IsLossless()
    {
        // Scale 7 (100 ns) is exactly a .NET tick, so the round-trip through DateTimeOffset is exact.
        var original = new DateTimeOffset(2023, 11, 14, 22, 13, 20, TimeSpan.Zero).AddTicks(1234567);
        ClickHouseDateTime64 value = ClickHouseDateTime64.FromDateTimeOffset(original, 7);

        Assert.That(value.ToDateTimeOffset(), Is.EqualTo(original));
    }

    [Test]
    public void ToDateTimeOffset_Scale9_TruncatesSubTickButKeepsExactCount()
    {
        // 987 ns past a whole second: DateTimeOffset can only show the 900 ns (9 * 100 ns), but Count is exact.
        var value = new ClickHouseDateTime64((1_700_000_000L * 1_000_000_000L) + 987L, 9, TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(value.ToDateTimeOffset(), Is.EqualTo(DateTimeOffset.FromUnixTimeSeconds(1_700_000_000).AddTicks(9)));
            Assert.That(value.ToUnixTimeNanoseconds(), Is.EqualTo((Int128)((1_700_000_000L * 1_000_000_000L) + 987L)));
        });
    }

    [Test]
    public void Equality_ComparesByInstantAcrossScalesAndOffsets()
    {
        // The same instant at different scales, and presented with different offsets, is equal.
        var milliseconds = new ClickHouseDateTime64(1_500L, 3, TimeSpan.Zero);
        var nanoseconds = new ClickHouseDateTime64(1_500_000_000L, 9, TimeSpan.FromHours(5));
        var later = new ClickHouseDateTime64(1_501L, 3, TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(milliseconds, Is.EqualTo(nanoseconds));
            Assert.That(milliseconds.GetHashCode(), Is.EqualTo(nanoseconds.GetHashCode()));
            Assert.That(milliseconds, Is.LessThan(later));
            Assert.That(milliseconds != later, Is.True);
        });
    }

    [Test]
    public void Operators_OrderByInstant()
    {
        var earlier = new ClickHouseDateTime64(1_500L, 3, TimeSpan.Zero);
        var later = new ClickHouseDateTime64(1_501L, 3, TimeSpan.Zero);
        var earlierSameInstant = new ClickHouseDateTime64(1_500_000_000L, 9, TimeSpan.FromHours(3));

        Assert.Multiple(() =>
        {
            Assert.That(earlier < later, Is.True);
            Assert.That(later > earlier, Is.True);
            Assert.That(earlier <= earlierSameInstant, Is.True);
            Assert.That(earlier >= earlierSameInstant, Is.True);
            Assert.That(earlier == earlierSameInstant, Is.True);
            Assert.That(earlier.Equals((object)earlierSameInstant), Is.True);
            Assert.That(earlier.Equals("not a datetime64"), Is.False);
        });
    }

    [Test]
    public void FromDateTimeOffset_ScaleOutOfRange_Throws()
        => Assert.Throws<ArgumentOutOfRangeException>(() => ClickHouseDateTime64.FromDateTimeOffset(DateTimeOffset.UnixEpoch, 10));

    [TestCase(1_700_000_000_123L, 3, 0.0, "2023-11-14 22:13:20.123 +00:00")]
    [TestCase(1_700_000_000_123_456_789L, 9, 0.0, "2023-11-14 22:13:20.123456789 +00:00")]
    [TestCase(5L, 0, 0.0, "1970-01-01 00:00:05 +00:00")]
    [TestCase(-1L, 9, 0.0, "1969-12-31 23:59:59.999999999 +00:00")]
    [TestCase(1_700_000_000_123L, 3, 5.5, "2023-11-15 03:43:20.123 +05:30")]
    public void ToString_RendersFullPrecisionInvariant(long count, int scale, double offsetHours, string expected)
    {
        var value = new ClickHouseDateTime64(count, scale, TimeSpan.FromHours(offsetHours));
        Assert.That(value.ToString(), Is.EqualTo(expected));
    }
}
