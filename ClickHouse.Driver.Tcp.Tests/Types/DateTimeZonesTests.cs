using System;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Timezone fallback for the <c>DateTime</c> / <c>DateTime64</c> codecs: the type string's zone, then the session's,
/// then UTC. The codecs' own tests always supply one of the first two, so the UTC step is pinned here.
///
/// <para>
/// Resolution itself never throws. A name this platform cannot represent is reported by
/// <c>ResolvedTimeZone.Value</c>, which only a caller asking for a calendar value reaches, so the counts a column
/// carries still read.
/// </para>
/// </summary>
[TestFixture]
public class DateTimeZonesTests
{
    [Test]
    public void Resolve_NoExplicitAndNoServerTimezone_ReturnsUtc()
        => Assert.That(DateTimeZones.Resolve(explicitTimezone: null, serverTimezone: null).Value, Is.EqualTo(TimeZoneInfo.Utc));

    // Empty means absent, not a zone id: an unset session timezone can arrive empty rather than null.
    [TestCase("", "")]
    [TestCase(null, "")]
    [TestCase("", null)]
    public void Resolve_EmptyTimezones_ReturnsUtc(string explicitTimezone, string serverTimezone)
        => Assert.That(DateTimeZones.Resolve(explicitTimezone, serverTimezone).Value, Is.EqualTo(TimeZoneInfo.Utc));

    [Test]
    public void Resolve_NoExplicitTimezone_UsesTheServerTimezone()
        => Assert.That(
            DateTimeZones.Resolve(explicitTimezone: null, serverTimezone: "Asia/Kolkata").Value.BaseUtcOffset,
            Is.EqualTo(new TimeSpan(5, 30, 0)));

    [Test]
    public void Resolve_ExplicitTimezone_TakesPrecedenceOverTheServerTimezone()
        => Assert.That(
            DateTimeZones.Resolve(explicitTimezone: "Asia/Kolkata", serverTimezone: "America/New_York").Value.BaseUtcOffset,
            Is.EqualTo(new TimeSpan(5, 30, 0)));

    [Test]
    public void Resolve_UnknownTimezone_IsUnresolvedAndThrowsWhenUsed()
    {
        ResolvedTimeZone zone = DateTimeZones.Resolve("Not/AZone", serverTimezone: null);

        Assert.Multiple(() =>
        {
            Assert.That(zone.IsResolved, Is.False);
            Assert.That(Assert.Throws<FormatException>(() => _ = zone.Value).Message, Does.Contain("Not/AZone"));
        });
    }

    // The synthetic fixed-offset name is not an IANA id, so it is parsed rather than looked up. The three
    // components are summed as written, which is what the server does: 26.6 applies Fixed/UTC+05:00:60 as
    // +05:01:00 and Fixed/UTC+05:70:00 as +06:10:00, keeping the name it was given.
    [TestCase("Fixed/UTC+00:00:00", 0, 0)]
    [TestCase("Fixed/UTC+05:30:00", 5, 30)]
    [TestCase("Fixed/UTC-07:00:00", -7, 0)]
    [TestCase("Fixed/UTC+14:00:00", 14, 0)]
    [TestCase("Fixed/UTC+05:00:60", 5, 1)]
    [TestCase("Fixed/UTC+05:70:00", 6, 10)]
    [TestCase("Fixed/UTC+13:60:00", 14, 0)]
    public void Resolve_FixedOffsetName_IsTheSumOfItsComponents(string id, int hours, int minutes)
        => Assert.That(
            DateTimeZones.Resolve(id, serverTimezone: null).Value.BaseUtcOffset,
            Is.EqualTo(new TimeSpan(hours, minutes, 0)));

    // Offsets the server accepts and applies (checked on 26.6) that TimeZoneInfo cannot hold: past ±14 hours, or
    // not a whole number of minutes. The message has to name the offset, since the name alone does not say which
    // rule it breaks.
    [TestCase("Fixed/UTC+19:00:00", "+19:00:00")]
    [TestCase("Fixed/UTC-18:00:00", "-18:00:00")]
    [TestCase("Fixed/UTC+05:30:15", "+05:30:15")]
    [TestCase("Fixed/UTC-00:00:01", "-00:00:01")]
    public void Resolve_OffsetTimeZoneInfoCannotHold_IsUnresolvedAndNamesTheOffset(string id, string offset)
    {
        ResolvedTimeZone zone = DateTimeZones.Resolve(id, serverTimezone: null);

        Assert.Multiple(() =>
        {
            Assert.That(zone.IsResolved, Is.False);
            Assert.That(
                Assert.Throws<FormatException>(() => _ = zone.Value).Message,
                Does.Contain(id).And.Contain(offset));
        });
    }

    // A name that looks synthetic but is not one of the server's: three components is the only shape it emits.
    [TestCase("Fixed/UTC+05:30")]
    [TestCase("Fixed/UTC+5:30:00")]
    [TestCase("Fixed/UTC 05:30:00")]
    public void Resolve_MalformedFixedOffsetName_FallsThroughToTheLookupAndIsUnresolved(string id)
        => Assert.That(DateTimeZones.Resolve(id, serverTimezone: null).IsResolved, Is.False);
}
