using System;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Timezone fallback for the <c>DateTime</c> / <c>DateTime64</c> codecs: the type string's zone, then the session's,
/// then UTC. The codecs' own tests always supply one of the first two, so the UTC step is pinned here.
/// </summary>
[TestFixture]
public class DateTimeZonesTests
{
    [Test]
    public void Resolve_NoExplicitAndNoServerTimezone_ReturnsUtc()
        => Assert.That(DateTimeZones.Resolve(explicitTimezone: null, serverTimezone: null), Is.EqualTo(TimeZoneInfo.Utc));

    // Empty means absent, not a zone id: an unset session timezone can arrive empty rather than null.
    [TestCase("", "")]
    [TestCase(null, "")]
    [TestCase("", null)]
    public void Resolve_EmptyTimezones_ReturnsUtc(string explicitTimezone, string serverTimezone)
        => Assert.That(DateTimeZones.Resolve(explicitTimezone, serverTimezone), Is.EqualTo(TimeZoneInfo.Utc));

    [Test]
    public void Resolve_NoExplicitTimezone_UsesTheServerTimezone()
        => Assert.That(
            DateTimeZones.Resolve(explicitTimezone: null, serverTimezone: "Asia/Kolkata").BaseUtcOffset,
            Is.EqualTo(new TimeSpan(5, 30, 0)));

    [Test]
    public void Resolve_ExplicitTimezone_TakesPrecedenceOverTheServerTimezone()
        => Assert.That(
            DateTimeZones.Resolve(explicitTimezone: "Asia/Kolkata", serverTimezone: "America/New_York").BaseUtcOffset,
            Is.EqualTo(new TimeSpan(5, 30, 0)));

    [Test]
    public void Resolve_UnknownTimezone_ThrowsFormat()
        => Assert.Throws<FormatException>(() => DateTimeZones.Resolve("Not/AZone", serverTimezone: null));
}
