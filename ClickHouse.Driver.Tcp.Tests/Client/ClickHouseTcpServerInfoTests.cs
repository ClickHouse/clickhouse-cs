using System;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// Only what a live server cannot show: the derived Version and ToString, and that a patch the server never
// sent still yields a three-part version rather than throwing.
[TestFixture]
public class ClickHouseTcpServerInfoTests
{
    [Test]
    public void Version_ComposesTheThreeParts()
    {
        var info = new ClickHouseTcpServerInfo { VersionMajor = 25, VersionMinor = 8, VersionPatch = 3 };

        Assert.That(info.Version, Is.EqualTo(new Version(25, 8, 3)));
    }

    [Test]
    public void Version_PatchNotSent_IsZeroRatherThanUnset()
    {
        // The handshake omits the patch below the revision that introduced it, so it defaults to 0. A
        // two-part Version would compare unequal to a three-part one, so it has to stay three parts.
        var info = new ClickHouseTcpServerInfo { VersionMajor = 25, VersionMinor = 8 };

        Assert.Multiple(() =>
        {
            Assert.That(info.Version, Is.EqualTo(new Version(25, 8, 0)));
            Assert.That(info.Version.Build, Is.EqualTo(0));
        });
    }

    [Test]
    public void ToString_RendersNameAndThreePartVersion()
    {
        var info = new ClickHouseTcpServerInfo { Name = "ClickHouse", VersionMajor = 25, VersionMinor = 8, VersionPatch = 3 };

        Assert.That(info.ToString(), Is.EqualTo("ClickHouse 25.8.3"));
    }

    [Test]
    public void TimezoneAndDisplayName_DefaultToEmptyRatherThanNull()
    {
        // Both are blank when the negotiated revision predates them; empty keeps callers off a null check.
        var info = new ClickHouseTcpServerInfo();

        Assert.Multiple(() =>
        {
            Assert.That(info.Timezone, Is.Empty);
            Assert.That(info.DisplayName, Is.Empty);
        });
    }
}
