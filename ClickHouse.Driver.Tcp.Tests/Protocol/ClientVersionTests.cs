using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

[TestFixture]
public class ClientVersionTests
{
    [TestCase("1.5.0+3f2a1c9", 1, 5, 0)]
    [TestCase("1.4.0-rc2+3f2a1c9", 1, 4, 0)]
    [TestCase("2.10.3", 2, 10, 3)]
    [TestCase("1.5", 1, 5, 0)]
    public void Parse_InformationalVersion_ReadsMajorMinorAndPatch(string informationalVersion, int major, int minor, int patch)
    {
        Assert.That(ClientVersion.Parse(informationalVersion), Is.EqualTo((major, minor, patch)));
    }

    // 0.0.0 is what a benchmark build carries, and it is the one version a server refuses for a distributed query.
    [TestCase("0.0.0-baseline")]
    [TestCase("0.0.0-pr.724+3f2a1c9")]
    [TestCase("not a version")]
    [TestCase("1")]
    [TestCase("")]
    [TestCase(null)]
    public void Parse_NoUsableVersion_ReturnsTheFallback(string informationalVersion)
    {
        Assert.That(ClientVersion.Parse(informationalVersion), Is.EqualTo(ClientVersion.Fallback));
    }

    [Test]
    public void ClientHandshakeParameters_Defaults_ReportTheDriverVersion()
    {
        var parameters = new ClientHandshakeParameters { Username = "default" };

        Assert.Multiple(() =>
        {
            Assert.That((parameters.VersionMajor, parameters.VersionMinor, parameters.VersionPatch), Is.EqualTo(ClientVersion.Current));
            Assert.That(ClientVersion.Current, Is.Not.EqualTo((0, 0, 0)));
        });
    }
}
