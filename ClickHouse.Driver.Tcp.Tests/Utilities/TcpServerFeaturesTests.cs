using System;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

// Pin CI-matrix versions because an incorrect feature gate silently skips tests.
[TestFixture]
public class TcpServerFeaturesTests
{
    [TestCase("25.8", TcpFeature.QBit, ExpectedResult = false, TestName = "The oldest matrix server has no QBit")]
    [TestCase("25.8", TcpFeature.Time, ExpectedResult = true, TestName = "The oldest matrix server has Time")]
    [TestCase("25.8", TcpFeature.Json, ExpectedResult = true, TestName = "The oldest matrix server has Json")]
    [TestCase("25.11", TcpFeature.QBit, ExpectedResult = true, TestName = "QBit arrives in 25.11")]
    [TestCase("25.10", TcpFeature.QBit, ExpectedResult = false, TestName = "QBit is gated one release later than it shipped")]
    [TestCase("26.5", TcpFeature.NullableTuple, ExpectedResult = false, TestName = "Nullable Tuple Beta is unavailable before 26.6")]
    [TestCase("26.6", TcpFeature.NullableTuple, ExpectedResult = true, TestName = "Nullable Tuple Beta arrives in 26.6")]
    [TestCase("26.6", TcpFeature.Geometry, ExpectedResult = true, TestName = "A recent server has Geometry")]
    [TestCase("23.12", TcpFeature.Variant, ExpectedResult = false, TestName = "Variant predates 24.1")]
    public bool Resolve_ForAPinnedVersion_ReportsTheFeatureAsTheReleaseNotesDo(string version, TcpFeature feature)
        => TcpServerFeatures.Resolve(TcpServerFeatures.Parse(version)).HasFlag(feature);

    [TestCase(null, TestName = "Unset")]
    [TestCase("", TestName = "Empty")]
    [TestCase("latest", TestName = "The latest tag")]
    [TestCase("head", TestName = "The head tag")]
    [TestCase("clickhouse/clickhouse-server@sha256:abc", TestName = "A digest")]
    public void Resolve_WhenTheVersionIsNotPinned_AssumesEverySupported(string version)
    {
        // Assuming full support makes a genuine gap fail loudly. Assuming none would skip the whole suite.
        Assert.That(TcpServerFeatures.Resolve(TcpServerFeatures.Parse(version)), Is.EqualTo(TcpFeature.All));
    }

    [TestCase("26.6.1.1193", ExpectedResult = true, TestName = "A four-part build version")]
    [TestCase("  25.11  ", ExpectedResult = true, TestName = "Surrounding whitespace")]
    [TestCase("clickhouse/clickhouse-server:25.11", ExpectedResult = true, TestName = "A tagged image reference")]
    public bool Parse_ForTheFormsTheMatrixUses_ReadsAVersion(string version)
        => TcpServerFeatures.Parse(version) is not null;

    [Test]
    public void Resolve_ForAServerOlderThanEveryFeature_ReportsNone()
    {
        Assert.That(TcpServerFeatures.Resolve(new Version(20, 1)), Is.EqualTo(TcpFeature.None));
    }
}
