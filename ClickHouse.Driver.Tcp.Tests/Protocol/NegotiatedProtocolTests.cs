using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

[TestFixture]
public class NegotiatedProtocolTests
{
    [Test]
    public void Version_ServerAboveClient_IsClampedToClientCeiling()
    {
        var negotiated = new NegotiatedProtocol(serverRevision: 54473);
        Assert.That(negotiated.Version, Is.EqualTo(NegotiatedProtocol.ClientTcpProtocolVersion));
        Assert.That(negotiated.Version, Is.EqualTo(54460));
    }

    [Test]
    public void Version_ServerBelowClient_TakesServerRevision()
    {
        var negotiated = new NegotiatedProtocol(serverRevision: 54000);
        Assert.That(negotiated.Version, Is.EqualTo(54000));
    }

    [Test]
    public void Supports_FeatureAtOrBelowNegotiatedVersion_IsTrue()
    {
        var negotiated = new NegotiatedProtocol(serverRevision: 54460);
        Assert.Multiple(() =>
        {
            Assert.That(negotiated.Supports(ProtocolFeature.Timezone), Is.True);      // 54058
            Assert.That(negotiated.Supports(ProtocolFeature.DisplayName), Is.True);   // 54372
            Assert.That(negotiated.Supports(ProtocolFeature.VersionPatch), Is.True);  // 54401
            Assert.That(negotiated.Supports(ProtocolFeature.Addendum), Is.True);      // 54458
        });
    }

    [Test]
    public void Supports_FeatureAboveNegotiatedVersion_IsFalse()
    {
        // Features gated above the client ceiling are never active, even against a newer server (the
        // negotiated version is clamped to the ceiling).
        var negotiated = new NegotiatedProtocol(serverRevision: 54473);
        Assert.Multiple(() =>
        {
            Assert.That(negotiated.Supports(ProtocolFeature.ChunkedProtocol), Is.False);   // 54470
            Assert.That(negotiated.Supports(ProtocolFeature.ParallelReplicas), Is.False);  // 54471
        });
    }

    [Test]
    public void Supports_FeatureExactlyAtNegotiatedVersion_IsTrue()
    {
        // A feature is active at >= its introducing version, so the introducing version itself qualifies while
        // one below does not.
        Assert.Multiple(() =>
        {
            Assert.That(new NegotiatedProtocol(54058).Supports(ProtocolFeature.Timezone), Is.True);
            Assert.That(new NegotiatedProtocol(54057).Supports(ProtocolFeature.Timezone), Is.False);
        });
    }

    [Test]
    public void Supports_OlderServer_GatesOutNewerFields()
    {
        var negotiated = new NegotiatedProtocol(serverRevision: 54000);
        Assert.Multiple(() =>
        {
            Assert.That(negotiated.Supports(ProtocolFeature.Timezone), Is.False);     // 54058 > 54000
            Assert.That(negotiated.Supports(ProtocolFeature.VersionPatch), Is.False); // 54401 > 54000
        });
    }
}
