using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

[TestFixture]
public class ReplicaRoutingTests
{
    private const string ServiceHost = "abc123.us-east-1.aws.clickhouse.cloud";

    [Test]
    public void StickyServerName_TagAndHost_PutsTheStickyLabelBetweenThem()
    {
        Assert.That(
            ReplicaRouting.StickyServerName("etl", ServiceHost),
            Is.EqualTo("etl.sticky.abc123.us-east-1.aws.clickhouse.cloud"));
    }

    [TestCase("a")]
    [TestCase("etl")]
    [TestCase("user-42")]
    [TestCase("UPPER-and-lower-0")]
    [TestCase("0123456789")]
    public void Validate_TagIsOneDnsLabel_DoesNotThrow(string tag)
    {
        Assert.DoesNotThrow(() => TlsOptions(tag).Validate());
    }

    [Test]
    public void Validate_TagOfSixtyThreeCharacters_DoesNotThrow()
    {
        Assert.DoesNotThrow(() => TlsOptions(new string('a', 63)).Validate());
    }

    [TestCase("-etl")]
    [TestCase("etl-")]
    [TestCase("a.b")]
    [TestCase("a_b")]
    [TestCase("a b")]
    [TestCase("user@example.com")]
    [TestCase("café")]
    public void Validate_TagIsNotOneDnsLabel_ThrowsArgumentException(string tag)
    {
        var thrown = Assert.Throws<ArgumentException>(() => TlsOptions(tag).Validate());

        Assert.Multiple(() =>
        {
            Assert.That(thrown.ParamName, Is.EqualTo(nameof(ClickHouseTcpClientOptions.ReplicaTag)));
            Assert.That(thrown.Message, Does.Contain("hash"), "the message tells the caller how to route by an arbitrary value");
        });
    }

    [Test]
    public void Validate_TagOfSixtyFourCharacters_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => TlsOptions(new string('a', 64)).Validate());
    }

    [Test]
    public void Validate_EmptyTagWithoutTls_DoesNotThrow()
    {
        // An empty tag means none, as an empty query id does, so it needs no TLS.
        Assert.DoesNotThrow(() => new ClickHouseTcpClientOptions { ReplicaTag = string.Empty }.Validate());
    }

    [TestCase("10.0.0.1")]
    [TestCase("::1")]
    public void Validate_TagWithAnAddressHost_ThrowsArgumentException(string host)
    {
        var options = new ClickHouseTcpClientOptions { Host = host, UseTls = true, ReplicaTag = "etl" };

        var thrown = Assert.Throws<ArgumentException>(() => options.Validate());

        Assert.That(thrown.Message, Does.Contain(nameof(ClickHouseTcpClientOptions.TlsServerName)));
    }

    [Test]
    public void Validate_TagWithAnAddressHostAndATlsServerName_DoesNotThrow()
    {
        // The sticky name is built on the name the certificate is checked against, not on the dialled address.
        var options = new ClickHouseTcpClientOptions
        {
            Host = "10.0.0.1",
            UseTls = true,
            TlsServerName = ServiceHost,
            ReplicaTag = "etl",
        };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_StickyNameLongerThanADnsName_ThrowsArgumentException()
    {
        // 63 + ".sticky." + 190 = 261 characters, past the 253 a DNS name allows.
        string host = string.Join('.', new string('h', 63), new string('h', 63), new string('h', 62));
        var options = new ClickHouseTcpClientOptions { Host = host, UseTls = true, ReplicaTag = new string('t', 63) };

        var thrown = Assert.Throws<ArgumentException>(() => options.Validate());

        Assert.That(thrown.Message, Does.Contain("253"));
    }

    private static ClickHouseTcpClientOptions TlsOptions(string tag)
        => new() { Host = ServiceHost, UseTls = true, ReplicaTag = tag };
}
