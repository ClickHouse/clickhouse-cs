using System;

namespace ClickHouse.Driver.Tcp.Tests.Client;

[TestFixture]
public class ClickHouseTcpClientOptionsTests
{
    [Test]
    public void Defaults_WhenNotOverridden_MatchNativeProtocolConventions()
    {
        var options = new ClickHouseTcpClientOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.Host, Is.EqualTo("localhost"));
            Assert.That(options.Port, Is.EqualTo(9000));
            Assert.That(options.Username, Is.EqualTo("default"));
            Assert.That(options.Password, Is.EqualTo(string.Empty));
            Assert.That(options.Database, Is.EqualTo("default"));
            Assert.That(options.DialTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(options.ReadTimeout, Is.EqualTo(TimeSpan.FromSeconds(300)));
            Assert.That(options.MaxSendBufferBytes, Is.EqualTo(10 * 1024 * 1024));
        });
    }

    [Test]
    public void Validate_ValidOptions_DoesNotThrow()
    {
        var options = new ClickHouseTcpClientOptions { Host = "example", Port = 9440, Username = "u", Database = "db" };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_EmptyHost_ThrowsArgumentException()
    {
        var options = new ClickHouseTcpClientOptions { Host = "" };

        Assert.Throws<ArgumentException>(() => options.Validate());
    }

    [Test]
    public void Validate_EmptyUsername_ThrowsArgumentException()
    {
        var options = new ClickHouseTcpClientOptions { Username = "" };

        Assert.Throws<ArgumentException>(() => options.Validate());
    }

    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(65536)]
    public void Validate_PortOutOfRange_ThrowsArgumentOutOfRangeException(int port)
    {
        var options = new ClickHouseTcpClientOptions { Port = port };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_NonPositiveDialTimeout_ThrowsArgumentOutOfRangeException()
    {
        var options = new ClickHouseTcpClientOptions { DialTimeout = TimeSpan.Zero };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_NonPositiveMaxSendBufferBytes_ThrowsArgumentOutOfRangeException()
    {
        var options = new ClickHouseTcpClientOptions { MaxSendBufferBytes = 0 };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void ToHandshakeParameters_MapsCredentialsAndDatabase()
    {
        var options = new ClickHouseTcpClientOptions
        {
            Username = "alice",
            Password = "secret",
            Database = "analytics",
            QuotaKey = "quota-1",
        };

        var handshake = options.ToHandshakeParameters();

        Assert.Multiple(() =>
        {
            Assert.That(handshake.Username, Is.EqualTo("alice"));
            Assert.That(handshake.Password, Is.EqualTo("secret"));
            Assert.That(handshake.Database, Is.EqualTo("analytics"));
            Assert.That(handshake.QuotaKey, Is.EqualTo("quota-1"));
        });
    }
}
