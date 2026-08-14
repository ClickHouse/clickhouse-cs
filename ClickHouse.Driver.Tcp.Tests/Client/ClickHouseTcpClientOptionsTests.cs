using System;
using System.Collections.Generic;
using System.Reflection;

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
    public void Validate_CustomSettingWithEmptyName_ThrowsArgumentException()
    {
        // An empty name would be written as the empty key that terminates the settings list on the wire, so the
        // server would stop reading settings there and misread the following bytes as the next Query fields.
        var options = new ClickHouseTcpClientOptions
        {
            CustomSettings = new Dictionary<string, string> { [string.Empty] = "1" },
        };

        Assert.Throws<ArgumentException>(() => options.Validate());
    }

    [Test]
    public void Validate_CustomSettingWithNullValue_ThrowsArgumentException()
    {
        // A null value cannot be written, and it would otherwise throw mid-packet with bytes already buffered.
        var options = new ClickHouseTcpClientOptions
        {
            CustomSettings = new Dictionary<string, string> { ["max_threads"] = null },
        };

        Assert.Throws<ArgumentException>(() => options.Validate());
    }

    [Test]
    public void Validate_CustomSettingWithEmptyValue_DoesNotThrow()
    {
        // An empty value is the flag-style setting the null-value message points callers at.
        var options = new ClickHouseTcpClientOptions
        {
            CustomSettings = new Dictionary<string, string> { ["some_flag"] = string.Empty },
        };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_ValidCustomSettings_DoesNotThrow()
    {
        var options = new ClickHouseTcpClientOptions
        {
            CustomSettings = new Dictionary<string, string> { ["max_threads"] = "4", ["max_block_size"] = "1000" },
        };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void WithOwnedCustomSettings_CopiesEveryPropertyAndSnapshotsTheSettings()
    {
        // WithOwnedCustomSettings copies each property by hand, so a property added later could silently stop being
        // carried across. The two reflection loops make that a test failure: the first insists this test sets every
        // property to a non-default value, and the second insists the copy preserves each one.
        var original = new ClickHouseTcpClientOptions
        {
            Host = "copy-host",
            Port = 1234,
            Username = "copy-user",
            Password = "copy-password",
            Database = "copy-db",
            QuotaKey = "copy-quota",
            CustomSettings = new Dictionary<string, string> { ["max_threads"] = "4" },
            MaxSendBufferBytes = 4096,
            DialTimeout = TimeSpan.FromSeconds(3),
            ReadTimeout = TimeSpan.FromSeconds(4),
        };
        var defaults = new ClickHouseTcpClientOptions();

        var copy = original.WithOwnedCustomSettings();

        PropertyInfo[] properties = typeof(ClickHouseTcpClientOptions).GetProperties();
        Assert.That(properties, Is.Not.Empty);
        Assert.That(copy, Is.Not.SameAs(original), "settings were present, so the copy must be a new instance");

        Assert.Multiple(() =>
        {
            foreach (PropertyInfo property in properties)
            {
                Assert.That(
                    property.GetValue(original),
                    Is.Not.EqualTo(property.GetValue(defaults)),
                    $"this test must set {property.Name} to a non-default value for the copy check below to mean anything");

                if (property.Name == nameof(ClickHouseTcpClientOptions.CustomSettings))
                {
                    Assert.That(property.GetValue(copy), Is.Not.SameAs(property.GetValue(original)), "the settings must be a private snapshot");
                    Assert.That(copy.CustomSettings, Is.EquivalentTo(original.CustomSettings));
                }
                else
                {
                    Assert.That(property.GetValue(copy), Is.EqualTo(property.GetValue(original)), $"{property.Name} was not carried into the copy");
                }
            }
        });
    }

    [Test]
    public void WithOwnedCustomSettings_NoCustomSettings_ReturnsTheSameInstance()
    {
        // Nothing mutable to snapshot: every other property is init-only, so the instance can be shared as-is.
        var options = new ClickHouseTcpClientOptions { Host = "h" };

        Assert.That(options.WithOwnedCustomSettings(), Is.SameAs(options));
    }

    [Test]
    public void WithOwnedCustomSettings_CallerMutatesSettingsAfterwards_SnapshotUnaffected()
    {
        var callerSettings = new Dictionary<string, string> { ["max_threads"] = "4" };
        var options = new ClickHouseTcpClientOptions { CustomSettings = callerSettings };

        var copy = options.WithOwnedCustomSettings();
        callerSettings["max_threads"] = "8";
        callerSettings["added_later"] = "1";

        Assert.Multiple(() =>
        {
            Assert.That(copy.CustomSettings["max_threads"], Is.EqualTo("4"));
            Assert.That(copy.CustomSettings.ContainsKey("added_later"), Is.False);
        });
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
