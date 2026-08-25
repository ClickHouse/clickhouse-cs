using System;
using System.Collections.Generic;
using System.Reflection;
using ClickHouse.Driver.Compression;
using Microsoft.Extensions.Logging.Abstractions;

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
            Assert.That(options.Port, Is.Null, "the port is derived from UseTls unless set");
            Assert.That(options.Username, Is.EqualTo("default"));
            Assert.That(options.UseTls, Is.False);
            Assert.That(options.TlsServerName, Is.Null);
            Assert.That(options.TlsAllowInvalidCertificates, Is.False);
            Assert.That(options.TlsCaCertificatePath, Is.Null);
            Assert.That(options.ConfigureTls, Is.Null);
            Assert.That(options.Password, Is.EqualTo(string.Empty));
            Assert.That(options.Database, Is.EqualTo("default"));
            Assert.That(options.DialTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(options.ReadTimeout, Is.EqualTo(TimeSpan.FromSeconds(300)));
            Assert.That(options.MaxSendBufferBytes, Is.EqualTo(1024 * 1024));
            Assert.That(options.MinPoolSize, Is.Zero);
            Assert.That(options.MaxPoolSize, Is.EqualTo(20));
            Assert.That(options.PoolTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(options.MaxConnectionLifetime, Is.EqualTo(TimeSpan.FromMinutes(30)));
            Assert.That(options.IdleTimeout, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(options.PoolReusePolicy, Is.EqualTo(ClickHouseTcpPoolReusePolicy.Lifo));
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

    [Test]
    public void Validate_EmptyDatabase_ThrowsArgumentException()
    {
        var options = new ClickHouseTcpClientOptions { Database = "" };

        Assert.Throws<ArgumentException>(() => options.Validate());
    }

    [Test]
    public void Validate_NonPositiveReadTimeout_ThrowsArgumentOutOfRangeException()
    {
        var options = new ClickHouseTcpClientOptions { ReadTimeout = TimeSpan.Zero };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
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
    public void ResolvedPort_UseTlsAndNoExplicitPort_IsTheSecureNativePort()
    {
        var options = new ClickHouseTcpClientOptions { UseTls = true };

        Assert.That(options.ResolvedPort, Is.EqualTo(9440));
    }

    [Test]
    public void ResolvedPort_NoTlsAndNoExplicitPort_IsThePlaintextNativePort()
    {
        var options = new ClickHouseTcpClientOptions();

        Assert.That(options.ResolvedPort, Is.EqualTo(9000));
    }

    [TestCase(true)]
    [TestCase(false)]
    public void ResolvedPort_ExplicitPort_IsHonouredWhateverTlsSays(bool useTls)
    {
        // Deriving the port must never override a caller who named one: TLS on a non-standard port is legitimate.
        var options = new ClickHouseTcpClientOptions { UseTls = useTls, Port = 9123 };

        Assert.That(options.ResolvedPort, Is.EqualTo(9123));
    }

    [Test]
    public void Validate_NullPort_DoesNotThrow()
    {
        // Null is a request to derive the port, not a value, so the range check must skip it.
        Assert.DoesNotThrow(() => new ClickHouseTcpClientOptions { Port = null }.Validate());
    }

    [Test]
    public void Validate_TlsWithAPinnedAuthority_DoesNotThrow()
    {
        var options = new ClickHouseTcpClientOptions
        {
            UseTls = true,
            TlsServerName = "cert.example",
            TlsCaCertificatePath = "/etc/ca.pem",
            ConfigureTls = _ => { },
        };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [TestCase("")]
    [TestCase("   ")]
    public void Validate_EmptyTlsCaCertificatePath_ThrowsArgumentException(string path)
    {
        var options = new ClickHouseTcpClientOptions
        {
            UseTls = true,
            TlsCaCertificatePath = path,
        };

        var thrown = Assert.Throws<ArgumentException>(() => options.Validate());

        Assert.That(thrown.ParamName, Is.EqualTo(nameof(ClickHouseTcpClientOptions.TlsCaCertificatePath)));
    }

    [Test]
    public void Validate_NullTlsCaCertificatePath_UsesHostTrustAndDoesNotThrow()
    {
        var options = new ClickHouseTcpClientOptions
        {
            UseTls = true,
            TlsCaCertificatePath = null,
        };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_TlsWithValidationTurnedOff_DoesNotThrow()
    {
        var options = new ClickHouseTcpClientOptions
        {
            UseTls = true,
            TlsServerName = "cert.example",
            TlsAllowInvalidCertificates = true,
            ConfigureTls = _ => { },
        };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_ValidationTurnedOffAndAnAuthorityPinned_ThrowsArgumentException()
    {
        // Contradictory: with validation off no certificate is checked, so the authority would be read from disk
        // and never consulted. Whichever the caller meant, they did not mean both.
        var options = new ClickHouseTcpClientOptions
        {
            UseTls = true,
            TlsAllowInvalidCertificates = true,
            TlsCaCertificatePath = "/etc/ca.pem",
        };

        Assert.Throws<ArgumentException>(() => options.Validate());
    }

    [Test]
    public void Validate_EmptyTlsServerNameWithoutUseTls_DoesNotThrow()
    {
        // An empty name is what the connection factory ignores, so Validate must not treat it as configured —
        // otherwise the two disagree about what "set" means.
        Assert.DoesNotThrow(() => new ClickHouseTcpClientOptions { TlsServerName = string.Empty }.Validate());
    }

    // Named per case: ToString prints none of the four properties, so without SetName all four report under one
    // display name and a failure would not say which property regressed.
    private static readonly TestCaseData[] TlsPropertiesWithoutUseTls =
    [
        new TestCaseData(new ClickHouseTcpClientOptions { TlsServerName = "cert.example" })
            .SetName($"{{m}}({nameof(ClickHouseTcpClientOptions.TlsServerName)})"),
        new TestCaseData(new ClickHouseTcpClientOptions { TlsAllowInvalidCertificates = true })
            .SetName($"{{m}}({nameof(ClickHouseTcpClientOptions.TlsAllowInvalidCertificates)})"),
        new TestCaseData(new ClickHouseTcpClientOptions { TlsCaCertificatePath = "/etc/ca.pem" })
            .SetName($"{{m}}({nameof(ClickHouseTcpClientOptions.TlsCaCertificatePath)})"),
        new TestCaseData(new ClickHouseTcpClientOptions { ConfigureTls = _ => { } })
            .SetName($"{{m}}({nameof(ClickHouseTcpClientOptions.ConfigureTls)})"),
    ];

    [TestCaseSource(nameof(TlsPropertiesWithoutUseTls))]
    public void Validate_TlsPropertySetButUseTlsFalse_ThrowsArgumentException(ClickHouseTcpClientOptions options)
    {
        // Ignoring the property would leave the caller with a plaintext connection they configured as encrypted.
        var thrown = Assert.Throws<ArgumentException>(() => options.Validate());

        Assert.That(thrown.Message, Does.Contain(nameof(ClickHouseTcpClientOptions.UseTls)));
    }

    [Test]
    public void Validate_UseTlsWithNothingElseSet_DoesNotThrow()
    {
        Assert.DoesNotThrow(() => new ClickHouseTcpClientOptions { UseTls = true }.Validate());
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
    public void Validate_NonPositivePoolTimeout_ThrowsArgumentOutOfRangeException()
    {
        var options = new ClickHouseTcpClientOptions { PoolTimeout = TimeSpan.Zero };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_MaxPoolSizeBelowOne_ThrowsArgumentOutOfRangeException(int maxPoolSize)
    {
        var options = new ClickHouseTcpClientOptions { MaxPoolSize = maxPoolSize };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_NegativeMinPoolSize_ThrowsArgumentOutOfRangeException()
    {
        var options = new ClickHouseTcpClientOptions { MinPoolSize = -1 };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_NoSweepInterval_DoesNotThrow()
    {
        // Null is not a value out of range but a request to derive the period, so it must pass validation.
        var options = new ClickHouseTcpClientOptions { SweepInterval = null };

        Assert.Multiple(() =>
        {
            Assert.DoesNotThrow(() => options.Validate());
            Assert.That(new ClickHouseTcpClientOptions().SweepInterval, Is.Null, "deriving the period is the default");
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_NonPositiveSweepInterval_ThrowsArgumentOutOfRangeException(int seconds)
    {
        // Zero does not mean "no sweep" here, unlike the two limits: that is what leaving it null does.
        var options = new ClickHouseTcpClientOptions { SweepInterval = TimeSpan.FromSeconds(seconds) };

        var thrown = Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        Assert.That(thrown.Message, Does.Contain("SweepInterval"));
    }

    [Test]
    public void Validate_SweepIntervalTooLargeToArm_ThrowsArgumentOutOfRangeException()
    {
        var options = new ClickHouseTcpClientOptions { SweepInterval = TimeSpan.FromDays(30) };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_MinPoolSizeAboveMaxPoolSize_ThrowsArgumentOutOfRangeException()
    {
        var options = new ClickHouseTcpClientOptions { MinPoolSize = 5, MaxPoolSize = 4 };

        var thrown = Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        Assert.That(thrown.Message, Does.Contain("MaxPoolSize (4)"));
    }

    [Test]
    public void Validate_MinPoolSizeEqualToMaxPoolSize_DoesNotThrow()
    {
        var options = new ClickHouseTcpClientOptions { MinPoolSize = 4, MaxPoolSize = 4 };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_NegativeMaxConnectionLifetime_ThrowsArgumentOutOfRangeException()
    {
        var options = new ClickHouseTcpClientOptions { MaxConnectionLifetime = TimeSpan.FromSeconds(-1) };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_NegativeIdleTimeout_ThrowsArgumentOutOfRangeException()
    {
        var options = new ClickHouseTcpClientOptions { IdleTimeout = TimeSpan.FromSeconds(-1) };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_LifetimeAndIdleTimeoutDisabled_DoesNotThrow()
    {
        var options = new ClickHouseTcpClientOptions { MaxConnectionLifetime = TimeSpan.Zero, IdleTimeout = TimeSpan.Zero };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_ShortMaxConnectionLifetime_IsAccepted()
    {
        // Nothing forbids rotating connections quickly; the pool never interrupts a running operation for age,
        // so a short lifetime only means a connection is retired sooner once it comes back.
        var options = new ClickHouseTcpClientOptions { MaxConnectionLifetime = TimeSpan.FromSeconds(10) };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [TestCase(nameof(ClickHouseTcpClientOptions.DialTimeout))]
    [TestCase(nameof(ClickHouseTcpClientOptions.ReadTimeout))]
    [TestCase(nameof(ClickHouseTcpClientOptions.PoolTimeout))]
    public void Validate_TimeoutBeyondWhatATimerCanHold_ThrowsAtConstructionNotAtEveryOperation(string property)
    {
        // These feed CancelAfter / SemaphoreSlim.WaitAsync, which take an int millisecond count. Past ~24.8 days
        // the failure would otherwise surface from inside every operation instead of here.
        var tooLong = TimeSpan.FromMilliseconds(int.MaxValue) + TimeSpan.FromSeconds(1);
        var options = property switch
        {
            nameof(ClickHouseTcpClientOptions.DialTimeout) => new ClickHouseTcpClientOptions { DialTimeout = tooLong },
            nameof(ClickHouseTcpClientOptions.ReadTimeout) => new ClickHouseTcpClientOptions { ReadTimeout = tooLong },
            _ => new ClickHouseTcpClientOptions { PoolTimeout = tooLong },
        };

        var thrown = Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        Assert.That(thrown.ParamName, Is.EqualTo(property));
    }

    [Test]
    public void Validate_TimeoutAtExactlyWhatATimerCanHold_IsAccepted()
    {
        // The boundary itself: rejecting it too would be an off-by-one that only shows up at an absurd setting.
        var atLimit = TimeSpan.FromMilliseconds(int.MaxValue);
        var options = new ClickHouseTcpClientOptions { DialTimeout = atLimit, ReadTimeout = atLimit, PoolTimeout = atLimit };

        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_UndefinedPoolReusePolicy_ThrowsArgumentOutOfRangeException()
    {
        var options = new ClickHouseTcpClientOptions { PoolReusePolicy = (ClickHouseTcpPoolReusePolicy)42 };

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
        // WithOwnedCustomSettings replaces one property and must carry every other one across. The two reflection
        // loops make a regression here a test failure: the first insists this test sets every property to a
        // non-default value, and the second insists the copy preserves each one. The `with` expression satisfies
        // this by construction; the loops stay to catch a later rewrite back to a hand-written copy.
        var original = new ClickHouseTcpClientOptions
        {
            Host = "copy-host",
            Port = 1234,
            Username = "copy-user",
            Password = "copy-password",
            Database = "copy-db",
            QuotaKey = "copy-quota",
            UseTls = true,
            TlsServerName = "copy-sni",
            TlsAllowInvalidCertificates = true,
            TlsCaCertificatePath = "copy-ca.pem",
            ConfigureTls = _ => { },
            CustomSettings = new Dictionary<string, string> { ["max_threads"] = "4" },
            MaxSendBufferBytes = 4096,
            DialTimeout = TimeSpan.FromSeconds(3),
            ReadTimeout = TimeSpan.FromSeconds(4),
            MinPoolSize = 1,
            MaxPoolSize = 7,
            PoolTimeout = TimeSpan.FromSeconds(5),
            MaxConnectionLifetime = TimeSpan.FromSeconds(6),
            IdleTimeout = TimeSpan.FromSeconds(7),
            SweepInterval = TimeSpan.FromSeconds(8),
            PoolReusePolicy = ClickHouseTcpPoolReusePolicy.Fifo,
            LoggerFactory = NullLoggerFactory.Instance,
            IncludeSqlInActivityTags = true,
            StatementMaxLength = 42,

            // Zstd rather than Lz4 so this stays non-default whichever codec the default becomes.
            Compressor = ZstdCompressor.Default,
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
    public void With_ChangingOneProperty_CarriesEveryOtherPropertyAcross()
    {
        var original = new ClickHouseTcpClientOptions
        {
            Host = "with-host",
            Port = 1234,
            Username = "with-user",
            Password = "with-password",
            Database = "with-db",
            QuotaKey = "with-quota",
            CustomSettings = new Dictionary<string, string> { ["max_threads"] = "4" },
            MaxSendBufferBytes = 4096,
            DialTimeout = TimeSpan.FromSeconds(3),
            ReadTimeout = TimeSpan.FromSeconds(4),
            MinPoolSize = 1,
            MaxPoolSize = 7,
            PoolTimeout = TimeSpan.FromSeconds(5),
            MaxConnectionLifetime = TimeSpan.FromSeconds(6),
            IdleTimeout = TimeSpan.FromSeconds(7),
            PoolReusePolicy = ClickHouseTcpPoolReusePolicy.Fifo,
        };

        var derived = original with { Port = 9440 };

        Assert.Multiple(() =>
        {
            Assert.That(derived.Port, Is.EqualTo(9440));

            foreach (PropertyInfo property in typeof(ClickHouseTcpClientOptions).GetProperties())
            {
                if (property.Name == nameof(ClickHouseTcpClientOptions.Port))
                {
                    continue;
                }

                Assert.That(property.GetValue(derived), Is.EqualTo(property.GetValue(original)), $"{property.Name} was not carried across");
            }
        });
    }

    [Test]
    public void ToString_WhenAPasswordIsSet_DoesNotIncludeIt()
    {
        // A record prints every property by default, which would put the plaintext password into any log line that
        // formats the options. ClickHouseTcpClient.Options is public, so a caller can reach this.
        var options = new ClickHouseTcpClientOptions
        {
            Host = "example.invalid",
            Port = 9440,
            Username = "alice",
            Password = "s3cr3t",
            Database = "analytics",
        };

        var text = options.ToString();

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Not.Contain("s3cr3t"));
            Assert.That(text, Does.Not.Contain(nameof(ClickHouseTcpClientOptions.Password)));
            Assert.That(
                text,
                Does.Contain("example.invalid").And.Contain("9440").And.Contain("alice").And.Contain("analytics"),
                "the endpoint, user and database stay, so the text is still useful for diagnostics");
        });
    }

    [Test]
    public void ToString_PortLeftToBeDerived_ShowsThePortAConnectionWouldDial()
    {
        // Printing "Port = " with nothing after it helps nobody diagnose a connection; the resolved port is the
        // one that was actually dialled.
        var text = new ClickHouseTcpClientOptions { Host = "example.invalid", UseTls = true }.ToString();

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("9440"));
            Assert.That(text, Does.Contain($"{nameof(ClickHouseTcpClientOptions.UseTls)} = True"));
        });
    }

    [Test]
    public void Equals_EqualScalarsButDistinctSettingsDictionaries_AreNotEqual()
    {
        // Records give value equality, but IReadOnlyDictionary has no value-equality contract, so the settings are
        // compared by reference. This pins the limitation the type documents: do not key a cache on these options.
        var first = new ClickHouseTcpClientOptions { CustomSettings = new Dictionary<string, string> { ["max_threads"] = "4" } };
        var second = new ClickHouseTcpClientOptions { CustomSettings = new Dictionary<string, string> { ["max_threads"] = "4" } };

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.EqualTo(second));
            Assert.That(first, Is.EqualTo(first with { }), "the clone shares the same dictionary instance, so it does compare equal");
        });
    }

    [Test]
    public void Equals_SettingsAbsentAndEveryScalarEqual_AreEqual()
    {
        Assert.That(new ClickHouseTcpClientOptions { Host = "h", Port = 9440 }, Is.EqualTo(new ClickHouseTcpClientOptions { Host = "h", Port = 9440 }));
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
