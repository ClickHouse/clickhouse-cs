using System;
using System.Data.Common;

namespace ClickHouse.Driver.Tcp.Tests.Client;

[TestFixture]
public class ClickHouseTcpConnectionStringBuilderTests
{
    [Test]
    public void ToOptions_AllKeys_ParsesEachValue()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder(
            "Host=db.example;Port=9440;Username=alice;Password=secret;Database=analytics;QuotaKey=q1;DialTimeout=5;ReadTimeout=60;MaxSendBufferBytes=2048");

        var options = builder.ToOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.Host, Is.EqualTo("db.example"));
            Assert.That(options.Port, Is.EqualTo(9440));
            Assert.That(options.Username, Is.EqualTo("alice"));
            Assert.That(options.Password, Is.EqualTo("secret"));
            Assert.That(options.Database, Is.EqualTo("analytics"));
            Assert.That(options.QuotaKey, Is.EqualTo("q1"));
            Assert.That(options.DialTimeout, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.ReadTimeout, Is.EqualTo(TimeSpan.FromSeconds(60)));
            Assert.That(options.MaxSendBufferBytes, Is.EqualTo(2048));
        });
    }

    [Test]
    public void ToOptions_MissingKeys_AppliesDefaults()
    {
        var options = new ClickHouseTcpConnectionStringBuilder("Host=only-host").ToOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.Host, Is.EqualTo("only-host"));
            Assert.That(options.Port, Is.Null, "an absent Port key derives the port from UseTls");
            Assert.That(options.Username, Is.EqualTo("default"));
            Assert.That(options.Password, Is.EqualTo(string.Empty));
            Assert.That(options.Database, Is.EqualTo("default"));
            Assert.That(options.DialTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(options.UseTls, Is.False);
            Assert.That(options.TlsServerName, Is.Null);
            Assert.That(options.TlsAllowInvalidCertificates, Is.False);
            Assert.That(options.TlsCaCertificatePath, Is.Null);
        });
    }

    [Test]
    public void ToOptions_SetPrefixedKeys_CollectedAsCustomSettings()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder("Host=h;set_max_threads=4;set_max_block_size=1000");

        var options = builder.ToOptions();

        Assert.That(options.CustomSettings, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(options.CustomSettings["max_threads"], Is.EqualTo("4"));
            Assert.That(options.CustomSettings["max_block_size"], Is.EqualTo("1000"));
        });
    }

    [Test]
    public void ToOptions_NoSetPrefixedKeys_LeavesCustomSettingsNull()
    {
        var options = new ClickHouseTcpConnectionStringBuilder("Host=h").ToOptions();

        Assert.That(options.CustomSettings, Is.Null);
    }

    [TestCase("set_")]
    [TestCase("SET_")]
    public void ToOptions_BareSetPrefixKey_ThrowsArgumentException(string bareKey)
    {
        // The prefix alone names an empty setting, which on the wire is the key that terminates the settings list.
        var builder = new ClickHouseTcpConnectionStringBuilder($"Host=h;{bareKey}=1");

        Assert.Throws<ArgumentException>(() => builder.ToOptions());
    }

    [Test]
    public void ToOptions_CustomSettingSetAsTypedValue_ConvertedToString()
    {
        // A setting assigned programmatically as a typed value must reach CustomSettings as its string form, not
        // as an empty value.
        var builder = new ClickHouseTcpConnectionStringBuilder { Host = "h" };
        builder["set_max_threads"] = 4;
        builder["set_ratio"] = 0.5;

        var options = builder.ToOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.CustomSettings["max_threads"], Is.EqualTo("4"));
            Assert.That(options.CustomSettings["ratio"], Is.EqualTo("0.5"));
        });
    }

    [Test]
    [SetCulture("de-DE")]
    public void ToOptions_FractionalValuesUnderCommaDecimalCulture_StayInvariant()
    {
        // de-DE writes 0,5 for 0.5. Values sent to the server must not pick up the ambient culture.
        var builder = new ClickHouseTcpConnectionStringBuilder { Host = "h", DialTimeout = TimeSpan.FromMilliseconds(1500) };
        builder["set_ratio"] = 0.5;

        var options = builder.ToOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.CustomSettings["ratio"], Is.EqualTo("0.5"));
            Assert.That(options.DialTimeout, Is.EqualTo(TimeSpan.FromMilliseconds(1500)));
        });
    }

    [Test]
    public void ToOptions_SetKeyWithEmptyValue_IsDroppedAndYieldsNoSetting()
    {
        // The base builder removes a key whose value is empty, so 'set_some_flag=' never reaches CustomSettings at
        // all. A flag-style setting needs an explicit value, e.g. 'set_some_flag=1'.
        var options = new ClickHouseTcpConnectionStringBuilder("Host=h;set_some_flag=").ToOptions();

        Assert.That(options.CustomSettings, Is.Null);
    }

    [Test]
    public void FromConnectionString_DelegatesToBuilder()
    {
        var options = ClickHouseTcpClientOptions.FromConnectionString("Host=h;Port=1234");

        Assert.Multiple(() =>
        {
            Assert.That(options.Host, Is.EqualTo("h"));
            Assert.That(options.Port, Is.EqualTo(1234));
        });
    }

    [Test]
    public void TypedSetters_ReadBackOnSameInstance_ReturnValuesNotDefaults()
    {
        // The typed setters store boxed int/double; the getters must read those back rather than falling through
        // to defaults (a set-then-get on one builder instance, without going through the connection string).
        var builder = new ClickHouseTcpConnectionStringBuilder
        {
            Port = 9440,
            MaxSendBufferBytes = 2048,
            DialTimeout = TimeSpan.FromSeconds(7),
            ReadTimeout = TimeSpan.FromSeconds(45),
        };

        Assert.Multiple(() =>
        {
            Assert.That(builder.Port, Is.EqualTo(9440));
            Assert.That(builder.MaxSendBufferBytes, Is.EqualTo(2048));
            Assert.That(builder.DialTimeout, Is.EqualTo(TimeSpan.FromSeconds(7)));
            Assert.That(builder.ReadTimeout, Is.EqualTo(TimeSpan.FromSeconds(45)));
        });
    }

    [Test]
    public void TypedTlsSetters_ReadBackOnSameInstance_ReturnValuesNotDefaults()
    {
        // The boolean setters store a boxed bool, which is a different getter arm from the string a parsed
        // connection string leaves behind. Both have to read back, as they already must for the numeric keys.
        var builder = new ClickHouseTcpConnectionStringBuilder
        {
            UseTls = true,
            TlsAllowInvalidCertificates = true,
            TlsServerName = "sni.example",
            TlsCaCertificatePath = "/etc/ca.pem",
        };

        Assert.Multiple(() =>
        {
            Assert.That(builder.UseTls, Is.True);
            Assert.That(builder.TlsAllowInvalidCertificates, Is.True);
            Assert.That(builder.TlsServerName, Is.EqualTo("sni.example"));
            Assert.That(builder.TlsCaCertificatePath, Is.EqualTo("/etc/ca.pem"));
        });
    }

    [Test]
    public void ToOptions_PoolKeys_ParsesEachValue()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder(
            "Host=h;MinPoolSize=2;MaxPoolSize=8;PoolTimeout=15;MaxConnectionLifetime=600;IdleTimeout=120;SweepInterval=5;PoolReusePolicy=Fifo");

        var options = builder.ToOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.MinPoolSize, Is.EqualTo(2));
            Assert.That(options.MaxPoolSize, Is.EqualTo(8));
            Assert.That(options.PoolTimeout, Is.EqualTo(TimeSpan.FromSeconds(15)));
            Assert.That(options.MaxConnectionLifetime, Is.EqualTo(TimeSpan.FromMinutes(10)));
            Assert.That(options.IdleTimeout, Is.EqualTo(TimeSpan.FromMinutes(2)));
            Assert.That(options.SweepInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.PoolReusePolicy, Is.EqualTo(ClickHouseTcpPoolReusePolicy.Fifo));
        });
    }

    [Test]
    public void ToOptions_MissingPoolKeys_AppliesDefaults()
    {
        var options = new ClickHouseTcpConnectionStringBuilder("Host=h").ToOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.MinPoolSize, Is.Zero);
            Assert.That(options.MaxPoolSize, Is.EqualTo(20));
            Assert.That(options.PoolTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(options.MaxConnectionLifetime, Is.EqualTo(TimeSpan.FromMinutes(30)));
            Assert.That(options.IdleTimeout, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(options.SweepInterval, Is.Null, "an absent key means the period is derived");
            Assert.That(options.PoolReusePolicy, Is.EqualTo(ClickHouseTcpPoolReusePolicy.Lifo));
        });
    }

    [Test]
    public void ToOptions_PoolReusePolicyInAnyCase_IsAccepted()
    {
        var options = new ClickHouseTcpConnectionStringBuilder("Host=h;PoolReusePolicy=fifo").ToOptions();

        Assert.That(options.PoolReusePolicy, Is.EqualTo(ClickHouseTcpPoolReusePolicy.Fifo));
    }

    [Test]
    public void ToOptions_UnknownPoolReusePolicy_ThrowsNamingTheAcceptedValues()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder("Host=h;PoolReusePolicy=Random");

        var thrown = Assert.Throws<ArgumentException>(() => builder.ToOptions());

        Assert.That(thrown.Message, Does.Contain("Lifo").And.Contains("Fifo"));
    }

    [Test]
    public void PoolTypedSetters_ReadBackOnSameInstance_ReturnValuesNotDefaults()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder
        {
            MinPoolSize = 3,
            MaxPoolSize = 9,
            PoolTimeout = TimeSpan.FromSeconds(11),
            MaxConnectionLifetime = TimeSpan.FromMinutes(20),
            IdleTimeout = TimeSpan.FromMinutes(3),
            SweepInterval = TimeSpan.FromSeconds(4),
            PoolReusePolicy = ClickHouseTcpPoolReusePolicy.Fifo,
        };

        Assert.Multiple(() =>
        {
            Assert.That(builder.MinPoolSize, Is.EqualTo(3));
            Assert.That(builder.MaxPoolSize, Is.EqualTo(9));
            Assert.That(builder.PoolTimeout, Is.EqualTo(TimeSpan.FromSeconds(11)));
            Assert.That(builder.MaxConnectionLifetime, Is.EqualTo(TimeSpan.FromMinutes(20)));
            Assert.That(builder.IdleTimeout, Is.EqualTo(TimeSpan.FromMinutes(3)));
            Assert.That(builder.SweepInterval, Is.EqualTo(TimeSpan.FromSeconds(4)));
            Assert.That(builder.PoolReusePolicy, Is.EqualTo(ClickHouseTcpPoolReusePolicy.Fifo));
        });
    }

    [Test]
    public void SweepInterval_SetToNull_RemovesTheKeyRatherThanStoringAValue()
    {
        // Null is how a caller goes back to deriving the period, so it must leave no key behind: a stored zero
        // would fail validation, and any stored number would keep overriding the derivation.
        var builder = new ClickHouseTcpConnectionStringBuilder { SweepInterval = TimeSpan.FromSeconds(4) };

        builder.SweepInterval = null;

        Assert.Multiple(() =>
        {
            Assert.That(builder.SweepInterval, Is.Null);
            Assert.That(builder.ConnectionString, Does.Not.Contain("SweepInterval").IgnoreCase);
            Assert.That(builder.ToOptions().SweepInterval, Is.Null);
        });
    }

    [Test]
    public void SweepInterval_Unparseable_ReadsAsAbsentSoThePeriodIsDerived()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder("Host=h;SweepInterval=soon");

        Assert.That(builder.SweepInterval, Is.Null);
    }

    [Test]
    public void Setters_RoundTripThroughConnectionString()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder
        {
            Host = "rt-host",
            Port = 9001,
            Username = "u",
            Database = "d",
        };

        var reparsed = new ClickHouseTcpConnectionStringBuilder(builder.ConnectionString).ToOptions();

        Assert.Multiple(() =>
        {
            Assert.That(reparsed.Host, Is.EqualTo("rt-host"));
            Assert.That(reparsed.Port, Is.EqualTo(9001));
            Assert.That(reparsed.Username, Is.EqualTo("u"));
            Assert.That(reparsed.Database, Is.EqualTo("d"));
        });
    }

    [Test]
    public void ToOptions_TlsKeys_ParsesEachValue()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder(
            "Host=db.example;UseTls=true;TlsServerName=cert.example;TlsAllowInvalidCertificates=true;TlsCaCertificatePath=/etc/ca.pem");

        var options = builder.ToOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.UseTls, Is.True);
            Assert.That(options.TlsServerName, Is.EqualTo("cert.example"));
            Assert.That(options.TlsAllowInvalidCertificates, Is.True);
            Assert.That(options.TlsCaCertificatePath, Is.EqualTo("/etc/ca.pem"));
        });
    }

    [Test]
    public void ToOptions_UseTlsWithNoPortKey_ResolvesToTheSecureNativePort()
    {
        var options = ClickHouseTcpClientOptions.FromConnectionString("Host=db.example;UseTls=true");

        Assert.Multiple(() =>
        {
            Assert.That(options.Port, Is.Null, "no key was given, so nothing is stored");
            Assert.That(options.ResolvedPort, Is.EqualTo(9440));
        });
    }

    [Test]
    public void ToOptions_UseTlsWithAnExplicitPort_KeepsThatPort()
    {
        // A server may serve secure native connections on a non-standard port; an explicit key always wins.
        var options = ClickHouseTcpClientOptions.FromConnectionString("Host=db.example;UseTls=true;Port=9000");

        Assert.That(options.ResolvedPort, Is.EqualTo(9000));
    }

    [TestCase("true", true)]
    [TestCase("True", true)]
    [TestCase("TRUE", true)]
    [TestCase("1", true)]
    [TestCase("false", false)]
    [TestCase("0", false)]
    public void UseTls_RecognizedBooleanSpelling_Parses(string value, bool expected)
    {
        var options = ClickHouseTcpClientOptions.FromConnectionString($"Host=h;UseTls={value}");

        Assert.That(options.UseTls, Is.EqualTo(expected));
    }

    [TestCase("Host=h;UseTls=")]
    [TestCase("Host=h;UseTls=   ")]
    [TestCase("Host=h;UseTls=\"\"")]
    [TestCase("Host=h;UseTls=\"   \"")]
    [TestCase("Host=h;UseTls=true;UseTls=")]
    public void Constructor_UseTlsWithAnEmptyValue_ThrowsRatherThanSelectingPlaintext(string connectionString)
    {
        Assert.Throws<ArgumentException>(() => new ClickHouseTcpConnectionStringBuilder(connectionString));
    }

    [Test]
    public void ConnectionString_SetThroughBaseTypeWithAnEmptyUseTls_ThrowsRatherThanSelectingPlaintext()
    {
        DbConnectionStringBuilder builder = new ClickHouseTcpConnectionStringBuilder();

        Assert.Throws<ArgumentException>(() => builder.ConnectionString = "Host=h;UseTls=");
    }

    [Test]
    public void FromConnectionString_EmptyUseTls_ThrowsRatherThanSelectingPlaintext()
    {
        Assert.Throws<ArgumentException>(() => ClickHouseTcpClientOptions.FromConnectionString("Host=h;UseTls="));
    }

    [Test]
    public void ClickHouseTcpClient_EmptyUseTls_ThrowsAtConstructionRatherThanSelectingPlaintext()
    {
        Assert.Throws<ArgumentException>(() => new ClickHouseTcpClient("Host=h;UseTls="));
    }

    [TestCase("yes")]
    [TestCase("on")]
    [TestCase("2")]
    public void UseTls_UnrecognizedValue_ThrowsRatherThanReadingAsFalse(string value)
    {
        // Falling back to the default here would hand back a plaintext connection the caller believes is
        // encrypted, so an unparseable value must be loud.
        var builder = new ClickHouseTcpConnectionStringBuilder($"Host=h;UseTls={value}");

        Assert.Throws<ArgumentException>(() => builder.ToOptions());
    }

    [Test]
    public void TlsAllowInvalidCertificates_UnrecognizedValue_ThrowsRatherThanReadingAsFalse()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder("Host=h;UseTls=true;TlsAllowInvalidCertificates=maybe");

        Assert.Throws<ArgumentException>(() => builder.ToOptions());
    }

    [TestCase(1, true)]
    [TestCase(0, false)]
    [TestCase(true, true)]
    [TestCase(false, false)]
    public void UseTls_SetThroughTheUntypedIndexer_ReadsAsTheValueGiven(object value, bool expected)
    {
        // The untyped indexer is a supported path on this class, and the base converts whatever it is handed to a
        // string — so an int or a bool set this way must still read as the boolean it means, never fall back to
        // false, which would connect in the clear while the caller believes otherwise.
        var builder = new ClickHouseTcpConnectionStringBuilder { Host = "h" };
        builder["UseTls"] = value;

        Assert.That(builder.ToOptions().UseTls, Is.EqualTo(expected));
    }

    [Test]
    public void TlsSetters_RoundTripThroughConnectionString()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder
        {
            Host = "rt-host",
            UseTls = true,
            TlsServerName = "rt-sni",
            TlsAllowInvalidCertificates = true,
            TlsCaCertificatePath = "/tmp/rt-ca.pem",
        };

        var reparsed = new ClickHouseTcpConnectionStringBuilder(builder.ConnectionString).ToOptions();

        Assert.Multiple(() =>
        {
            Assert.That(reparsed.UseTls, Is.True);
            Assert.That(reparsed.TlsServerName, Is.EqualTo("rt-sni"));
            Assert.That(reparsed.TlsAllowInvalidCertificates, Is.True);
            Assert.That(reparsed.TlsCaCertificatePath, Is.EqualTo("/tmp/rt-ca.pem"));
        });
    }

    [TestCase("Host=h;UseTls=true;TlsCaCertificatePath=")]
    [TestCase("Host=h;UseTls=true;TlsCaCertificatePath=   ")]
    [TestCase("Host=h;UseTls=true;TlsCaCertificatePath=\"\"")]
    [TestCase("Host=h;UseTls=true;TlsCaCertificatePath=\"   \"")]
    [TestCase("Host=h;UseTls=true;TlsCaCertificatePath=/tmp/ca.pem;TlsCaCertificatePath=")]
    public void Constructor_TlsCaCertificatePathWithAnEmptyValue_ThrowsRatherThanUsingHostTrust(string connectionString)
    {
        Assert.Throws<ArgumentException>(() => new ClickHouseTcpConnectionStringBuilder(connectionString));
    }

    [Test]
    public void TlsCaCertificatePath_SetToNull_RemovesTheKeyAndUsesHostTrust()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder
        {
            Host = "h",
            UseTls = true,
            TlsCaCertificatePath = "/tmp/ca.pem",
        };

        builder.TlsCaCertificatePath = null;

        Assert.Multiple(() =>
        {
            Assert.That(builder.TlsCaCertificatePath, Is.Null);
            Assert.That(builder.ConnectionString, Does.Not.Contain(nameof(builder.TlsCaCertificatePath)).IgnoreCase);
            Assert.That(builder.ToOptions().TlsCaCertificatePath, Is.Null);
        });
    }

    [Test]
    public void Clear_BuilderHoldingSecurityKeys_RemovesThemWithoutTreatingThemAsEmptyValues()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder
        {
            Host = "h",
            UseTls = true,
            TlsCaCertificatePath = "/tmp/ca.pem",
        };

        Assert.DoesNotThrow(builder.Clear);
        Assert.That(builder.ConnectionString, Is.Empty);
    }

    [Test]
    public void Port_SetToNull_RemovesTheKeySoThePortIsDerivedAgain()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder { Host = "h", Port = 9001, UseTls = true };

        builder.Port = null;

        Assert.Multiple(() =>
        {
            Assert.That(builder.Port, Is.Null);
            Assert.That(builder.ToOptions().ResolvedPort, Is.EqualTo(9440));
        });
    }
}
