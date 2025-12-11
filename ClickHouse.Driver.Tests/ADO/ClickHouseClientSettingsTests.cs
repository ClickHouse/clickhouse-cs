using System;
using System.Collections.Generic;
using System.Net.Http;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

[TestFixture]
public class ClickHouseClientSettingsTests
{
    [Test]
    public void Constructor_ShouldInitializeWithDefaults()
    {
        var settings = new ClickHouseClientSettings();

        Assert.Multiple(() =>
        {
            Assert.That(settings.Host, Is.EqualTo(ClickHouseDefaults.Host));
            Assert.That(settings.Port, Is.EqualTo(ClickHouseDefaults.HttpPort));
            Assert.That(settings.Protocol, Is.EqualTo(ClickHouseDefaults.Protocol));
            Assert.That(settings.Database, Is.EqualTo(ClickHouseDefaults.Database));
            Assert.That(settings.Path, Is.EqualTo(ClickHouseDefaults.Path));
            Assert.That(settings.Username, Is.EqualTo(ClickHouseDefaults.Username));
            Assert.That(settings.Password, Is.EqualTo(ClickHouseDefaults.Password));
            Assert.That(settings.UseCompression, Is.EqualTo(ClickHouseDefaults.Compression));
            Assert.That(settings.UseServerTimezone, Is.EqualTo(ClickHouseDefaults.UseServerTimezone));
            Assert.That(settings.UseCustomDecimals, Is.EqualTo(ClickHouseDefaults.UseCustomDecimals));
            Assert.That(settings.UseSession, Is.EqualTo(ClickHouseDefaults.UseSession));
            Assert.That(settings.SessionId, Is.Null);
            Assert.That(settings.SkipServerCertificateValidation, Is.EqualTo(ClickHouseDefaults.SkipServerCertificateValidation));
            Assert.That(settings.UseFormDataParameters, Is.EqualTo(ClickHouseDefaults.UseFormDataParameters));
            Assert.That(settings.Timeout, Is.EqualTo(ClickHouseDefaults.Timeout));
            Assert.That(settings.HttpClient, Is.Null);
            Assert.That(settings.HttpClientFactory, Is.Null);
            Assert.That(settings.HttpClientName, Is.Null);
            Assert.That(settings.LoggerFactory, Is.Null);
            Assert.That(settings.CustomSettings, Is.Not.Null);
            Assert.That(settings.CustomSettings, Is.Empty);
        });
    }

    [Test]
    public void Constructor_ShouldInitializeEmptyCustomSettingsDictionary()
    {
        var settings = new ClickHouseClientSettings();

        Assert.That(settings.CustomSettings, Is.Not.Null);
        Assert.That(settings.CustomSettings, Is.Empty);
    }

    [Test]
    public void CustomSettings_ShouldBeSettable()
    {
        var customSettings = new Dictionary<string, object>
        {
            { "max_threads", 4 },
            { "readonly", 1 }
        };

        var settings = new ClickHouseClientSettings
        {
            CustomSettings = customSettings
        };

        Assert.That(settings.CustomSettings, Is.SameAs(customSettings));
        Assert.That(settings.CustomSettings["max_threads"], Is.EqualTo(4));
        Assert.That(settings.CustomSettings["readonly"], Is.EqualTo(1));
    }

    [Test]
    public void FromConnectionString_WithEmptyString_ShouldUseDefaults()
    {
        var settings = ClickHouseClientSettings.FromConnectionString("");

        Assert.Multiple(() =>
        {
            Assert.That(settings.Host, Is.EqualTo(ClickHouseDefaults.Host));
            Assert.That(settings.Port, Is.EqualTo(ClickHouseDefaults.HttpPort));
            Assert.That(settings.Protocol, Is.EqualTo(ClickHouseDefaults.Protocol));
            Assert.That(settings.Database, Is.EqualTo(ClickHouseDefaults.Database));
            Assert.That(settings.Username, Is.EqualTo(ClickHouseDefaults.Username));
            Assert.That(settings.Password, Is.EqualTo(ClickHouseDefaults.Password));
        });
    }

    [Test]
    public void FromConnectionString_WithFullConnectionString_ShouldParseAllValues()
    {
        var connectionString = "Host=myhost;Port=9000;Protocol=https;Database=mydb;" +
                              "Username=myuser;Password=mypass;Path=/custom;" +
                              "Compression=false;UseSession=true;SessionId=session123;" +
                              "Timeout=300;UseServerTimezone=false;UseCustomDecimals=false";

        var settings = ClickHouseClientSettings.FromConnectionString(connectionString);

        Assert.Multiple(() =>
        {
            Assert.That(settings.Host, Is.EqualTo("myhost"));
            Assert.That(settings.Port, Is.EqualTo(9000));
            Assert.That(settings.Protocol, Is.EqualTo("https"));
            Assert.That(settings.Database, Is.EqualTo("mydb"));
            Assert.That(settings.Username, Is.EqualTo("myuser"));
            Assert.That(settings.Password, Is.EqualTo("mypass"));
            Assert.That(settings.Path, Is.EqualTo("/custom"));
            Assert.That(settings.UseCompression, Is.False);
            Assert.That(settings.UseSession, Is.True);
            Assert.That(settings.SessionId, Is.EqualTo("session123"));
            Assert.That(settings.Timeout, Is.EqualTo(TimeSpan.FromSeconds(300)));
            Assert.That(settings.UseServerTimezone, Is.False);
            Assert.That(settings.UseCustomDecimals, Is.False);
        });
    }

    [Test]
    public void FromConnectionString_WithPartialConnectionString_ShouldParseSpecifiedValuesAndUseDefaultsForRest()
    {
        var connectionString = "Host=myhost;Database=mydb;Username=myuser";

        var settings = ClickHouseClientSettings.FromConnectionString(connectionString);

        Assert.Multiple(() =>
        {
            Assert.That(settings.Host, Is.EqualTo("myhost"));
            Assert.That(settings.Database, Is.EqualTo("mydb"));
            Assert.That(settings.Username, Is.EqualTo("myuser"));
            Assert.That(settings.Port, Is.EqualTo(ClickHouseDefaults.HttpPort));
            Assert.That(settings.Protocol, Is.EqualTo(ClickHouseDefaults.Protocol));
            Assert.That(settings.Password, Is.EqualTo(ClickHouseDefaults.Password));
        });
    }

    [Test]
    public void FromConnectionString_WithCustomSettings_ShouldParseCustomSettings()
    {
        var connectionString = "Host=myhost;set_max_threads=4;set_readonly=1;set_max_memory_usage=10000000000";

        var settings = ClickHouseClientSettings.FromConnectionString(connectionString);

        Assert.Multiple(() =>
        {
            Assert.That(settings.Host, Is.EqualTo("myhost"));
            Assert.That(settings.CustomSettings, Is.Not.Null);
            Assert.That(settings.CustomSettings.Count, Is.EqualTo(3));
            Assert.That(settings.CustomSettings["max_threads"], Is.EqualTo("4"));
            Assert.That(settings.CustomSettings["readonly"], Is.EqualTo("1"));
            Assert.That(settings.CustomSettings["max_memory_usage"], Is.EqualTo("10000000000"));
        });
    }

    [Test]
    public void FromConnectionString_WithHttpsProtocol_ShouldUseHttpsPort()
    {
        var connectionString = "Protocol=https";

        var settings = ClickHouseClientSettings.FromConnectionString(connectionString);

        Assert.Multiple(() =>
        {
            Assert.That(settings.Protocol, Is.EqualTo("https"));
            Assert.That(settings.Port, Is.EqualTo(ClickHouseDefaults.HttpsPort));
        });
    }

    [Test]
    public void Validate_WithValidSettings_ShouldNotThrow()
    {
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Port = 8123,
            Protocol = "http"
        };

        Assert.DoesNotThrow(() => settings.Validate());
    }

    [Test]
    [TestCase("")]
    [TestCase(null)]
    [TestCase("   ")]
    public void Validate_BadHost_ShouldThrow(string host)
    {
        var settings = new ClickHouseClientSettings
        {
            Host = host
        };

        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate());
        Assert.That(ex.Message, Does.Contain("Host cannot be null or whitespace"));
    }

    [Test]
    public void Validate_WithPortZero_ShouldThrow()
    {
        var settings = new ClickHouseClientSettings
        {
            Port = 0
        };

        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate());
        Assert.That(ex.Message, Does.Contain("Port must be between 1 and 65535"));
    }

    [Test]
    public void Validate_WithNullProtocol_ShouldThrow()
    {
        var settings = new ClickHouseClientSettings
        {
            Protocol = null
        };

        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate());
        Assert.That(ex.Message, Does.Contain("Protocol cannot be null or whitespace"));
    }

    [Test]
    public void Validate_WithEmptyProtocol_ShouldThrow()
    {
        var settings = new ClickHouseClientSettings
        {
            Protocol = ""
        };

        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate());
        Assert.That(ex.Message, Does.Contain("Protocol cannot be null or whitespace"));
    }

    [Test]
    public void Validate_WithInvalidProtocol_ShouldThrow()
    {
        var settings = new ClickHouseClientSettings
        {
            Protocol = "ftp"
        };

        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate());
        Assert.That(ex.Message, Does.Contain("Protocol must be 'http' or 'https'"));
    }

    [Test]
    public void Validate_WithNegativeTimeout_ShouldThrow()
    {
        var settings = new ClickHouseClientSettings
        {
            Timeout = TimeSpan.FromSeconds(-1)
        };

        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate());
        Assert.That(ex.Message, Does.Contain("Timeout cannot be negative"));
    }

    [Test]
    public void Validate_WithBothHttpClientAndFactory_ShouldThrow()
    {
        var settings = new ClickHouseClientSettings
        {
            HttpClient = new HttpClient(),
            HttpClientFactory = new TestHttpClientFactory()
        };

        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate());
        Assert.That(ex.Message, Does.Contain("Cannot specify both HttpClient and HttpClientFactory"));
    }

    [Test]
    public void Validate_WithEnableDebugModeButNoLoggerFactory_ShouldThrow()
    {
        var settings = new ClickHouseClientSettings
        {
            EnableDebugMode = true,
            LoggerFactory = null
        };

        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate());
        Assert.That(ex.Message, Does.Contain("LoggerFactory must be provided when EnableDebugMode is true"));
    }

    [Test]
    public void Equals_WithSameValues_ShouldReturnTrue()
    {
        var settings1 = new ClickHouseClientSettings
        {
            Host = "myhost",
            Port = 9000,
            Database = "mydb"
        };

        var settings2 = new ClickHouseClientSettings
        {
            Host = "myhost",
            Port = 9000,
            Database = "mydb"
        };

        Assert.That(settings1.Equals(settings2), Is.True);
        Assert.That(settings1 == settings2, Is.True);
        Assert.That(settings1 != settings2, Is.False);
    }

    [Test]
    public void Equals_WithDifferentValues_ShouldReturnFalse()
    {
        var settings1 = new ClickHouseClientSettings
        {
            Host = "myhost"
        };

        var settings2 = new ClickHouseClientSettings
        {
            Host = "otherhost"
        };

        Assert.That(settings1.Equals(settings2), Is.False);
        Assert.That(settings1 == settings2, Is.False);
        Assert.That(settings1 != settings2, Is.True);
    }

    [Test]
    public void Equals_WithSameReference_ShouldReturnTrue()
    {
        var settings = new ClickHouseClientSettings();

        Assert.That(settings.Equals(settings), Is.True);
        Assert.That(settings == settings, Is.True);
    }

    [Test]
    public void Equals_WithNull_ShouldReturnFalse()
    {
        var settings = new ClickHouseClientSettings();

        Assert.That(settings.Equals(null), Is.False);
        Assert.That(settings == null, Is.False);
        Assert.That(settings != null, Is.True);
    }

    [Test]
    public void Equals_WithNullOnBothSides_ShouldReturnTrue()
    {
        ClickHouseClientSettings settings1 = null;
        ClickHouseClientSettings settings2 = null;

        Assert.That(settings1 == settings2, Is.True);
        Assert.That(settings1 != settings2, Is.False);
    }

    [Test]
    public void Equals_WithDifferentType_ShouldReturnFalse()
    {
        var settings = new ClickHouseClientSettings();
        var other = new object();

        Assert.That(settings.Equals(other), Is.False);
    }

    [Test]
    public void GetHashCode_WithSameValues_ShouldReturnSameHashCode()
    {
        var settings1 = new ClickHouseClientSettings
        {
            Host = "myhost",
            Port = 9000,
            Database = "mydb"
        };

        var settings2 = new ClickHouseClientSettings
        {
            Host = "myhost",
            Port = 9000,
            Database = "mydb"
        };

        Assert.That(settings1.GetHashCode(), Is.EqualTo(settings2.GetHashCode()));
    }

    [Test]
    public void GetHashCode_WithDifferentValues_ShouldReturnDifferentHashCode()
    {
        var settings1 = new ClickHouseClientSettings
        {
            Host = "myhost"
        };

        var settings2 = new ClickHouseClientSettings
        {
            Host = "otherhost"
        };

        Assert.That(settings1.GetHashCode(), Is.Not.EqualTo(settings2.GetHashCode()));
    }

    [Test]
    public void GetHashCode_IncludesCustomSettings()
    {
        var settings1 = new ClickHouseClientSettings();
        settings1.CustomSettings["max_threads"] = 4;

        var settings2 = new ClickHouseClientSettings();
        settings2.CustomSettings["max_threads"] = 8;

        Assert.That(settings1.GetHashCode(), Is.Not.EqualTo(settings2.GetHashCode()));
    }

    [Test]
    public void ToString_ShouldRedactPassword()
    {
        var settings = new ClickHouseClientSettings
        {
            Password = "secretpassword"
        };

        var str = settings.ToString();

        Assert.Multiple(() =>
        {
            Assert.That(str, Does.Contain("Password=****"));
            Assert.That(str, Does.Not.Contain("secretpassword"));
        });
    }

    [Test]
    public void ToConnectionStringBuilder_AndBack_ShouldPreserveAllValues()
    {
        var originalSettings = new ClickHouseClientSettings
        {
            Host = "myhost",
            Port = 9000,
            Protocol = "https",
            Database = "mydb",
            Username = "myuser",
            Password = "mypass",
            Path = "/custom",
            UseCompression = false,
            UseSession = true,
            SessionId = "session123",
            Timeout = TimeSpan.FromMinutes(5),
            UseServerTimezone = false,
            UseCustomDecimals = false
        };

        var builder = ClickHouseConnectionStringBuilder.FromSettings(originalSettings);
        var roundTrippedSettings = builder.ToSettings();

        Assert.Multiple(() =>
        {
            Assert.That(roundTrippedSettings.Host, Is.EqualTo(originalSettings.Host));
            Assert.That(roundTrippedSettings.Port, Is.EqualTo(originalSettings.Port));
            Assert.That(roundTrippedSettings.Protocol, Is.EqualTo(originalSettings.Protocol));
            Assert.That(roundTrippedSettings.Database, Is.EqualTo(originalSettings.Database));
            Assert.That(roundTrippedSettings.Username, Is.EqualTo(originalSettings.Username));
            Assert.That(roundTrippedSettings.Password, Is.EqualTo(originalSettings.Password));
            Assert.That(roundTrippedSettings.Path, Is.EqualTo(originalSettings.Path));
            Assert.That(roundTrippedSettings.UseCompression, Is.EqualTo(originalSettings.UseCompression));
            Assert.That(roundTrippedSettings.UseSession, Is.EqualTo(originalSettings.UseSession));
            Assert.That(roundTrippedSettings.SessionId, Is.EqualTo(originalSettings.SessionId));
            Assert.That(roundTrippedSettings.Timeout, Is.EqualTo(originalSettings.Timeout));
            Assert.That(roundTrippedSettings.UseServerTimezone, Is.EqualTo(originalSettings.UseServerTimezone));
            Assert.That(roundTrippedSettings.UseCustomDecimals, Is.EqualTo(originalSettings.UseCustomDecimals));
        });
    }

    [Test]
    public void FromConnectionString_ToBuilder_AndBack_ShouldPreserveValues()
    {
        var connectionString = "Host=myhost;Port=9000;Database=mydb;";

        var settings = ClickHouseClientSettings.FromConnectionString(connectionString);
        var builder = ClickHouseConnectionStringBuilder.FromSettings(settings);
        var finalSettings = builder.ToSettings();

        Assert.Multiple(() =>
        {
            Assert.That(finalSettings.Host, Is.EqualTo("myhost"));
            Assert.That(finalSettings.Port, Is.EqualTo(9000));
            Assert.That(finalSettings.Database, Is.EqualTo("mydb"));
        });
    }

    [Test]
    public void FromSettings_WithNullSettings_ShouldThrowArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            ClickHouseConnectionStringBuilder.FromSettings(null));
    }
    
    [Test]
    public void SessionId_WhenExplicitlySet_ShouldReturnSetValue()
    {
        var settings = new ClickHouseClientSettings
        {
            UseSession = false,
            SessionId = "explicit-session"
        };

        var settings2 = new ClickHouseClientSettings
        {
            UseSession = true,
            SessionId = "my-custom-session"
        };
        
        Assert.That(settings.SessionId, Is.EqualTo("explicit-session"));
        Assert.That(settings2.SessionId, Is.EqualTo("my-custom-session"));
    }

    [Test]
    public void SessionId_WhenUseSessionIsTrueAndNotSet_ShouldGenerateGuid()
    {
        var settings = new ClickHouseClientSettings
        {
            UseSession = true
        };

        var sessionId = settings.SessionId;

        Assert.That(sessionId, Is.Not.Null);
        Assert.That(sessionId, Is.Not.Empty);
        Assert.DoesNotThrow(() => Guid.Parse(sessionId));
    }

    [Test]
    public void SessionId_WhenUseSessionIsTrueAndNotSet_ShouldReturnSameGuidOnMultipleCalls()
    {
        var settings = new ClickHouseClientSettings
        {
            UseSession = true
        };

        var sessionId1 = settings.SessionId;
        var sessionId2 = settings.SessionId;

        Assert.That(sessionId1, Is.EqualTo(sessionId2));
    }

    [Test]
    public void SessionId_CopyConstructor_ShouldCopyGeneratedGuid()
    {
        var original = new ClickHouseClientSettings
        {
            UseSession = true
        };

        // Trigger GUID generation
        var originalSessionId = original.SessionId;

        var copy = new ClickHouseClientSettings(original);

        Assert.That(copy.SessionId, Is.EqualTo(originalSessionId));
    }

    [Test]
    public void Constructor_WithNullSettings_ShouldThrowArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new ClickHouseClientSettings((string)null));
        Assert.Throws<ArgumentNullException>(() => new ClickHouseClientSettings((ClickHouseClientSettings)null));
        Assert.Throws<ArgumentNullException>(() => new ClickHouseClientSettings((ClickHouseConnectionStringBuilder)null));
    }

    [Test]
    public void CopyConstructor_ShouldDeepCopyCustomSettings()
    {
        var original = new ClickHouseClientSettings();
        original.CustomSettings["key1"] = "value1";

        var copy = new ClickHouseClientSettings(original);

        // Modify the copy's CustomSettings
        copy.CustomSettings["key2"] = "value2";

        // Verify original is not affected
        Assert.Multiple(() =>
        {
            Assert.That(original.CustomSettings.Count, Is.EqualTo(1));
            Assert.That(original.CustomSettings.ContainsKey("key2"), Is.False);
            Assert.That(copy.CustomSettings.Count, Is.EqualTo(2));
        });
    }

    [Test]
    public void CopyConstructor_AllPropertiesAreCopied_UsingReflection()
    {
        // This test uses reflection to ensure that if new properties are added in the future,
        // they are included in the copy constructor

        var httpClient = new HttpClient();
        var factory = new TestHttpClientFactory();
        var logger = new TestLoggerFactory();

        var original = new ClickHouseClientSettings
        {
            Host = "testhost",
            Port = 9000,
            Protocol = "https",
            Database = "testdb",
            Path = "/custom",
            Username = "testuser",
            Password = "testpass",
            UseCompression = false,
            UseServerTimezone = false,
            UseCustomDecimals = false,
            UseSession = true,
            SessionId = "session123",
            SkipServerCertificateValidation = true,
            UseFormDataParameters = true,
            Timeout = TimeSpan.FromMinutes(5),
            HttpClient = httpClient,
            HttpClientFactory = factory,
            HttpClientName = "test-client",
            LoggerFactory = logger
        };

        var copy = new ClickHouseClientSettings(original);

        // Get all public instance properties with getters
        var properties = typeof(ClickHouseClientSettings).GetProperties(
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);

        // Check each property
        foreach (var property in properties)
        {
            if (!property.CanRead)
                continue;

            var originalValue = property.GetValue(original);
            var copyValue = property.GetValue(copy);

            if (property.Name == nameof(ClickHouseClientSettings.CustomSettings))
            {
                // CustomSettings should be a different instance (deep copy)
                Assert.That(copyValue, Is.Not.SameAs(originalValue),
                    $"Property {property.Name} should be deep copied");

                var originalDict = originalValue as IDictionary<string, object>;
                var copyDict = copyValue as IDictionary<string, object>;
                Assert.That(copyDict, Is.EquivalentTo(originalDict),
                    $"Dictionary values should be copied");
            }
            else if (property.Name == nameof(ClickHouseClientSettings.CustomHeaders))
            {
                var originalDict = originalValue as IDictionary<string, string>;
                var copyDict = copyValue as IDictionary<string, string>;
                Assert.That(copyDict, Is.EquivalentTo(originalDict), "Custom headers should be copied");
            }
            else if (property.PropertyType.IsValueType || property.PropertyType == typeof(string))
            {
                // Value types and strings should be equal
                Assert.That(copyValue, Is.EqualTo(originalValue),
                    $"Property {property.Name} was not copied correctly");
            }
            else
            {
                // Reference types (HttpClient, Logger, etc.) should be the same reference
                Assert.That(copyValue, Is.SameAs(originalValue),
                    $"Property {property.Name} should reference the same object");
            }
        }
    }
    
    [Test]
    public void Settings_Roles_ShouldBeEmptyByDefault()
    {
        var settings = new ClickHouseClientSettings();

        Assert.That(settings.Roles, Is.Not.Null);
        Assert.That(settings.Roles, Is.Empty);
    }

    [Test]
    public void Settings_Roles_ShouldBeSettable()
    {
        var roles = new[] { "admin", "reader" };
        var settings = new ClickHouseClientSettings { Roles = roles };

        Assert.That(settings.Roles, Has.Count.EqualTo(2));
        Assert.That(settings.Roles, Contains.Item("admin"));
        Assert.That(settings.Roles, Contains.Item("reader"));
    }

    [Test]
    public void Settings_CopyConstructor_ShouldCopyRoles()
    {
        var original = new ClickHouseClientSettings { Roles = new[] { "admin", "writer" } };
        var copy = new ClickHouseClientSettings(original);

        Assert.That(copy.Roles, Has.Count.EqualTo(2));
        Assert.That(copy.Roles, Contains.Item("admin"));
        Assert.That(copy.Roles, Contains.Item("writer"));
        // Verify it's a copy, not the same reference
        Assert.That(copy.Roles, Is.Not.SameAs(original.Roles));
    }

    [Test]
    public void Settings_Equals_ShouldReturnTrue_WhenRolesMatch()
    {
        var settings1 = new ClickHouseClientSettings { Roles = new[] { "admin", "reader" } };
        var settings2 = new ClickHouseClientSettings { Roles = new[] { "admin", "reader" } };

        Assert.That(settings1.Equals(settings2), Is.True);
    }

    [Test]
    public void Settings_Equals_ShouldReturnFalse_WhenRolesDiffer()
    {
        var settings1 = new ClickHouseClientSettings { Roles = new[] { "admin" } };
        var settings2 = new ClickHouseClientSettings { Roles = new[] { "reader" } };

        Assert.That(settings1.Equals(settings2), Is.False);
    }

    [Test]
    public void Settings_GetHashCode_ShouldIncludeRoles()
    {
        var settings1 = new ClickHouseClientSettings { Roles = new[] { "admin" } };
        var settings2 = new ClickHouseClientSettings { Roles = new[] { "reader" } };

        Assert.That(settings1.GetHashCode(), Is.Not.EqualTo(settings2.GetHashCode()));
    }

    [Test]
    public void Settings_ToString_ShouldIncludeRoles()
    {
        var settings = new ClickHouseClientSettings { Roles = new[] { "admin", "reader" } };
        var str = settings.ToString();

        Assert.That(str, Does.Contain("Roles=admin,reader"));
    }

    [Test]
    public void Settings_ToString_ShouldNotIncludeRoles_WhenEmpty()
    {
        var settings = new ClickHouseClientSettings();
        var str = settings.ToString();

        Assert.That(str, Does.Not.Contain("Roles="));
    }

    [Test]
    public void Settings_ConstructorFromConnectionString_ShouldParseSingleRole()
    {
        var settings = new ClickHouseClientSettings("Host=localhost;Roles=admin");
        Assert.That(settings.Roles, Contains.Item("admin"));
    }

    [Test]
    public void Settings_ConstructorFromConnectionString_ShouldParseMultipleRoles()
    {
        var settings = new ClickHouseClientSettings("Host=localhost;Roles=admin,janitor");
        Assert.That(settings.Roles.Count, Is.EqualTo(2));
        Assert.That(settings.Roles, Contains.Item("admin"));
        Assert.That(settings.Roles, Contains.Item("janitor"));
    }

    [Test]
    public void Settings_CustomHeaders_ShouldBeEmptyByDefault()
    {
        var settings = new ClickHouseClientSettings();

        Assert.That(settings.CustomHeaders, Is.Not.Null);
        Assert.That(settings.CustomHeaders, Is.Empty);
    }

    [Test]
    public void Settings_CustomHeaders_ShouldBeSettable()
    {
        var headers = new Dictionary<string, string>
        {
            { "X-Custom-Header", "value1" },
            { "X-Another-Header", "value2" }
        };
        var settings = new ClickHouseClientSettings { CustomHeaders = headers };

        Assert.That(settings.CustomHeaders, Has.Count.EqualTo(2));
        Assert.That(settings.CustomHeaders["X-Custom-Header"], Is.EqualTo("value1"));
        Assert.That(settings.CustomHeaders["X-Another-Header"], Is.EqualTo("value2"));
    }

    [Test]
    public void Settings_CopyConstructor_ShouldCopyCustomHeaders()
    {
        var original = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string>
            {
                { "X-Custom-Header", "value1" }
            }
        };
        var copy = new ClickHouseClientSettings(original);

        Assert.That(copy.CustomHeaders, Has.Count.EqualTo(1));
        Assert.That(copy.CustomHeaders["X-Custom-Header"], Is.EqualTo("value1"));
        // Verify it's a copy, not the same reference
        Assert.That(copy.CustomHeaders, Is.Not.SameAs(original.CustomHeaders));
    }

    [Test]
    public void Settings_Equals_ShouldReturnTrue_WhenCustomHeadersMatch()
    {
        var settings1 = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string> { { "X-Header", "value" } }
        };
        var settings2 = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string> { { "X-Header", "value" } }
        };

        Assert.That(settings1.Equals(settings2), Is.True);
    }

    [Test]
    public void Settings_Equals_ShouldReturnFalse_WhenCustomHeadersDiffer()
    {
        var settings1 = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string> { { "X-Header", "value1" } }
        };
        var settings2 = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string> { { "X-Header", "value2" } }
        };

        Assert.That(settings1.Equals(settings2), Is.False);
    }

    [Test]
    public void Settings_GetHashCode_ShouldIncludeCustomHeaders()
    {
        var settings1 = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string> { { "X-Header", "value1" } }
        };
        var settings2 = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string> { { "X-Header", "value2" } }
        };

        Assert.That(settings1.GetHashCode(), Is.Not.EqualTo(settings2.GetHashCode()));
    }
    
    [Test]
    public void Settings_ToString_ShouldNotIncludeCustomHeaders_WhenEmpty()
    {
        var settings = new ClickHouseClientSettings();
        var str = settings.ToString();

        Assert.That(str, Does.Not.Contain("CustomHeaders="));
    }

    [Test]
    public void Settings_ToString_ShouldNotExposeCustomHeaderValues()
    {
        var settings = new ClickHouseClientSettings
        {
            CustomHeaders = new Dictionary<string, string>
            {
                { "X-Secret-Header", "secret-value" }
            }
        };
        var str = settings.ToString();

        Assert.That(str, Does.Not.Contain("secret-value"));
    }

    private class TestLoggerFactory : ILoggerFactory
    {
        public void Dispose()
        {
        }

        public ILogger Logger { get; set; }

        public ILogger CreateLogger(string categoryName) => Logger;

        public void AddProvider(ILoggerProvider provider)
        {
        }
    }
}
