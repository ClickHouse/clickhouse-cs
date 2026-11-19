using System;
using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

public class ConnectionStringBuilderTests
{
    [Test]
    public void ShouldHaveReasonableDefaults()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new ClickHouseConnectionStringBuilder().Port, Is.EqualTo(8123));
            Assert.That(new ClickHouseConnectionStringBuilder("Protocol=https").Port, Is.EqualTo(8443));
            Assert.That(new ClickHouseConnectionStringBuilder().Database, Is.EqualTo(""));
            Assert.That(new ClickHouseConnectionStringBuilder().Username, Is.EqualTo("default"));
            Assert.That(new ClickHouseConnectionStringBuilder().ReadBufferSize, Is.EqualTo(ClickHouseDefaults.ReadBufferSize));
            Assert.That(new ClickHouseConnectionStringBuilder().UseFormDataParameters, Is.EqualTo(ClickHouseDefaults.UseFormDataParameters));
        });
    }

    [TestCase("lz4")]
    [TestCase("br, gzip")]
    [TestCase("identity")]
    public void ConnectionStringBuilder_AcceptEncoding_ShouldRoundTripThroughConnectionStringAndSettings(string acceptEncoding)
    {
        var builder = new ClickHouseConnectionStringBuilder { AcceptEncoding = acceptEncoding };

        var reparsed = new ClickHouseConnectionStringBuilder(builder.ConnectionString);
        var settings = ClickHouseClientSettings.FromConnectionString(builder.ConnectionString);
        var backToBuilder = ClickHouseConnectionStringBuilder.FromSettings(settings);

        Assert.Multiple(() =>
        {
            Assert.That(reparsed.AcceptEncoding, Is.EqualTo(acceptEncoding));
            Assert.That(settings.AcceptEncoding, Is.EqualTo(acceptEncoding));
            Assert.That(backToBuilder.AcceptEncoding, Is.EqualTo(acceptEncoding));
        });
    }

    [Test]
    public void ConnectionStringBuilder_WithoutAcceptEncoding_LeavesTheSettingUnset()
    {
        var settings = ClickHouseClientSettings.FromConnectionString("Host=localhost");

        Assert.That(settings.AcceptEncoding, Is.Null, "null is what selects the driver's default codec list");
    }

    [Test]
    public void ConnectionStringBuilder_ReadBufferSize_ShouldRoundTripThroughConnectionString()
    {
        var builder = new ClickHouseConnectionStringBuilder { ReadBufferSize = 65536 };

        var reparsed = new ClickHouseConnectionStringBuilder(builder.ConnectionString);

        Assert.That(reparsed.ReadBufferSize, Is.EqualTo(65536));
    }
    
    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public void ConnectionStringBuilder_UseFormDataParameters_ShouldRoundTripThroughConnectionString(bool useFormDataParameters)
    {
        var builder = new ClickHouseConnectionStringBuilder { UseFormDataParameters = useFormDataParameters };

        var reparsed = new ClickHouseConnectionStringBuilder(builder.ConnectionString);

        Assert.That(reparsed.UseFormDataParameters, Is.EqualTo(useFormDataParameters));
    }
    
    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public void ConnectionStringBuilder_UseFormDataParameters_ShouldParseUseFormDataParameters(bool useFormDataParameters)
    {
        var builder = new ClickHouseConnectionStringBuilder($"UseFormDataParameters={useFormDataParameters}");

        Assert.That(builder.UseFormDataParameters, Is.EqualTo(useFormDataParameters));
    }
    
    [TestCase("true", true)]
    [TestCase("TRUE", true)]
    [TestCase("1", true)]
    [TestCase("false", false)]
    [TestCase("False", false)]
    [TestCase("0", false)]
    public void ConnectionStringBuilder_BooleanValue_ShouldParseAcceptedSpellings(string value, bool expected)
    {
        // Compression defaults to true and UseSession to false, so neither result can come from the default.
        var settings = ClickHouseClientSettings.FromConnectionString($"Host=localhost;Compression={value};UseSession={value}");

        Assert.Multiple(() =>
        {
            Assert.That(settings.UseCompression, Is.EqualTo(expected));
            Assert.That(settings.UseSession, Is.EqualTo(expected));
        });
    }

    [TestCase("string")]
    [TestCase("1")]
    public void ConnectionStringBuilder_EnumValue_ShouldParseNameInAnyCaseOrDefinedNumber(string value)
    {
        var settings = ClickHouseClientSettings.FromConnectionString($"Host=localhost;JsonReadMode={value}");

        Assert.That(settings.JsonReadMode, Is.EqualTo(JsonReadMode.String));
    }

    [TestCase("Compression=yes", "Compression")]
    [TestCase("Compression=ture", "Compression")]
    [TestCase("Compression=lz4", "Compression")]
    [TestCase("UseSession=on", "UseSession")]
    [TestCase("Port=abc", "Port")]
    [TestCase("Port=65536", "Port")]
    [TestCase("Port=-1", "Port")]
    [TestCase("ReadBufferSize=lots", "ReadBufferSize")]
    [TestCase("Timeout=abc", "Timeout")]
    [TestCase("JsonReadMode=bogus", "JsonReadMode")]
    [TestCase("JsonReadMode=42", "JsonReadMode")]
    [TestCase("JsonWriteMode=xyz", "JsonWriteMode")]
    [TestCase("MapReadMode=nonsense", "MapReadMode")]
    public void ConnectionStringBuilder_UnparseableValue_ShouldThrowNamingTheKey(string pair, string key)
    {
        var ex = Assert.Throws<ArgumentException>(() => new ClickHouseClient($"Host=localhost;{pair}"));

        Assert.That(ex.Message, Does.Contain($"'{key}'"));
    }

    [Test]
    public void ConnectionStringBuilder_ShouldParseRoles_SingleRole()
    {
        var connectionString = "Host=localhost;Roles=admin";
        var builder = new ClickHouseConnectionStringBuilder(connectionString);

        Assert.That(builder.Roles, Has.Count.EqualTo(1));
        Assert.That(builder.Roles[0], Is.EqualTo("admin"));
    }

    [Test]
    public void ConnectionStringBuilder_ShouldParseRoles_MultipleRoles()
    {
        var connectionString = "Host=localhost;Roles=admin,reader,writer";
        var builder = new ClickHouseConnectionStringBuilder(connectionString);

        Assert.That(builder.Roles, Has.Count.EqualTo(3));
        Assert.That(builder.Roles, Contains.Item("admin"));
        Assert.That(builder.Roles, Contains.Item("reader"));
        Assert.That(builder.Roles, Contains.Item("writer"));
    }

    [Test]
    public void ConnectionStringBuilder_ShouldTrimRoles()
    {
        var connectionString = "Host=localhost;Roles= admin , reader ";
        var builder = new ClickHouseConnectionStringBuilder(connectionString);

        Assert.That(builder.Roles, Has.Count.EqualTo(2));
        Assert.That(builder.Roles[0], Is.EqualTo("admin"));
        Assert.That(builder.Roles[1], Is.EqualTo("reader"));
    }

    [Test]
    public void ConnectionStringBuilder_ShouldIgnoreEmptyRoles()
    {
        var connectionString = "Host=localhost;Roles=admin,,reader,";
        var builder = new ClickHouseConnectionStringBuilder(connectionString);

        Assert.That(builder.Roles, Has.Count.EqualTo(2));
        Assert.That(builder.Roles, Contains.Item("admin"));
        Assert.That(builder.Roles, Contains.Item("reader"));
    }

    [Test]
    public void ConnectionStringBuilder_Roles_ShouldBeSettable()
    {
        var builder = new ClickHouseConnectionStringBuilder();
        builder.Roles = new[] { "admin", "reader" };

        Assert.That(builder.Roles, Has.Count.EqualTo(2));
        Assert.That(builder.ConnectionString, Does.Contain("Roles=admin,reader"));
    }

    [Test]
    public void ConnectionStringBuilder_Roles_SetToEmpty_ShouldRemoveKey()
    {
        var builder = new ClickHouseConnectionStringBuilder("Host=localhost;Roles=admin");
        builder.Roles = Array.Empty<string>();

        Assert.That(builder.Roles, Is.Empty);
        Assert.That(builder.ConnectionString, Does.Not.Contain("Roles"));
    }

    [Test]
    public void ConnectionStringBuilder_Roles_SetToNull_ShouldRemoveKey()
    {
        var builder = new ClickHouseConnectionStringBuilder("Host=localhost;Roles=admin");
        builder.Roles = null;

        Assert.That(builder.Roles, Is.Empty);
        Assert.That(builder.ConnectionString, Does.Not.Contain("Roles"));
    }

    [Test]
    public void ConnectionStringBuilder_RoundToSettingsTrip_ShouldPreserveRoles()
    {
        var originalSettings = new ClickHouseClientSettings { Roles = new[] { "admin", "reader" } };
        var builder = ClickHouseConnectionStringBuilder.FromSettings(originalSettings);
        var restoredSettings = builder.ToSettings();

        Assert.That(restoredSettings.Roles, Is.EqualTo(originalSettings.Roles));
    }
}
