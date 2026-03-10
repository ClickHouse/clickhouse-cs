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
        });
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
