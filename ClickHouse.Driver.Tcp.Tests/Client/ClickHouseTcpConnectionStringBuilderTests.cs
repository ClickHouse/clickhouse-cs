using System;

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
            Assert.That(options.Port, Is.EqualTo(9000));
            Assert.That(options.Username, Is.EqualTo("default"));
            Assert.That(options.Password, Is.EqualTo(string.Empty));
            Assert.That(options.Database, Is.EqualTo("default"));
            Assert.That(options.DialTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
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
}
