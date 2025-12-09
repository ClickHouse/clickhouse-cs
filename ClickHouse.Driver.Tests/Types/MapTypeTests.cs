using System;
using System.Collections.Generic;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

public class MapTypeTests
{
    [Test]
    public void MapType_WithDefaultSettings_ReturnsDictionary()
    {
        // Default settings have mapAsListOfTuples = false
        var settings = TypeSettings.Default;

        var mapType = TypeConverter.ParseClickHouseType("Map(String, Int32)", settings);

        Assert.That(mapType.FrameworkType, Is.EqualTo(typeof(Dictionary<string, int>)));
    }

    [Test]
    public void MapType_WithMapAsListOfTuples_ReturnsListOfTuples()
    {
        var settings = new TypeSettings(useBigDecimal: true, timezone: TypeSettings.DefaultTimezone, mapAsListOfTuples: true);

        var mapType = TypeConverter.ParseClickHouseType("Map(String, Int32)", settings);

        Assert.That(mapType.FrameworkType, Is.EqualTo(typeof(List<(string, int)>)));
    }

    [Test]
    public void MapType_WithComplexKeyType_ReturnsCorrectListType()
    {
        var settings = new TypeSettings(useBigDecimal: true, timezone: TypeSettings.DefaultTimezone, mapAsListOfTuples: true);

        var mapType = TypeConverter.ParseClickHouseType("Map(Tuple(Int32, Int32), String)", settings);

        Assert.That(mapType.FrameworkType, Is.EqualTo(typeof(List<(Tuple<int, int>, string)>)));
    }

    [Test]
    public void MapType_WithNullableValue_ReturnsCorrectListType()
    {
        var settings = new TypeSettings(useBigDecimal: true, timezone: TypeSettings.DefaultTimezone, mapAsListOfTuples: true);

        var mapType = TypeConverter.ParseClickHouseType("Map(String, Nullable(UInt8))", settings);

        Assert.That(mapType.FrameworkType, Is.EqualTo(typeof(List<(string, byte?)>)));
    }

    [Test]
    public void TypeSettings_Default_HasMapAsListOfTuplesFalse()
    {
        var settings = TypeSettings.Default;

        Assert.That(settings.mapAsListOfTuples, Is.False);
    }

    [Test]
    public void ConnectionStringBuilder_MapAsListOfTuples_DefaultsFalse()
    {
        var builder = new ClickHouseConnectionStringBuilder();

        Assert.That(builder.MapAsListOfTuples, Is.False);
    }

    [Test]
    public void ConnectionStringBuilder_MapAsListOfTuples_CanBeSetToTrue()
    {
        var builder = new ClickHouseConnectionStringBuilder
        {
            MapAsListOfTuples = true
        };

        Assert.That(builder.MapAsListOfTuples, Is.True);
    }

    [Test]
    public void ConnectionStringBuilder_MapAsListOfTuples_RoundTripsViaConnectionString()
    {
        var builder = new ClickHouseConnectionStringBuilder
        {
            Host = "localhost",
            MapAsListOfTuples = true
        };

        var connectionString = builder.ConnectionString;
        var parsedBuilder = new ClickHouseConnectionStringBuilder(connectionString);

        Assert.That(parsedBuilder.MapAsListOfTuples, Is.True);
    }

    [Test]
    public void ClientSettings_MapAsListOfTuples_DefaultsFalse()
    {
        var settings = new ClickHouseClientSettings();

        Assert.That(settings.MapAsListOfTuples, Is.False);
    }

    [Test]
    public void ClientSettings_MapAsListOfTuples_FromConnectionString()
    {
        var settings = new ClickHouseClientSettings("Host=localhost;MapAsListOfTuples=true");

        Assert.That(settings.MapAsListOfTuples, Is.True);
    }

    [Test]
    public void ClientSettings_MapAsListOfTuples_CopiesCorrectly()
    {
        var original = new ClickHouseClientSettings { MapAsListOfTuples = true };
        var copy = new ClickHouseClientSettings(original);

        Assert.That(copy.MapAsListOfTuples, Is.True);
    }
}
