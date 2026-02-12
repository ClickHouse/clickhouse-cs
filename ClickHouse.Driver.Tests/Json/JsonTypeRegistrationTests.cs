using System;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Json;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Json;

[TestFixture]
public class JsonTypeRegistrationTests
{
    private ClickHouseClient client;

    private class ValidPoco
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public double Score { get; set; }
    }

    private class NestedValidPoco
    {
        public int Id { get; set; }
        public ValidPoco Child { get; set; }
    }

    private class PocoWithUnsupportedProperty
    {
        public int Id { get; set; }
        public IntPtr Pointer { get; set; }
    }

    private class PocoWithNestedUnsupportedProperty
    {
        public int Id { get; set; }
        public PocoWithUnsupportedProperty Nested { get; set; }
    }

    private class PocoWithDuplicatePaths
    {
        [ClickHouseJsonPath("shared.path")]
        public int First { get; set; }

        [ClickHouseJsonPath("shared.path")]
        public string Second { get; set; }
    }

    [SetUp]
    public void SetUp()
    {
        client = new ClickHouseClient(new ClickHouseClientSettings());
    }

    [TearDown]
    public void TearDown()
    {
        client?.Dispose();
    }

    [Test]
    public void RegisterJsonSerializationType_WithValidPoco_ShouldSucceed()
    {
        // Should not throw
        client.RegisterJsonSerializationType<ValidPoco>();
    }

    [Test]
    public void RegisterJsonSerializationType_WithNestedPoco_ShouldSucceed()
    {
        // Should not throw - nested types are registered automatically
        client.RegisterJsonSerializationType<NestedValidPoco>();
    }

    [Test]
    public void RegisterJsonSerializationType_CalledTwice_ShouldBeIdempotent()
    {
        // Should not throw when called multiple times
        client.RegisterJsonSerializationType<ValidPoco>();
        client.RegisterJsonSerializationType<ValidPoco>();
    }

    [Test]
    public void RegisterJsonSerializationType_WithUnsupportedPropertyType_ShouldThrowWithHelpfulMessage()
    {
        var ex = Assert.Throws<ClickHouseJsonSerializationException>(() =>
            client.RegisterJsonSerializationType<PocoWithUnsupportedProperty>());

        Assert.That(ex.TargetType, Is.EqualTo(typeof(PocoWithUnsupportedProperty)));
        Assert.That(ex.PropertyName, Is.EqualTo("Pointer"));
        Assert.That(ex.PropertyType, Is.EqualTo(typeof(IntPtr)));
        Assert.That(ex.Message, Does.Contain("PocoWithUnsupportedProperty"));
        Assert.That(ex.Message, Does.Contain("Pointer"));
        Assert.That(ex.Message, Does.Contain("IntPtr"));
    }

    [Test]
    public void RegisterJsonSerializationType_WithNestedUnsupportedPropertyType_ShouldThrowWithHelpfulMessage()
    {
        var ex = Assert.Throws<ClickHouseJsonSerializationException>(() =>
            client.RegisterJsonSerializationType<PocoWithNestedUnsupportedProperty>());

        // The exception should be about the nested type's unsupported property
        Assert.That(ex.TargetType, Is.EqualTo(typeof(PocoWithUnsupportedProperty)));
        Assert.That(ex.PropertyName, Is.EqualTo("Pointer"));
        Assert.That(ex.PropertyType, Is.EqualTo(typeof(IntPtr)));
    }

    [Test]
    public void RegisterJsonSerializationType_WithNullType_ShouldThrowArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            client.RegisterJsonSerializationType(null));
    }

    [Test]
    public void RegisterJsonSerializationType_WithDuplicateJsonPaths_ShouldThrow()
    {
        var ex = Assert.Throws<ClickHouseJsonSerializationException>(() =>
            client.RegisterJsonSerializationType<PocoWithDuplicatePaths>());

        Assert.That(ex.Message, Does.Contain("shared.path"));
    }

    [Test]
    public void JsonPathAttribute_WithEmptyPath_ShouldThrow()
    {
        Assert.Throws<ArgumentException>(() => new ClickHouseJsonPathAttribute(""));
        Assert.Throws<ArgumentException>(() => new ClickHouseJsonPathAttribute("   "));
        Assert.Throws<ArgumentException>(() => new ClickHouseJsonPathAttribute(null));
    }

    [Test]
    public void JsonPathAttribute_WithValidPath_ShouldSucceed()
    {
        var attr1 = new ClickHouseJsonPathAttribute("simple");
        Assert.That(attr1.Path, Is.EqualTo("simple"));

        var attr2 = new ClickHouseJsonPathAttribute("nested.path");
        Assert.That(attr2.Path, Is.EqualTo("nested.path"));

        var attr3 = new ClickHouseJsonPathAttribute("deeply.nested.path.here");
        Assert.That(attr3.Path, Is.EqualTo("deeply.nested.path.here"));
    }
}
