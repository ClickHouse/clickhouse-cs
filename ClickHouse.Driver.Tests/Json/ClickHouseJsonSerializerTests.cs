using System;
using ClickHouse.Driver.Json;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Json;

[TestFixture]
public class ClickHouseJsonSerializerTests
{
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

    [Test]
    public void RegisterType_WithValidPoco_ShouldSucceed()
    {
        // Should not throw
        ClickHouseJsonSerializer.RegisterType<ValidPoco>();

        Assert.That(ClickHouseJsonSerializer.IsTypeRegistered<ValidPoco>(), Is.True);
    }

    [Test]
    public void RegisterType_WithNestedPoco_ShouldRegisterNestedTypesToo()
    {
        ClickHouseJsonSerializer.RegisterType<NestedValidPoco>();

        Assert.That(ClickHouseJsonSerializer.IsTypeRegistered<NestedValidPoco>(), Is.True);
        Assert.That(ClickHouseJsonSerializer.IsTypeRegistered<ValidPoco>(), Is.True);
    }

    [Test]
    public void RegisterType_CalledTwice_ShouldBeIdempotent()
    {
        ClickHouseJsonSerializer.RegisterType<ValidPoco>();
        ClickHouseJsonSerializer.RegisterType<ValidPoco>();

        Assert.That(ClickHouseJsonSerializer.IsTypeRegistered<ValidPoco>(), Is.True);
    }

    [Test]
    public void RegisterType_WithUnsupportedPropertyType_ShouldThrowWithHelpfulMessage()
    {
        var ex = Assert.Throws<ClickHouseJsonSerializationException>(() =>
            ClickHouseJsonSerializer.RegisterType<PocoWithUnsupportedProperty>());

        Assert.That(ex.TargetType, Is.EqualTo(typeof(PocoWithUnsupportedProperty)));
        Assert.That(ex.PropertyName, Is.EqualTo("Pointer"));
        Assert.That(ex.PropertyType, Is.EqualTo(typeof(IntPtr)));
        Assert.That(ex.Message, Does.Contain("PocoWithUnsupportedProperty"));
        Assert.That(ex.Message, Does.Contain("Pointer"));
        Assert.That(ex.Message, Does.Contain("IntPtr"));
    }

    [Test]
    public void RegisterType_WithNestedUnsupportedPropertyType_ShouldThrowWithHelpfulMessage()
    {
        var ex = Assert.Throws<ClickHouseJsonSerializationException>(() =>
            ClickHouseJsonSerializer.RegisterType<PocoWithNestedUnsupportedProperty>());

        // The exception should be about the nested type's unsupported property
        Assert.That(ex.TargetType, Is.EqualTo(typeof(PocoWithUnsupportedProperty)));
        Assert.That(ex.PropertyName, Is.EqualTo("Pointer"));
        Assert.That(ex.PropertyType, Is.EqualTo(typeof(IntPtr)));
    }

    [Test]
    public void RegisterType_WithNullType_ShouldThrowArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            ClickHouseJsonSerializer.RegisterType(null));
    }

    private class PocoWithDuplicatePaths
    {
        [ClickHouseJsonPath("shared.path")]
        public int First { get; set; }

        [ClickHouseJsonPath("shared.path")]
        public string Second { get; set; }
    }

    [Test]
    public void RegisterType_WithDuplicateJsonPaths_ShouldThrow()
    {
        var ex = Assert.Throws<ClickHouseJsonSerializationException>(() =>
            ClickHouseJsonSerializer.RegisterType<PocoWithDuplicatePaths>());

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

    [Test]
    public void IsTypeRegistered_BeforeRegistration_ShouldReturnFalse()
    {
        // Use a unique type that hasn't been registered
        Assert.That(ClickHouseJsonSerializer.IsTypeRegistered<UnregisteredTestClass>(), Is.False);
    }

    [Test]
    public void IsTypeRegistered_WithNullType_ShouldReturnFalse()
    {
        Assert.That(ClickHouseJsonSerializer.IsTypeRegistered(null), Is.False);
    }

    private class UnregisteredTestClass
    {
        public int Value { get; set; }
    }
}
