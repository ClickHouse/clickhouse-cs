using System;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

public class EnumTypeTests
{
    [Test]
    [TestCase("Enum8('Active' = 0, 'Inactive' = 1)")]
    [TestCase("Enum16('Low' = -32768, 'High' = 32767)")]
    [TestCase("Enum('A' = 1, 'B' = 2)")]
    public void ShouldParseEnumType(string typeString)
    {
        var type = TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);
        ClassicAssert.IsInstanceOf<EnumType>(type);
    }

    [Test]
    [TestCase("status Enum8('Active' = 0, 'Inactive' = 1)")]
    [TestCase("priority Enum16('Low' = 0, 'Medium' = 1, 'High' = 2)")]
    [TestCase("type Enum('A' = 1, 'B' = 2, 'C' = 3)")]
    public void ShouldParseNamedEnumField(string typeString)
    {
        // Named enum fields (as they appear in tuples) should parse correctly
        Assert.DoesNotThrow(() =>
        {
            var type = TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);
            ClassicAssert.IsInstanceOf<EnumType>(type);
        });
    }

    [Test]
    public void ShouldDistinguishEnumVariants()
    {
        var enum8 = TypeConverter.ParseClickHouseType("Enum8('A' = 0)", TypeSettings.Default);
        var enum16 = TypeConverter.ParseClickHouseType("Enum16('A' = 0)", TypeSettings.Default);
        
        ClassicAssert.IsInstanceOf<Enum8Type>(enum8);
        ClassicAssert.IsInstanceOf<Enum16Type>(enum16);
    }

    [Test]
    public void ShouldParseEnumWithMultipleValues()
    {
        var typeString = "Enum8('Active' = 0, 'Inactive' = 1, 'Pending' = 2, 'Deleted' = 3)";
        var type = TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);
        
        ClassicAssert.IsInstanceOf<Enum8Type>(type);
        var enumType = (EnumType)type;
        
        Assert.Multiple(() =>
        {
            Assert.That(enumType.Lookup("Active"), Is.EqualTo(0));
            Assert.That(enumType.Lookup("Inactive"), Is.EqualTo(1));
            Assert.That(enumType.Lookup("Pending"), Is.EqualTo(2));
            Assert.That(enumType.Lookup("Deleted"), Is.EqualTo(3));
        });
    }

    [Test]
    public void ShouldReverseLookupEnumValue()
    {
        var typeString = "Enum8('Active' = 0, 'Inactive' = 1)";
        var type = TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);
        var enumType = (EnumType)type;
        
        Assert.Multiple(() =>
        {
            Assert.That(enumType.Lookup(0), Is.EqualTo("Active"));
            Assert.That(enumType.Lookup(1), Is.EqualTo("Inactive"));
        });
    }
}
