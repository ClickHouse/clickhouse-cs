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

    [Test]
    public void TryLookupShouldReturnTrueForKnownLabelAndFalseForUnknown()
    {
        var typeString = "Enum8('Active' = 1, 'Inactive' = 2)";
        var enumType = (EnumType)TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);

        Assert.Multiple(() =>
        {
            Assert.That(enumType.TryLookup("Active", out var active), Is.True);
            Assert.That(active, Is.EqualTo(1));
            Assert.That(enumType.TryLookup("Missing", out var missing), Is.False);
            Assert.That(missing, Is.EqualTo(0));
        });
    }

    [Test]
    public void ShouldParseEnumLabelWithEscapedQuotesAndParentheses()
    {
        var typeString = "Enum8('None' = -1, 'DateTime(\\'UTC\\')' = 0, 'String' = 1)";
        var type = (EnumType)TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);

        Assert.That(type.Lookup("DateTime('UTC')"), Is.EqualTo(0));
        Assert.That(type.Lookup(0), Is.EqualTo("DateTime('UTC')"));
    }

    [Test]
    public void ShouldParseEnumLabelWithEqualsSign()
    {
        var typeString = "Enum8('a=b' = 1, 'plain' = 2)";
        var type = (EnumType)TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);

        Assert.That(type.Lookup("a=b"), Is.EqualTo(1));
        Assert.That(type.Lookup(1), Is.EqualTo("a=b"));
    }

    // The server numbers a member declared without an ordinal: the first member's ordinal (1 when the first
    // member has none) plus the member's position, counted from 0. Each expected type is the server's
    // toTypeName() of the input (checked against 26.3 and 26.10).
    [TestCase("Enum8('a', 'b')", "Enum8('a' = 1, 'b' = 2)")]
    [TestCase("Enum('a', 'b')", "Enum8('a' = 1, 'b' = 2)")]
    [TestCase("Enum16('a', 'b')", "Enum16('a' = 1, 'b' = 2)")]
    [TestCase("Enum8('a')", "Enum8('a' = 1)")]
    [TestCase("Enum8('a' = 5, 'b', 'c')", "Enum8('a' = 5, 'b' = 6, 'c' = 7)")]
    [TestCase("Enum8('a' = -128, 'b')", "Enum8('a' = -128, 'b' = -127)")]
    [TestCase("Enum8('a', 'b' = 3)", "Enum8('a' = 1, 'b' = 3)")]
    [TestCase("Enum8('x=y', 'b')", "Enum8('x=y' = 1, 'b' = 2)")]
    [TestCase("Enum8('it\\'s', 'b')", "Enum8('it\\'s' = 1, 'b' = 2)")]
    [TestCase("Nullable(Enum8('a', 'b'))", "Nullable(Enum8('a' = 1, 'b' = 2))")]
    [TestCase("Array(Enum8('a', 'b'))", "Array(Enum8('a' = 1, 'b' = 2))")]
    [TestCase("Map(String, Enum16('a', 'b'))", "Map(String, Enum16('a' = 1, 'b' = 2))")]
    public void ParseClickHouseType_EnumMembersWithoutOrdinals_NumbersThemLikeTheServer(string typeString, string expected)
    {
        var type = TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);

        Assert.That(type.ToString(), Is.EqualTo(expected));
    }

    [Test]
    public void ParseClickHouseType_NamedTupleElementEnumWithoutOrdinals_NumbersItsMembers()
    {
        var tuple = (TupleType)TypeConverter.ParseClickHouseType("Tuple(e Enum8('a', 'b'), s String)", TypeSettings.Default);

        Assert.That(tuple.UnderlyingTypes[0].ToString(), Is.EqualTo("Enum8('a' = 1, 'b' = 2)"));
    }

    // The server rejects these with UNEXPECTED_AST_STRUCTURE: after the first member, either every member
    // declares an ordinal or none does.
    [TestCase("Enum8('a', 'b' = 3, 'c')")]
    [TestCase("Enum8('a' = 5, 'b' = 7, 'c')")]
    [TestCase("Enum8('a' = 5, 'b', 'c' = 10, 'd')")]
    public void ParseClickHouseType_EnumMembersMixingBothForms_ThrowsFormatException(string typeString)
    {
        Assert.Throws<FormatException>(() => TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default));
    }

    [Test]
    public void ToString_WithEscapedEnumLabels_ReturnsParseableTypeDeclaration()
    {
        var type = (EnumType)TypeConverter.ParseClickHouseType(
            "Enum8('DateTime(\\'UTC\\')' = -1, 'a=b' = 1)",
            TypeSettings.Default);

        var rendered = type.ToString();
        var reparsed = (EnumType)TypeConverter.ParseClickHouseType(rendered, TypeSettings.Default);

        Assert.Multiple(() =>
        {
            Assert.That(rendered, Is.EqualTo("Enum8('DateTime(\\'UTC\\')' = -1, 'a=b' = 1)"));
            Assert.That(reparsed.Lookup("DateTime('UTC')"), Is.EqualTo(-1));
            Assert.That(reparsed.Lookup("a=b"), Is.EqualTo(1));
        });
    }

    [Test]
    public void ToString_WithEnum16Type_ReturnsCompleteTypeDeclaration()
    {
        var type = (EnumType)TypeConverter.ParseClickHouseType(
            "Enum16('Low' = -32768, 'High' = 32767)",
            TypeSettings.Default);

        Assert.That(type.ToString(), Is.EqualTo("Enum16('Low' = -32768, 'High' = 32767)"));
    }

    // SchemaDescriber writes ToString() back into GetSchemaTable()'s ProviderType and re-parses it, and
    // PocoTypeRegistry keys its row-reader cache on it, so the rendering has to survive a round trip. An
    // unquoted or unescaped label containing a comma or a quote would re-parse as the wrong members.
    [TestCase("Enum8('Active' = 0, 'Inactive' = 1)")]
    [TestCase("Enum16('Low' = -32768, 'High' = 32767)")]
    [TestCase("Enum8('a,b' = 1, 'c' = 2)")]
    [TestCase("Enum8('a=b' = 1, 'plain' = 2)")]
    [TestCase("Enum8('None' = -1, 'DateTime(\\'UTC\\')' = 0, 'String' = 1)")]
    [TestCase("Enum8('back\\\\slash' = 1)")]
    public void ToString_WithQuotableEnumLabels_RoundTripsThroughParse(string typeString)
    {
        var original = (EnumType)TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);

        var reparsed = (EnumType)TypeConverter.ParseClickHouseType(original.ToString(), TypeSettings.Default);

        Assert.That(reparsed.Values, Is.EqualTo(original.Values));
        Assert.That(reparsed.ToString(), Is.EqualTo(original.ToString()));
    }
}
