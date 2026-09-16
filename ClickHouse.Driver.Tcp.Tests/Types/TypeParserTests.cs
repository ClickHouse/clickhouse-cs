using System;
using System.Linq;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class TypeParserTests
{
    [Test]
    public void Parse_PlainType_HasNameAndNoArguments()
    {
        TypeNode node = TypeParser.Parse("UInt64");

        Assert.Multiple(() =>
        {
            Assert.That(node.Name, Is.EqualTo("UInt64"));
            Assert.That(node.Arguments, Is.Empty);
        });
    }

    [Test]
    public void Parse_SurroundingWhitespace_IsTrimmed()
    {
        TypeNode node = TypeParser.Parse("  String  ");
        Assert.That(node.Name, Is.EqualTo("String"));
    }

    [Test]
    public void Parse_SingleIntegerArgument_IsALeafNode()
    {
        TypeNode node = TypeParser.Parse("FixedString(16)");

        Assert.Multiple(() =>
        {
            Assert.That(node.Name, Is.EqualTo("FixedString"));
            Assert.That(node.Arguments.Select(a => a.Name), Is.EqualTo(new[] { "16" }));
        });
    }

    [Test]
    public void Parse_MultipleArguments_SplitOnTopLevelCommasAndTrimmed()
    {
        TypeNode node = TypeParser.Parse("Decimal(10, 2)");

        Assert.Multiple(() =>
        {
            Assert.That(node.Name, Is.EqualTo("Decimal"));
            Assert.That(node.Arguments.Select(a => a.Name), Is.EqualTo(new[] { "10", "2" }));
        });
    }

    [Test]
    public void Parse_QuotedArgumentWithComma_DoesNotSplitInsideQuotes()
    {
        TypeNode node = TypeParser.Parse("Enum8('a,b' = 1)");

        Assert.Multiple(() =>
        {
            Assert.That(node.Name, Is.EqualTo("Enum8"));
            Assert.That(node.Arguments.Select(a => a.Name), Is.EqualTo(new[] { "'a,b' = 1" }));
        });
    }

    [Test]
    public void Parse_NestedType_DoesNotSplitInsideNestedParens()
    {
        TypeNode node = TypeParser.Parse("Map(String, UInt64)");
        Assert.That(node.Arguments.Select(a => a.Name), Is.EqualTo(new[] { "String", "UInt64" }));
    }

    [Test]
    public void Parse_NestedType_ArgumentIsAFullyParsedChildNode()
    {
        TypeNode node = TypeParser.Parse("Array(Nullable(String))");
        Assert.That(node.Name, Is.EqualTo("Array"));

        TypeNode inner = node.Arguments.Single();
        Assert.Multiple(() =>
        {
            Assert.That(inner.Name, Is.EqualTo("Nullable"));
            Assert.That(inner.Arguments.Single().Name, Is.EqualTo("String"));
            Assert.That(inner.Arguments.Single().Arguments, Is.Empty);
        });
    }

    [Test]
    public void Parse_DeeplyNested_RoundTripsThroughToString()
    {
        TypeNode node = TypeParser.Parse("Map(String, Array(Nullable(UInt64)))");
        Assert.That(node.ToString(), Is.EqualTo("Map(String, Array(Nullable(UInt64)))"));
    }

    [Test]
    public void Parse_DateTimeWithTimezone_NameIsBaseTypeAndArgIsQuoted()
    {
        TypeNode node = TypeParser.Parse("DateTime('UTC')");

        Assert.Multiple(() =>
        {
            Assert.That(node.Name, Is.EqualTo("DateTime"));
            Assert.That(node.Arguments.Single().Name, Is.EqualTo("'UTC'"));
        });
    }

    [Test]
    public void Parse_Null_ThrowsArgumentNull()
        => Assert.Throws<ArgumentNullException>(() => TypeParser.Parse(null));

    [TestCase("")]
    [TestCase("   ")]
    [TestCase("(UInt8)")]
    [TestCase("Array(String")]
    [TestCase("Array(String))")]
    [TestCase("Tuple(,)")]
    [TestCase("Array(String)junk")]
    [TestCase("Enum8('a")]
    [TestCase("DateTime('UTC")]
    public void Parse_Malformed_ThrowsFormat(string type)
        => Assert.Throws<FormatException>(() => TypeParser.Parse(type));
}
