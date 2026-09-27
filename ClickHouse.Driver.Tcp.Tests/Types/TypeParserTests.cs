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

    [TestCase(",")]
    [TestCase("(")]
    [TestCase(")")]
    [TestCase(" ")]
    public void Parse_BacktickedIdentifierWithBreakCharacter_IsOneArgument(string inside)
    {
        // Structural characters inside backticks belong to the identifier.
        TypeNode node = TypeParser.Parse($"JSON(`a{inside}b` Int64)");

        Assert.Multiple(() =>
        {
            Assert.That(node.Name, Is.EqualTo("JSON"));
            Assert.That(node.Arguments.Select(a => a.Name), Is.EqualTo(new[] { $"`a{inside}b` Int64" }));
        });
    }

    [TestCase("JSON(`a,b` Int64)")]
    [TestCase("Tuple(`a b` Int64, c String)")]
    [TestCase("Nested(`a(b` UInt8)")]
    [TestCase(@"Tuple(`a\`b` Int8)")]
    [TestCase(@"Tuple(`a\nb` Int8)")]
    public void Parse_BacktickedIdentifier_RoundTripsThroughToString(string type)
    {
        // Preserve the server's quoted spelling when rebuilding the type name.
        Assert.That(TypeParser.Parse(type).ToString(), Is.EqualTo(type));
    }

    [Test]
    public void Parse_DoubledBacktick_ClosesAtTheFinalBacktick()
    {
        // The server accepts the doubled form on input, though it prints the backslash form.
        TypeNode node = TypeParser.Parse("Tuple(`a``b` Int8)");
        Assert.That(node.Arguments.Single().Name, Is.EqualTo("`a``b` Int8"));
    }

    [Test]
    public void Parse_EmptyQuotedLabel_ClosesTheSpan()
    {
        // Two quotes form an empty label, not an escaped quote.
        TypeNode node = TypeParser.Parse("Enum8('' = 1)");
        Assert.That(node.Arguments.Single().Name, Is.EqualTo("'' = 1"));
    }

    [TestCase("Array( Array(Int32) )", "Array(Array(Int32))")]
    [TestCase("Tuple(a UInt8, b Tuple(c UInt8) )", "Tuple(a UInt8, b Tuple(c UInt8))")]
    [TestCase("Map( String , UInt64 )", "Map(String, UInt64)")]
    [TestCase("Array(Int32) ", "Array(Int32)")]
    public void Parse_WhitespaceBetweenStructuralCharacters_ParsesLikeTheCompactSpelling(string type, string expected)
    {
        // Ignore whitespace between structural tokens.
        Assert.That(TypeParser.Parse(type).ToString(), Is.EqualTo(expected));
    }

    [Test]
    public void Parse_SpacedEmptyArgumentList_IsTheZeroElementNode()
    {
        TypeNode node = TypeParser.Parse("Tuple( )");

        Assert.Multiple(() =>
        {
            Assert.That(node.Arguments, Is.Empty);
            Assert.That(node.HasArgumentList, Is.True);
            Assert.That(node.ToString(), Is.EqualTo("Tuple()"));
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
    public void Parse_EmptyArgumentList_HasNoArgumentsButKeepsTheParentheses()
    {
        // Tuple() is a legal ClickHouse type, so an empty argument list parses rather than throwing. Arguments is
        // empty either way, so HasArgumentList is what separates it from a bare Tuple, and ToString must keep the
        // parentheses or the two collapse into one string.
        TypeNode node = TypeParser.Parse("Tuple()");

        Assert.Multiple(() =>
        {
            Assert.That(node.Name, Is.EqualTo("Tuple"));
            Assert.That(node.Arguments, Is.Empty);
            Assert.That(node.HasArgumentList, Is.True);
            Assert.That(node.ToString(), Is.EqualTo("Tuple()"));
        });
    }

    [Test]
    public void Parse_NameWithoutParentheses_HasNoArgumentList()
    {
        TypeNode node = TypeParser.Parse("Tuple");

        Assert.Multiple(() =>
        {
            Assert.That(node.Arguments, Is.Empty);
            Assert.That(node.HasArgumentList, Is.False);
            Assert.That(node.ToString(), Is.EqualTo("Tuple"));
        });
    }

    [Test]
    public void Parse_EmptyArgumentListNested_RoundTripsThroughToString()
    {
        TypeNode node = TypeParser.Parse("Array(Tuple(Int32, Tuple()))");

        Assert.That(node.ToString(), Is.EqualTo("Array(Tuple(Int32, Tuple()))"));
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
    [TestCase("Tuple(`a b Int64)")]
    [TestCase("Tuple( , )")]
    public void Parse_Malformed_ThrowsFormat(string type)
        => Assert.Throws<FormatException>(() => TypeParser.Parse(type));
}
