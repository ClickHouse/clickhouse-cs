using System.Collections.Generic;
using System.Linq;
using ClickHouse.Driver.Types.Grammar;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

public class TypeGrammarParsingTests
{
    [Test]
    [TestCaseSource(typeof(TypeGrammarParsingTests), nameof(Types))]
    public static void ShouldRoundTripParsedType(string input)
    {
        var output = Parser.Parse(input);
        Assert.That(output.ToString(), Is.EqualTo(input));
    }

    [Test]
    [TestCase("Enum8('a b' = 1, 'c d' = 2)")]
    [TestCase("Enum8('a,b' = 1, 'c(d)' = 2)")]
    [TestCase(@"Enum8('a\'b' = 1, 'c\`d' = 2)")]
    [TestCase("DateTime64(3, 'Europe/Amsterdam')")]
    [TestCase("Map(String, Array(Nullable(Int32)))")]
    public static void ShouldRoundTripParsedTypeWhenArgumentsAreSingleQuoted(string input)
    {
        var output = Parser.Parse(input);
        Assert.That(output.ToString(), Is.EqualTo(input));
    }

    [Test]
    [TestCase("JSON(`a b` Int64)")]
    [TestCase("JSON(`a,b` Int64)")]
    [TestCase("JSON(`a b` Decimal(10, 2))")]
    [TestCase(@"JSON(`a\`b` Int64)")]
    [TestCase("JSON(max_dynamic_paths=8, `a b` Int64, SKIP `x,y`)")]
    public static void ShouldRoundTripParsedTypeWhenIdentifiersAreBacktickQuoted(string input)
    {
        var output = Parser.Parse(input);
        Assert.That(output.ToString(), Is.EqualTo(input));
    }

    [Test]
    public static void ParseShouldReturnNodeWithoutChildrenWhenParameterListIsEmpty()
    {
        var output = Parser.Parse("Tuple()");
        Assert.Multiple(() =>
        {
            Assert.That(output.Value, Is.EqualTo("Tuple"));
            Assert.That(output.ChildNodes, Is.Empty);
        });
    }

    [Test]
    [TestCase("Tuple()")]
    [TestCase("Array(Tuple())")]
    [TestCase("Map(String, Tuple())")]
    [TestCase("Tuple(Tuple(), Int32)")]
    [TestCase("Tuple(Int32, Tuple())")]
    [TestCase("Array(Array(Tuple()))")]
    public static void ParseShouldNotProduceCyclicTreeWhenParameterListIsEmpty(string input)
    {
        AssertAcyclic(Parser.Parse(input), []);
    }

    [Test]
    public static void ParseShouldKeepSiblingsWhenOneParameterListIsEmpty()
    {
        var output = Parser.Parse("Tuple(Tuple(), Int32)");
        Assert.That(output.ChildNodes, Has.Count.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(output.ChildNodes[0].Value, Is.EqualTo("Tuple"));
            Assert.That(output.ChildNodes[0].ChildNodes, Is.Empty);
            Assert.That(output.ChildNodes[1].Value, Is.EqualTo("Int32"));
        });
    }

    [Test]
    [TestCase("Tuple(Int32)", 1)]
    [TestCase("Tuple(Int32, String)", 2)]
    [TestCase("DateTime64(3, 'UTC')", 2)]
    public static void ParseShouldKeepEveryChildWhenParameterListIsNotEmpty(string input, int expectedChildCount)
    {
        Assert.That(Parser.Parse(input).ChildNodes, Has.Count.EqualTo(expectedChildCount));
    }

    private static void AssertAcyclic(SyntaxTreeNode node, HashSet<SyntaxTreeNode> ancestors)
    {
        Assert.That(ancestors, Does.Not.Contain(node), $"Node '{node.Value}' is its own ancestor");
        ancestors.Add(node);
        foreach (var child in node.ChildNodes)
        {
            AssertAcyclic(child, ancestors);
        }
        ancestors.Remove(node);
    }

    public static IList<string> Types => TestCases.GetDataTypeSamples().Select(s => s.ClickHouseType).Distinct().OrderBy(t => t).ToList();
}

