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

    public static IList<string> Types => TestCases.GetDataTypeSamples().Select(s => s.ClickHouseType).Distinct().OrderBy(t => t).ToList();
}

