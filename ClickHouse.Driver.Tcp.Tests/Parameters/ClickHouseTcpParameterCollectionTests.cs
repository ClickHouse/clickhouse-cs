using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.Driver.Tcp.Tests.Parameters;

[TestFixture]
public class ClickHouseTcpParameterCollectionTests
{
    [Test]
    public void Add_TwoParameters_KeepsThemInInsertionOrder()
    {
        var parameters = new ClickHouseTcpParameterCollection { { "b", 1 }, { "a", 2 } };

        Assert.That(parameters, Is.EqualTo(new[]
        {
            new ClickHouseTcpParameter("b", 1),
            new ClickHouseTcpParameter("a", 2),
        }));
    }

    [TestCase(null)]
    [TestCase("")]
    public void Add_NullOrEmptyName_Throws(string name)
    {
        // An empty name would collide with the empty key that terminates the wire parameter list.
        var parameters = new ClickHouseTcpParameterCollection();

        Assert.Throws<ArgumentException>(() => parameters.Add(name, 1));
    }

    [Test]
    public void Add_NameAlreadyBound_Throws()
    {
        var parameters = new ClickHouseTcpParameterCollection { { "id", 1 } };

        Assert.Throws<ArgumentException>(() => parameters.Add("id", 2));
    }

    [Test]
    public void Add_NamesDifferingOnlyByCase_AreDistinct()
    {
        // ClickHouse parameter names are case-sensitive.
        var parameters = new ClickHouseTcpParameterCollection { { "id", 1 }, { "ID", 2 } };

        Assert.That(parameters.Count, Is.EqualTo(2));
    }

    [Test]
    public void Constructor_FromASequence_AddsThemInOrder()
    {
        var parameters = new ClickHouseTcpParameterCollection(
        [
            new ClickHouseTcpParameter("a", 1),
            new ClickHouseTcpParameter("b", 2, "UInt8"),
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.Count, Is.EqualTo(2));
            Assert.That(parameters["b"].ClickHouseType, Is.EqualTo("UInt8"));
        });
    }

    [Test]
    public void Constructor_FromASequenceRepeatingAName_Throws()
    {
        Assert.Throws<ArgumentException>(() => new ClickHouseTcpParameterCollection(
        [
            new ClickHouseTcpParameter("a", 1),
            new ClickHouseTcpParameter("a", 2),
        ]));
    }

    [Test]
    public void Constructor_FromNull_Throws()
        => Assert.Throws<ArgumentNullException>(() => new ClickHouseTcpParameterCollection(null));

    [Test]
    public void Add_NullParameter_Throws()
        => Assert.Throws<ArgumentNullException>(() => new ClickHouseTcpParameterCollection().Add(null));

    [Test]
    public void Contains_BoundAndUnboundNames_AnswersForBoth()
    {
        var parameters = new ClickHouseTcpParameterCollection { { "id", 1 } };

        Assert.Multiple(() =>
        {
            Assert.That(parameters.Contains("id"), Is.True);
            Assert.That(parameters.Contains("other"), Is.False);
            Assert.That(parameters.Contains(null), Is.False);
        });
    }

    [Test]
    public void Indexer_UnboundName_Throws()
    {
        var parameters = new ClickHouseTcpParameterCollection();

        Assert.Throws<KeyNotFoundException>(() => _ = parameters["missing"]);
    }

    [Test]
    public void TryGetValue_BoundName_ReturnsTheParameter()
    {
        var parameters = new ClickHouseTcpParameterCollection { { "id", 42, "UInt8" } };

        Assert.Multiple(() =>
        {
            Assert.That(parameters.TryGetValue("id", out ClickHouseTcpParameter parameter), Is.True);
            Assert.That(parameter.ClickHouseType, Is.EqualTo("UInt8"));
            Assert.That(parameters.TryGetValue("other", out _), Is.False);
        });
    }
}

// Type precedence: parameter override, then SQL hint.
[TestFixture]
public class BuildParametersTests
{
    [Test]
    public void BuildParameters_NoParameters_ReturnsNull()
    {
        // Null keeps the caller from sending an empty list where none was asked for.
        Assert.Multiple(() =>
        {
            Assert.That(ClickHouseTcpClient.BuildParameters("SELECT 1", null), Is.Null);
            Assert.That(ClickHouseTcpClient.BuildParameters("SELECT 1", new ClickHouseTcpQueryOptions()), Is.Null);
            Assert.That(
                ClickHouseTcpClient.BuildParameters("SELECT 1", new ClickHouseTcpQueryOptions { Parameters = new ClickHouseTcpParameterCollection() }),
                Is.Null);
        });
    }

    [Test]
    public void BuildParameters_TypeFromThePlaceholder_FormatsAsThatType()
    {
        // A string bound to a Date placeholder proves the placeholder drove the formatting, not the CLR type.
        IReadOnlyDictionary<string, string> wire = Build(
            "SELECT {d:Date}",
            new ClickHouseTcpParameterCollection { { "d", new DateOnly(2024, 1, 2) } });

        Assert.That(wire["d"], Is.EqualTo("'2024-01-02'"));
    }

    [Test]
    public void BuildParameters_ExplicitType_WinsOverThePlaceholder()
    {
        IReadOnlyDictionary<string, string> wire = Build(
            "SELECT {p:String}",
            new ClickHouseTcpParameterCollection { { "p", "a'b", "Identifier" } });

        // Identifier is the only type left unescaped, so an unescaped quote shows the override was used.
        Assert.That(wire["p"], Is.EqualTo(@"'a\'b'"));
    }

    [Test]
    public void BuildParameters_NoExplicitTypeOrSqlHint_ThrowsNamingTheParameter()
    {
        var parameters = new ClickHouseTcpParameterCollection { { "unused", 42 } };

        var exception = Assert.Throws<ArgumentException>(() => Build("SELECT 1", parameters));

        Assert.That(exception.Message, Does.Contain("unused").And.Contain("{unused:Type}"));
    }

    [TestCase("")]
    [TestCase(" ")]
    public void BuildParameters_ExplicitTypeIsEmpty_ThrowsEvenWhenSqlHasAHint(string clickHouseType)
    {
        var parameters = new ClickHouseTcpParameterCollection { { "p", 42, clickHouseType } };

        var exception = Assert.Throws<ArgumentException>(() => Build("SELECT {p:Int32}", parameters));

        Assert.That(exception.Message, Does.Contain("p").And.Contain("empty ClickHouseType"));
    }

    [Test]
    public void BuildParameters_PlaceholderInsideAComment_IsNotUsedAsAHint()
    {
        var parameters = new ClickHouseTcpParameterCollection { { "p", 7 } };

        var exception = Assert.Throws<ArgumentException>(() => Build("SELECT 1 -- {p:Date}\n", parameters));

        Assert.That(exception.Message, Does.Contain("p").And.Contain("no ClickHouse type"));
    }

    [Test]
    public void BuildParameters_ValueOfAnUnmappableType_ThrowsNamingTheParameter()
    {
        var parameters = new ClickHouseTcpParameterCollection { { "p", new object(), "DateTime" } };

        var exception = Assert.Throws<ArgumentException>(() => Build("SELECT 1", parameters));

        Assert.That(exception.Message, Does.Contain("p"));
    }

    [Test]
    public void BuildParameters_SequenceReadableOnlyOnceWithAnExplicitType_IsReadOnce()
    {
        var once = new SingleUseSequence(1, 2, 3);

        IReadOnlyDictionary<string, string> wire = Build(
            "SELECT 1",
            new ClickHouseTcpParameterCollection { { "p", once, "Array(Int32)" } });

        Assert.Multiple(() =>
        {
            Assert.That(wire["p"], Is.EqualTo("'[1,2,3]'"));
            Assert.That(once.EnumerationCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void BuildParameters_SequenceReadableOnlyOnceWithAPlaceholder_IsAlsoSafe()
    {
        var once = new SingleUseSequence(4, 5);

        IReadOnlyDictionary<string, string> wire = Build(
            "SELECT {p:Array(Int32)}",
            new ClickHouseTcpParameterCollection { { "p", once } });

        Assert.That(wire["p"], Is.EqualTo("'[4,5]'"));
    }

    private static IReadOnlyDictionary<string, string> Build(string sql, ClickHouseTcpParameterCollection parameters)
        => ClickHouseTcpClient.BuildParameters(sql, new ClickHouseTcpQueryOptions { Parameters = parameters });

    /// <summary>A sequence that returns its values only on the first enumeration.</summary>
    private sealed class SingleUseSequence(params int[] values) : IEnumerable<int>
    {
        private bool consumed;

        public int EnumerationCount { get; private set; }

        public IEnumerator<int> GetEnumerator()
        {
            EnumerationCount++;
            if (consumed)
            {
                return Enumerable.Empty<int>().GetEnumerator();
            }

            consumed = true;
            return ((IEnumerable<int>)values).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
