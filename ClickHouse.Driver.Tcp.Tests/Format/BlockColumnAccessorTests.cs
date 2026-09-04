using System;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Format;

// The by-name and typed column accessors. Hand-built blocks rather than a server round-trip: the point of these
// is the lookup and the failure messages, which a well-formed result never reaches.
[TestFixture]
public class BlockColumnAccessorTests
{
    [Test]
    public void IndexerByName_ColumnPresent_ReturnsIt()
    {
        using Block block = TwoColumnBlock();

        Assert.That(block["id"].Name, Is.EqualTo("id"));
        Assert.That(block["label"].Name, Is.EqualTo("label"));
    }

    [Test]
    public void IndexerByName_ColumnAbsent_ThrowsListingTheColumnsItHas()
    {
        using Block block = TwoColumnBlock();

        var e = Assert.Throws<ArgumentException>(() => _ = block["nope"]);

        Assert.Multiple(() =>
        {
            Assert.That(e.Message, Does.Contain("'nope'"), "the name that was asked for");
            Assert.That(e.Message, Does.Contain("id").And.Contain("label"), "what the caller could have asked for");
        });
    }

    [Test]
    public void IndexerByName_NamedBlock_NamesTheBlockInTheMessage()
    {
        using Block block = new("Log", default, 1, [Ids()], null, default);

        var e = Assert.Throws<ArgumentException>(() => _ = block["nope"]);

        Assert.That(e.Message, Does.Contain("Block 'Log'"));
    }

    [Test]
    public void IndexerByName_NullName_Throws()
    {
        using Block block = TwoColumnBlock();

        Assert.Throws<ArgumentNullException>(() => _ = block[null]);
    }

    [Test]
    public void TryGetColumn_ColumnPresent_ReturnsTrueAndTheColumn()
    {
        using Block block = TwoColumnBlock();

        Assert.That(block.TryGetColumn("label", out IColumn column), Is.True);
        Assert.That(column.Name, Is.EqualTo("label"));
    }

    [Test]
    public void TryGetColumn_ColumnAbsent_ReturnsFalseAndNull()
    {
        using Block block = TwoColumnBlock();

        Assert.That(block.TryGetColumn("nope", out IColumn column), Is.False);
        Assert.That(column, Is.Null);
    }

    [Test]
    public void TryGetColumn_NameDifferingOnlyInCase_ReturnsFalse()
    {
        // ClickHouse column names are case-sensitive, so the match is ordinal.
        using Block block = TwoColumnBlock();

        Assert.That(block.TryGetColumn("ID", out _), Is.False);
    }

    [Test]
    public void ColumnByName_MatchingType_ReturnsTheTypedView()
    {
        using Block block = TwoColumnBlock();

        IColumn<ulong> ids = block.Column<ulong>("id");

        Assert.That(ids.Values.ToArray(), Is.EqualTo(new ulong[] { 7, 8 }));
    }

    [Test]
    public void ColumnByName_WrongType_ThrowsNamingTheTypeTheColumnHas()
    {
        using Block block = TwoColumnBlock();

        var e = Assert.Throws<InvalidCastException>(() => block.Column<string>("id"));

        Assert.Multiple(() =>
        {
            Assert.That(e.Message, Does.Contain("'id'"));
            Assert.That(e.Message, Does.Contain("UInt64"), "the ClickHouse type it actually has");
            Assert.That(e.Message, Does.Contain("String"), "the CLR type that was asked for");
        });
    }

    [Test]
    public void ColumnByName_ColumnAbsent_Throws()
    {
        using Block block = TwoColumnBlock();

        Assert.Throws<ArgumentException>(() => block.Column<ulong>("nope"));
    }

    [Test]
    public void ColumnByIndex_MatchingType_ReturnsTheTypedView()
    {
        using Block block = TwoColumnBlock();

        Assert.That(block.Column<string>(1).Values.ToArray(), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void ColumnByIndex_WrongType_Throws()
    {
        using Block block = TwoColumnBlock();

        Assert.Throws<InvalidCastException>(() => block.Column<ulong>(1));
    }

    [TestCase(-1)]
    [TestCase(2)]
    public void ColumnByIndex_OutOfRange_Throws(int index)
    {
        using Block block = TwoColumnBlock();

        Assert.Throws<ArgumentOutOfRangeException>(() => block.Column<ulong>(index));
    }

    private static Block TwoColumnBlock()
        => new(string.Empty, default, 2, [Ids(), Labels()], null, default);

    private static IColumn Ids() => PrimitiveColumn<ulong>.FromValues("id", "UInt64", [7UL, 8UL]);

    private static IColumn Labels() => new ArrayColumn<string>("label", "String", ["a", "b"]);
}
