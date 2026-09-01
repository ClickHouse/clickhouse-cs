using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class EnumColumnCodecTests
{
    [Test]
    public void Create_Enum8_ParsesLabelMap()
    {
        var codec = (EnumColumnCodec<sbyte>)Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = -1, 'b' = 0, 'c' = 127)"));

        Assert.Multiple(() =>
        {
            Assert.That(codec.LabelToOrdinal["a"], Is.EqualTo((sbyte)-1));
            Assert.That(codec.LabelToOrdinal["c"], Is.EqualTo((sbyte)127));
            Assert.That(codec.OrdinalToLabel[(sbyte)0], Is.EqualTo("b"));
        });
    }

    [Test]
    public void Create_Enum8_OrdinalOutOfRange_Throws()
        => Assert.Throws<FormatException>(() => Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = 128)")));

    [Test]
    public void Create_Enum8_NoMembers_Throws()
        => Assert.Throws<FormatException>(() => Enum8ColumnCodec.Create(TypeParser.Parse("Enum8()")));

    [Test]
    public void Create_MalformedMember_Throws()
        => Assert.Throws<FormatException>(() => Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a')")));

    [Test]
    public void Create_DuplicateLabel_Throws()
        => Assert.Throws<FormatException>(() => Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = 1, 'a' = 2)")));

    [Test]
    public void Create_DuplicateOrdinal_Throws()
        => Assert.Throws<FormatException>(() => Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = 1, 'b' = 1)")));

    [TestCase(@"'a\'b' = 1", "a'b")]
    [TestCase(@"'a\\b' = 1", @"a\b")]
    [TestCase(@"'a\nb' = 1", "a\nb")]
    [TestCase(@"'a\tb' = 1", "a\tb")]
    [TestCase(@"'a\0b' = 1", "a\0b")]
    [TestCase(@"'a\x41b' = 1", "aAb")]
    [TestCase(@"'a\zb' = 1", @"a\zb")]
    [TestCase("'a''b' = 1", "a'b")]
    [TestCase("'' = 1", "")]
    public void Create_LabelWithAnEscape_DecodesItLikeTheServer(string member, string expected)
    {
        // The label in a header carries the server's escaping, and the label a caller reads or writes is the
        // decoded one. Checked against 26.6: hex('\a') is 07 … hex('\x41') is 41, an escape the server does not
        // define keeps both characters (hex('\z') is 5C7A), and a doubled quote is one quote.
        var codec = (EnumColumnCodec<sbyte>)Enum8ColumnCodec.Create(TypeParser.Parse($"Enum8({member})"));
        Assert.That(codec.OrdinalToLabel[(sbyte)1], Is.EqualTo(expected));
    }

    [Test]
    public void CanWrite_LabelOrOrdinalColumn_AcceptsBothAndRejectsOthers()
    {
        IColumnCodec codec = Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = -1, 'b' = 127)"));

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<sbyte>("v", null, new sbyte[] { -1 })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<string>("v", null, new[] { "a" })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<int>("v", null, new[] { 1 })), Is.False, "the ordinal has to be the declared width");
            Assert.That(codec.CanWriteElementType(typeof(string)), Is.True);
        });
    }

    /// <summary>
    /// The placeholder a <c>Nullable(Enum8)</c> writes at a null row has to be a declared member — the server
    /// rejects an undeclared ordinal even where it is never read — so the label form must be a declared label.
    /// </summary>
    [Test]
    public void NullPlaceholderAs_String_IsADeclaredLabel()
    {
        IColumnCodec codec = Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = -1, 'b' = 127)"));

        Assert.Multiple(() =>
        {
            Assert.That(codec.NullPlaceholderAs(typeof(string)), Is.EqualTo("a"));
            Assert.That(codec.NullPlaceholderAs(typeof(sbyte)), Is.EqualTo((sbyte)-1));
            Assert.Throws<NotSupportedException>(() => codec.NullPlaceholderAs(typeof(int)));
        });
    }

    [Test]
    public void WriteColumn_LabelTheTypeDoesNotDeclare_ThrowsNamingTheLabelAndTheLabelsItHas()
    {
        IColumnCodec codec = Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = -1, 'b' = 127)"));
        var labels = new ArrayColumn<string>("v", null, new[] { "a", "c" });

        ArgumentException thrown = Assert.ThrowsAsync<ArgumentException>(() => CodecTestHarness.WriteSliceAsync(codec, labels, 0, 2));
        Assert.That(thrown.Message, Does.Contain("'c' is not a label of 'Enum8('a' = -1, 'b' = 127)'").And.Contain("'a', 'b'"));
    }

    /// <summary>
    /// Enum16 stores its ordinals two bytes wide, and the view widens them to long for the label lookup. A
    /// negative ordinal is the case that separates a sign extension from a reinterpretation.
    /// </summary>
    [Test]
    public void GetLabel_Enum16_WidensTheTwoByteOrdinalIncludingNegativeOnes()
    {
        var members = new EnumMemberTable(
            "Enum16('low' = -32768, 'high' = 32767)",
            new[] { new KeyValuePair<string, long>("low", -32768), new KeyValuePair<string, long>("high", 32767) });
        using var column = new EnumColumn<short>(
            PrimitiveColumn<short>.FromValues("v", "Enum16('low' = -32768, 'high' = 32767)", new short[] { -32768, 32767 }),
            members);

        Assert.Multiple(() =>
        {
            Assert.That(column.GetLabel(0), Is.EqualTo("low"));
            Assert.That(column.GetLabel(1), Is.EqualTo("high"));
            Assert.That(column.TryGetOrdinal("low", out long ordinal), Is.True);
            Assert.That(ordinal, Is.EqualTo(-32768));
        });
    }

    [Test]
    public void GetLabel_OrdinalWithNoDeclaredMember_ThrowsWhileTheDeclaredOnesResolve()
    {
        var members = new EnumMemberTable("Enum8('a' = 1)", new[] { new KeyValuePair<string, long>("a", 1) });
        using var column = new EnumColumn<sbyte>(PrimitiveColumn<sbyte>.FromValues("v", "Enum8('a' = 1)", new sbyte[] { 1, 0 }), members);

        Assert.Multiple(() =>
        {
            Assert.That(column.Members, Is.EqualTo(new[] { new KeyValuePair<string, long>("a", 1) }));
            Assert.That(column.GetLabel(0), Is.EqualTo("a"));
            Assert.Throws<KeyNotFoundException>(() => column.GetLabel(1), "row 1 holds the undeclared ordinal 0");
            Assert.That(column.TryGetLabel(0, out string absent), Is.False);
            Assert.That(absent, Is.Null);
            Assert.That(column.TryGetOrdinal("a", out long ordinal), Is.True);
            Assert.That(ordinal, Is.EqualTo(1));
            Assert.That(column.TryGetOrdinal("zz", out _), Is.False);
            Assert.Throws<ArgumentNullException>(() => column.TryGetOrdinal(null, out _));
        });
    }

    /// <summary>
    /// <c>Members</c> is public API over a list the codec built while parsing, so it is handed out wrapped: a
    /// consumer that cast it back could otherwise leave it disagreeing with the lookups beside it.
    /// </summary>
    [Test]
    public void Members_IsNotWritableThroughACastBackToItsBackingCollection()
    {
        var declared = new List<KeyValuePair<string, long>> { new("a", 1) };
        var members = new EnumMemberTable("Enum8('a' = 1)", declared);

        declared.Add(new KeyValuePair<string, long>("b", 2));

        Assert.Multiple(() =>
        {
            Assert.That(members.Members, Has.Count.EqualTo(1), "the table copied what it was given");
            Assert.Throws<NotSupportedException>(() => ((IList<KeyValuePair<string, long>>)members.Members)[0] = new("z", 9));
            Assert.That(members.Members[0].Key, Is.EqualTo("a"));
        });
    }

    /// <summary>
    /// An Enum16 declaring more members than a message can list still names the offending label, and says how many
    /// it left out rather than printing thousands.
    /// </summary>
    [Test]
    public void NoSuchLabel_TypeWithManyMembers_ListsTenAndCountsTheRest()
    {
        var members = new EnumMemberTable(
            "Enum16(...)",
            Enumerable.Range(0, 12).Select(i => new KeyValuePair<string, long>($"m{i}", i)).ToArray());

        ArgumentException thrown = members.NoSuchLabel("nope", "label");

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain("'m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7', 'm8', 'm9' and 2 more"));
            Assert.That(thrown.Message, Does.Not.Contain("'m10'"));
            Assert.That(thrown.ParamName, Is.EqualTo("label"));
        });
    }
}
