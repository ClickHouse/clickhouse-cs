using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Parameters;

namespace ClickHouse.Driver.Tcp.Tests.Parameters;

[TestFixture]
public class SqlParameterTypeExtractorTests
{
    [TestCase("UInt64")]
    [TestCase("String")]
    [TestCase("DateTime('Europe/Amsterdam')")]
    [TestCase("DateTime64(3, 'UTC')")]
    [TestCase("Array(Nullable(String))")]
    [TestCase("Array(Tuple(String, Array(Nullable(Int32))))")]
    [TestCase("Map(String, Int32)")]
    [TestCase("Enum8('a' = 1, 'b' = 2)")]
    [TestCase("Enum8('it''s' = 1, 'hello' = 2)")]
    [TestCase("Enum8('}' = 1, '{' = 2)")]
    [TestCase("Enum8('{type:value}' = 1, '{' = 2)")]
    public void ExtractTypeHints_SingleParameter_ReturnsWholeType(string type)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints("SELECT {p:" + type + "}");

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["p"], Is.EqualTo(type));
        });
    }

    // Backslash-escaped quotes must not hide a later placeholder.
    [TestCase(@"SELECT 'it\'s', {p:Int32}", TestName = "A backslash-escaped quote in a literal")]
    [TestCase(@"SELECT 'ends with a backslash\\', {p:Int32}", TestName = "An escaped backslash at the end of a literal")]
    [TestCase(@"SELECT 'a\'b\'c', {p:Int32}", TestName = "Several escaped quotes")]
    [TestCase(@"SELECT 'brace \' {p:Wrong}', {p:Int32}", TestName = "A decoy placeholder inside the literal")]
    public void ExtractTypeHints_LiteralWithABackslashEscape_StillFindsTheHintAfterIt(string sql)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints["p"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_EnumLabelWithABackslashEscapedQuote_KeepsTheWholeType()
    {
        const string type = @"Enum8('it\'s' = 1, 'b' = 2)";

        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints("SELECT {p:" + type + "}");

        Assert.That(hints["p"], Is.EqualTo(type));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("SELECT 1")]
    [TestCase("SELECT {id}")]
    [TestCase("SELECT {id:}")]
    [TestCase("SELECT {id: }")]
    [TestCase("SELECT {:Int32}")]
    [TestCase("SELECT { :Int32}")]
    [TestCase("SELECT {id:Int32")]
    public void ExtractTypeHints_NoUsableHint_ReturnsEmptyDictionary(string sql)
    {
        Assert.That(SqlParameterTypeExtractor.ExtractTypeHints(sql), Is.Empty);
    }

    [TestCase("SELECT {a:Int32 }")]
    [TestCase("SELECT {a : Int32}")]
    [TestCase("SELECT { a:Int32}")]
    [TestCase("SELECT {  a  :  Int32  }")]
    public void ExtractTypeHints_WhitespaceAroundNameAndType_TrimsBoth(string sql)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["a"], Is.EqualTo("Int32"));
        });
    }

    [Test]
    public void ExtractTypeHints_MultipleParameters_ReturnsAllTypes()
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(
            "SELECT {id:UInt64}, {name:String}, {dt:DateTime('UTC')}");

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(3));
            Assert.That(hints["id"], Is.EqualTo("UInt64"));
            Assert.That(hints["name"], Is.EqualTo("String"));
            Assert.That(hints["dt"], Is.EqualTo("DateTime('UTC')"));
        });
    }

    [Test]
    public void ExtractTypeHints_MultilineInsertQuery_ReturnsAllTypes()
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(
            "INSERT INTO test_table (id, name, created_at, tags)\n" +
            "VALUES ({id:UInt64}, {name:String}, {created_at:DateTime('Europe/London')}, {tags:Array(String)})");

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(4));
            Assert.That(hints["id"], Is.EqualTo("UInt64"));
            Assert.That(hints["name"], Is.EqualTo("String"));
            Assert.That(hints["created_at"], Is.EqualTo("DateTime('Europe/London')"));
            Assert.That(hints["tags"], Is.EqualTo("Array(String)"));
        });
    }

    [Test]
    public void ExtractTypeHints_SameParameterWithSameType_ReturnsOneHint()
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints("SELECT {val:Int32}, {val:Int32}");

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["val"], Is.EqualTo("Int32"));
        });
    }

    [Test]
    public void ExtractTypeHints_SameParameterWithDifferentTypes_ThrowsArgumentException()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => SqlParameterTypeExtractor.ExtractTypeHints("SELECT {val:Int32}, {val:String}"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("Parameter 'val' has conflicting type hints"));
            Assert.That(ex.Message, Does.Contain("Int32"));
            Assert.That(ex.Message, Does.Contain("String"));
        });
    }

    // A commented-out hint must not override, or conflict with, the real one.
    [TestCase("SELECT {val:Int32} -- {val:String}")]
    [TestCase("SELECT {val:Int32} --{val:String}")]
    [TestCase("SELECT {val:Int32} # {val:String}")]
    [TestCase("SELECT {val:Int32} #! {val:String}")]
    [TestCase("SELECT {val:Int32} #!{val:String}")]
    [TestCase("SELECT {val:Int32} -- /* {val:String} */")]
    [TestCase("SELECT {val:Int32} /* {val:String} */")]
    [TestCase("SELECT {val:Int32} /*{val:String}*/")]
    [TestCase("SELECT {val:Int32}\n/*\n{val:String}\n*/")]
    [TestCase("SELECT {val:Int32} /* {val:String}")]
    public void ExtractTypeHints_ParameterInsideComment_IgnoresCommentedParameter(string sql)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["val"], Is.EqualTo("Int32"));
        });
    }

    [TestCase("-- comment\nSELECT {val:Int32}")]
    [TestCase("# comment\nSELECT {val:Int32}")]
    [TestCase("SELECT /**/{val:Int32}")]
    [TestCase("SELECT /* comment */ {val:Int32}")]
    public void ExtractTypeHints_CommentBeforeParameter_ParsesParameter(string sql)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["val"], Is.EqualTo("Int32"));
        });
    }

    [TestCase("SELECT {val:Int32} -- comment\n, {other:String}")]
    [TestCase("SELECT {val:Int32} /* comment */ , {other:String}")]
    public void ExtractTypeHints_ParameterAfterComment_ParsesBothParameters(string sql)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(2));
            Assert.That(hints["val"], Is.EqualTo("Int32"));
            Assert.That(hints["other"], Is.EqualTo("String"));
        });
    }

    [TestCase("-- {val:Int32}")]
    [TestCase("# {val:Int32}")]
    [TestCase("/* {val:Int32} */")]
    [TestCase("/* {val:Int32}")]
    public void ExtractTypeHints_OnlyCommentedParameter_ReturnsEmptyDictionary(string sql)
    {
        Assert.That(SqlParameterTypeExtractor.ExtractTypeHints(sql), Is.Empty);
    }

    // A comment marker inside a string literal starts no comment, so the parameter after it stays visible.
    [TestCase("SELECT {val:String} WHERE name = '--not a comment' AND {other:String}")]
    [TestCase("SELECT {val:String} WHERE name = '#not a comment' AND {other:String}")]
    [TestCase("SELECT {val:String} WHERE name = '/* not a comment' AND {other:String}")]
    [TestCase("SELECT {val:String} WHERE name = '/* not a comment */' AND {other:String}")]
    [TestCase("SELECT {val:String} WHERE name = 'it''s -- not a comment' AND {other:String}")]
    public void ExtractTypeHints_CommentMarkerInsideStringLiteral_NotTreatedAsComment(string sql)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(2));
            Assert.That(hints["val"], Is.EqualTo("String"));
            Assert.That(hints["other"], Is.EqualTo("String"));
        });
    }

    [Test]
    public void ExtractTypeHints_ParameterInsideStringLiteral_IsSkipped()
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints("SELECT '{val:Int32}', {other:String}");

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["other"], Is.EqualTo("String"));
        });
    }

    [Test]
    public void ExtractTypeHints_BareHash_NotTreatedAsComment()
    {
        // Only "# " and "#!" start a comment; the server rejects a query holding a bare "#" token.
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints("SELECT {val:Int32} #{other:String}");

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(2));
            Assert.That(hints["val"], Is.EqualTo("Int32"));
            Assert.That(hints["other"], Is.EqualTo("String"));
        });
    }

    // A brace that is not a type hint must not reach past its own name for a colon, or it takes the colon of
    // the parameter after it and that parameter loses its hint. Every one of these is a query the server runs.
    [TestCase("SELECT 1 AS \"col{x}\", {p:Int32}")]
    [TestCase("SELECT 1 AS `col{x}`, {p:Int32}")]
    [TestCase("SELECT $$ {x} $$, {p:Int32}")]
    public void ExtractTypeHints_BraceThatIsNotATypeHint_KeepsTheLaterHint(string sql)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["p"], Is.EqualTo("Int32"));
        });
    }

    // A name is a bare word, so anything else before the colon means this brace is not a placeholder at all.
    [TestCase("SELECT {x}, {p:Int32}")]
    [TestCase("SELECT {}, {p:Int32}")]
    [TestCase("SELECT {a b:Int32}, {p:Int32}")]
    [TestCase("SELECT {'a':1}, {p:Int32}")]
    public void ExtractTypeHints_BraceWithoutAParameterName_YieldsOnlyTheRealHint(string sql)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["p"], Is.EqualTo("Int32"));
        });
    }

    // A quoted identifier, a heredoc and a // comment each hide their contents, as a string literal does.
    [TestCase("SELECT 1 AS \"a{val:Int32}b\", {other:String}")]
    [TestCase("SELECT 1 AS `a{val:Int32}b`, {other:String}")]
    [TestCase("SELECT $tag$ {val:Int32} $tag$, {other:String}")]
    [TestCase("SELECT 1 // {val:Int32}\n, {other:String}")]
    [TestCase("SELECT /* /* {val:Int32} */ */ {other:String}")]
    public void ExtractTypeHints_HintInsideAHiddenToken_IsSkipped(string sql)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["other"], Is.EqualTo("String"));
        });
    }

    // A $ opens a heredoc only at a token boundary and only when a closing tag follows. The server lexes
    // a$b$ as one identifier, so neither dollar there starts one even though the tag repeats later.
    [TestCase("SELECT $x, {p:Int32}")]
    [TestCase("SELECT 1 AS a$b$, {p:Int32}, 2 AS c$b$")]
    public void ExtractTypeHints_DollarThatOpensNoHeredoc_IsAnOrdinaryCharacter(string sql)
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["p"], Is.EqualTo("Int32"));
        });
    }

    // Shapes that end the scan without a hint: no colon before the end, an unterminated quote, and a brace
    // inside what would be the type, which no type definition contains.
    [TestCase("SELECT {abc", TestName = "name runs to the end with no colon")]
    [TestCase("SELECT 'abc", TestName = "unterminated string literal")]
    [TestCase("SELECT \"abc", TestName = "unterminated quoted identifier")]
    [TestCase("SELECT {p:Int{32}", TestName = "brace inside the type")]
    public void ExtractTypeHints_ScanThatNeverCompletesAHint_ReturnsEmptyDictionary(string sql)
    {
        Assert.That(SqlParameterTypeExtractor.ExtractTypeHints(sql), Is.Empty);
    }

    [Test]
    public void ExtractTypeHints_BraceInsideTheType_LeavesALaterHintVisible()
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints("SELECT {p:Int{32}, {q:Int32}");

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["q"], Is.EqualTo("Int32"));
        });
    }

    [Test]
    public void ExtractTypeHints_UnterminatedHeredoc_DoesNotSwallowTheRestOfTheQuery()
    {
        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints("SELECT $tag$ oops, {p:Int32}");

        Assert.Multiple(() =>
        {
            Assert.That(hints, Has.Count.EqualTo(1));
            Assert.That(hints["p"], Is.EqualTo("Int32"));
        });
    }
}
