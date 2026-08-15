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
    [TestCase("SELECT {val:Int32} #{val:String}")]
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
}
