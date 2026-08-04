using System;
using System.Collections.Generic;
using ClickHouse.Driver.ADO.Parameters;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

public class SqlParameterTypeExtractorTests
{
    [Test]
    public void ExtractTypeHints_SimpleType_ReturnsType()
    {
        var expectedType = "UInt64";
        var sql = $"SELECT {{id:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["id"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_DateTimeWithTimezone_ReturnsFullType()
    {
        var expectedType = "DateTime('Europe/Amsterdam')";
        var sql = $"SELECT {{dt:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["dt"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_DateTime64WithScaleAndTimezone_ReturnsFullType()
    {
        var expectedType = "DateTime64(3, 'UTC')";
        var sql = $"SELECT {{dt:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["dt"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_NestedType_ReturnsFullType()
    {
        var expectedType = "Array(Nullable(String))";
        var sql = $"SELECT {{arr:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["arr"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_EnumWithMultipleValues_ReturnsFullType()
    {
        var expectedType = "Enum8('a' = 1, 'b' = 2)";
        var sql = $"SELECT {{e:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["e"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_MultipleParameters_ReturnsAllTypes()
    {
        var expectedIdType = "UInt64";
        var expectedNameType = "String";
        var expectedDtType = "DateTime('UTC')";
        var sql = $"SELECT {{id:{expectedIdType}}}, {{name:{expectedNameType}}}, {{dt:{expectedDtType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(3));
        Assert.That(hints["id"], Is.EqualTo(expectedIdType));
        Assert.That(hints["name"], Is.EqualTo(expectedNameType));
        Assert.That(hints["dt"], Is.EqualTo(expectedDtType));
    }

    [Test]
    public void ExtractTypeHints_EscapedQuotesInEnum_ReturnsFullType()
    {
        var expectedType = "Enum8('it''s' = 1, 'hello' = 2)";
        var sql = $"SELECT {{e:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["e"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_NullString_ReturnsEmptyDictionary()
    {
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(null);

        Assert.That(hints, Is.Empty);
    }

    [Test]
    public void ExtractTypeHints_EmptyString_ReturnsEmptyDictionary()
    {
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(string.Empty);

        Assert.That(hints, Is.Empty);
    }

    [Test]
    public void ExtractTypeHints_NoParameters_ReturnsEmptyDictionary()
    {
        var sql = "SELECT 1";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Is.Empty);
    }

    [Test]
    public void ExtractTypeHints_ComplexInsertQuery_ReturnsAllTypes()
    {
        var expectedIdType = "UInt64";
        var expectedNameType = "String";
        var expectedCreatedAtType = "DateTime('Europe/London')";
        var expectedTagsType = "Array(String)";
        var sql = $@"INSERT INTO test_table (id, name, created_at, tags)
                    VALUES ({{id:{expectedIdType}}}, {{name:{expectedNameType}}}, {{created_at:{expectedCreatedAtType}}}, {{tags:{expectedTagsType}}})";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(4));
        Assert.That(hints["id"], Is.EqualTo(expectedIdType));
        Assert.That(hints["name"], Is.EqualTo(expectedNameType));
        Assert.That(hints["created_at"], Is.EqualTo(expectedCreatedAtType));
        Assert.That(hints["tags"], Is.EqualTo(expectedTagsType));
    }

    [Test]
    public void ExtractTypeHints_DeeplyNestedType_ReturnsFullType()
    {
        var expectedType = "Array(Tuple(String, Array(Nullable(Int32))))";
        var sql = $"SELECT {{data:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["data"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_MapType_ReturnsFullType()
    {
        var expectedType = "Map(String, Int32)";
        var sql = $"SELECT {{m:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["m"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_BraceInsideQuotedString_ReturnsFullType()
    {
        var expectedType = "Enum8('}' = 1, '{' = 2)";
        var sql = $"SELECT {{e:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["e"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_FullTypeDefinitionInsideQuotedString_IgnoresQuotedString()
    {
        var expectedType = "Enum8('{type:value}' = 1, '{' = 2)";
        var sql = $"SELECT {{e:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["e"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_TrailingSpaceInType_Trimmed()
    {
        var expectedType = "Int32";
        var sql = $"SELECT {{a:{expectedType} }}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["a"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_SpacesAroundColon_Trimmed()
    {
        var expectedType = "Int32";
        var sql = $"SELECT {{a : {expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["a"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_LeadingSpaceInName_Trimmed()
    {
        var expectedType = "Int32";
        var sql = $"SELECT {{ a:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["a"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_UnterminatedParameter_ReturnsEmptyDictionary()
    {
        var sql = "SELECT {id:Int32";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Is.Empty);
    }

    [Test]
    public void ExtractTypeHints_SameParameterWithDifferentTypes_ThrowsArgumentException()
    {
        var sql = "SELECT {val:Int32}, {val:String}";

        var ex = Assert.Throws<ArgumentException>(() => SqlParameterTypeExtractor.ExtractTypeHints(sql));
        Assert.That(ex.Message, Does.Contain("Parameter 'val' has conflicting type hints"));
        Assert.That(ex.Message, Does.Contain("Int32"));
        Assert.That(ex.Message, Does.Contain("String"));
    }

    [Test]
    public void ExtractTypeHints_SameParameterWithSameType_ReturnsType()
    {
        var sql = "SELECT {val:Int32}, {val:Int32}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_ParameterInDoubleDashComment_IgnoresComment()
    {
        var sql = "SELECT {val:Int32} -- {val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_ParameterInDoubleDashCommentNoSpace_IgnoresComment()
    {
        var sql = "SELECT {val:Int32} --{val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_ParameterInHashBangComment_IgnoresComment()
    {
        var sql = "SELECT {val:Int32} #! {val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_ParameterInHashBangCommentNoSpace_IgnoresComment()
    {
        var sql = "SELECT {val:Int32} #!{val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_ParameterInHashComment_IgnoresComment()
    {
        var sql = "SELECT {val:Int32} # {val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_BareHash_NotTreatedAsComment()
    {
        // Only "# " and "#!" start a comment; a bare "#" is not a comment marker for the server either
        var sql = "SELECT {val:Int32} #{other:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(2));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
        Assert.That(hints["other"], Is.EqualTo("String"));
    }

    [Test]
    public void ExtractTypeHints_ParameterInCStyleComment_IgnoresComment()
    {
        var sql = "SELECT {val:Int32} /* {val:String} */";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_ParameterInCStyleCommentNoSpaces_IgnoresComment()
    {
        var sql = "SELECT {val:Int32} /*{val:String}*/";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_ParameterInMultilineCStyleComment_IgnoresComment()
    {
        var sql = @"SELECT {val:Int32}
/*
{val:String}
*/";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_ParameterAfterLineComment_ParsesCorrectly()
    {
        var sql = @"SELECT {val:Int32} -- comment
, {other:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(2));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
        Assert.That(hints["other"], Is.EqualTo("String"));
    }

    [Test]
    public void ExtractTypeHints_ParameterAfterCStyleComment_ParsesCorrectly()
    {
        var sql = "SELECT {val:Int32} /* comment */ , {other:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(2));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
        Assert.That(hints["other"], Is.EqualTo("String"));
    }

    [Test]
    public void ExtractTypeHints_DoubleDashInsideString_NotTreatedAsComment()
    {
        var sql = "SELECT {val:String} WHERE name = '--not a comment' AND {val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("String"));
    }

    [Test]
    public void ExtractTypeHints_HashInsideString_NotTreatedAsComment()
    {
        var sql = "SELECT {val:String} WHERE name = '#not a comment' AND {val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("String"));
    }

    [Test]
    public void ExtractTypeHints_CStyleCommentInsideString_NotTreatedAsComment()
    {
        var sql = "SELECT {val:String} WHERE name = '/* not a comment */' AND {val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("String"));
    }

    [Test]
    public void ExtractTypeHints_UnclosedBlockComment_TreatsRestAsComment()
    {
        var sql = "SELECT {val:Int32} /* {val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_CommentAtStart_IgnoresComment()
    {
        var sql = "-- comment\nSELECT {val:Int32}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_EmptyBlockComment_IgnoresComment()
    {
        var sql = "SELECT /**/{val:Int32}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_BlockCommentMarkersInsideLineComment_IgnoresAll()
    {
        var sql = "SELECT {val:Int32} -- /* {val:String} */";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_CommentAtEndNoNewline_IgnoresComment()
    {
        var sql = "SELECT {val:Int32} -- {val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [Test]
    public void ExtractTypeHints_EscapedQuotesWithCommentMarkers_NotTreatedAsComment()
    {
        var sql = "SELECT 1 WHERE name = 'it''s -- not a comment' AND {val:String}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("String"));
    }

    private static IEnumerable<string> HintInsideCommentOrQuotedToken()
    {
        yield return "SELECT {val:Int32} // {val:String}";
        yield return "SELECT {val:Int32} /* a /* b */ {val:String} */";
        yield return "SELECT {val:Int32} AS `x {val:String}`";
        yield return "SELECT {val:Int32} AS \"x {val:String}\"";
        yield return "SELECT {val:Int32}, $$ {val:String} $$";
        yield return "SELECT {val:Int32}, $tag$ {val:String} $tag$";
        yield return "SELECT {val:Int32}, 'a\\' {val:String} b'";
    }

    [TestCaseSource(nameof(HintInsideCommentOrQuotedToken))]
    public void ExtractTypeHints_HintInsideCommentOrQuotedToken_HintIgnored(string sql)
    {
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    private static IEnumerable<string> CommentOrQuotedTokenPrecedingHint()
    {
        yield return "SELECT 1 // comment\n, {val:Int32}";
        yield return "SELECT 1 AS `a--b`, {val:Int32}";
        yield return "SELECT 1 AS `a\\`--b`, {val:Int32}";
        yield return "SELECT 1 AS `a`` --b`, {val:Int32}";
        yield return "SELECT 1 AS \"a'b\", {val:Int32}";
        yield return "SELECT 1 AS \"a\"\" --b\", {val:Int32}";
        yield return "SELECT $$--$$, {val:Int32}";
        yield return "SELECT $tag$ # $tag$, {val:Int32}";
        yield return "SELECT 'a\\'b', {val:Int32}";
        yield return "SELECT 'a\\\\', {val:Int32}";
    }

    private static IEnumerable<string> DollarSignThatDoesNotOpenAHeredoc()
    {
        // No closing tag
        yield return "SELECT $tag$, {val:Int32}";
        // Tags are empty or ASCII word characters only
        yield return "SELECT $a-b$, {val:Int32}, $a-b$";
        yield return "SELECT $a b$, {val:Int32}, $a b$";
    }

    [TestCaseSource(nameof(DollarSignThatDoesNotOpenAHeredoc))]
    public void ExtractTypeHints_DollarSignNotOpeningHeredoc_HintStillExtracted(string sql)
    {
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    [TestCaseSource(nameof(CommentOrQuotedTokenPrecedingHint))]
    public void ExtractTypeHints_CommentOrQuotedTokenPrecedingHint_HintStillExtracted(string sql)
    {
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }

    private static IEnumerable<TestCaseData> QuotedTokenInsideTypeHint()
    {
        yield return new TestCaseData(@"SELECT {p:Enum8('a\'b' = 1)}", @"Enum8('a\'b' = 1)");
        yield return new TestCaseData("SELECT tupleElement({p:Tuple(`a}b` UInt8)}, 'a}b')", "Tuple(`a}b` UInt8)");
        yield return new TestCaseData("SELECT {p:Tuple(\"a}b\" UInt8)}", "Tuple(\"a}b\" UInt8)");
        yield return new TestCaseData(
            "SELECT tupleElement({p:Tuple(`a``}b` UInt8)}, 'a`}b')",
            "Tuple(`a``}b` UInt8)");
    }

    [TestCaseSource(nameof(QuotedTokenInsideTypeHint))]
    public void ExtractTypeHints_QuotedTokenInsideTypeHint_ReturnsFullType(string sql, string expectedType)
    {
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["p"], Is.EqualTo(expectedType));
    }

    private static IEnumerable<string> UnterminatedQuotedToken()
    {
        // Unterminated quoted identifier before the hint
        yield return "SELECT 1 AS \"a, {val:Int32}";
        // Unterminated quoted identifier inside the type of the hint
        yield return "SELECT {val:Tuple(`a UInt8)}";
    }

    [TestCaseSource(nameof(UnterminatedQuotedToken))]
    public void ExtractTypeHints_UnterminatedQuotedToken_ReturnsEmptyDictionary(string sql)
    {
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Is.Empty);
    }
}
