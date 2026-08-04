using System.Collections.Generic;
using ClickHouse.Driver.ADO.Parameters;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

public class SqlPlaceholderRewriteTests
{
    private static readonly IReadOnlyDictionary<string, string> ResolvedTypes =
        new Dictionary<string, string> { ["id"] = "Int32", ["id_2"] = "String" };

    public static IEnumerable<TestCaseData> Placeholders =>
    [
        // Code positions are rewritten
        new TestCaseData("SELECT @id", "SELECT {id:Int32}").SetName("code position"),
        new TestCaseData("SELECT @id, @id_2, @id", "SELECT {id:Int32}, {id_2:String}, {id:Int32}").SetName("several placeholders"),
        new TestCaseData("SELECT @id_2", "SELECT {id_2:String}").SetName("longer name is not shadowed by shorter one"),
        new TestCaseData("SELECT @idx", "SELECT @idx").SetName("longer unknown identifier"),
        new TestCaseData("SELECT @idä", "SELECT @idä").SetName("longer unknown identifier with non-ascii character"),
        new TestCaseData("SELECT 1 @", "SELECT 1 @").SetName("lone at sign"),
        new TestCaseData("SELECT @id+1", "SELECT {id:Int32}+1").SetName("followed by operator"),
        // Non-code positions are left alone
        new TestCaseData("SELECT 'user@id'", "SELECT 'user@id'").SetName("string literal"),
        new TestCaseData("SELECT 'user@id', @id", "SELECT 'user@id', {id:Int32}").SetName("string literal and code position"),
        new TestCaseData("SELECT '%@id%' , @id", "SELECT '%@id%' , {id:Int32}").SetName("like pattern"),
        new TestCaseData("SELECT 'it''s @id', @id", "SELECT 'it''s @id', {id:Int32}").SetName("literal with doubled quote"),
        new TestCaseData(@"SELECT 'it\'s @id', @id", @"SELECT 'it\'s @id', {id:Int32}").SetName("literal with backslash-escaped quote"),
        new TestCaseData(@"SELECT 'a\\' , @id", @"SELECT 'a\\' , {id:Int32}").SetName("literal ending with escaped backslash"),
        new TestCaseData("SELECT 1 AS \"a@id\", @id", "SELECT 1 AS \"a@id\", {id:Int32}").SetName("double-quoted identifier"),
        new TestCaseData("SELECT 1 AS `a@id`, @id", "SELECT 1 AS `a@id`, {id:Int32}").SetName("backtick identifier"),
        new TestCaseData("SELECT $$a@id$$, @id", "SELECT $$a@id$$, {id:Int32}").SetName("heredoc"),
        new TestCaseData("SELECT $tag$a@id$tag$, @id", "SELECT $tag$a@id$tag$, {id:Int32}").SetName("tagged heredoc"),
        new TestCaseData("SELECT @id -- see @id", "SELECT {id:Int32} -- see @id").SetName("line comment"),
        new TestCaseData("SELECT @id # see @id", "SELECT {id:Int32} # see @id").SetName("hash comment"),
        new TestCaseData("SELECT /* @id */ @id", "SELECT /* @id */ {id:Int32}").SetName("block comment"),
        new TestCaseData("SELECT /* /* @id */ @id */ @id", "SELECT /* /* @id */ @id */ {id:Int32}").SetName("nested block comment"),
        new TestCaseData("SELECT /* /* @id */ @id", "SELECT /* /* @id */ @id").SetName("unterminated nested block comment"),
        new TestCaseData("SELECT @id #! see @id", "SELECT {id:Int32} #! see @id").SetName("shebang comment"),
        new TestCaseData("SELECT /**/@id", "SELECT /**/{id:Int32}").SetName("empty block comment"),
        new TestCaseData("SELECT @id // see @id", "SELECT {id:Int32} // see @id").SetName("double-slash line comment"),
        new TestCaseData("SELECT @id //@id", "SELECT {id:Int32} //@id").SetName("double-slash line comment without newline"),
        new TestCaseData("SELECT // @id\n@id", "SELECT // @id\n{id:Int32}").SetName("code after double-slash line comment"),
        new TestCaseData("SELECT ''@id", "SELECT ''{id:Int32}").SetName("empty literal followed by code position"),
        new TestCaseData("SELECT 1 AS \"a\\\"@id\", @id", "SELECT 1 AS \"a\\\"@id\", {id:Int32}").SetName("double-quoted identifier with escaped quote"),
        new TestCaseData("SELECT -- @id\n@id", "SELECT -- @id\n{id:Int32}").SetName("code after line comment"),
        // Only "# " and "#!" start a comment; the server rejects a bare "#x" as an unrecognized
        // token, so the text after it is code and its placeholders must still be rewritten
        new TestCaseData("SELECT @id, #x @id", "SELECT {id:Int32}, #x {id:Int32}").SetName("bare hash is not a comment"),
        new TestCaseData("SELECT @id, #\t@id", "SELECT {id:Int32}, #\t{id:Int32}").SetName("hash followed by tab is not a comment"),
        new TestCaseData("SELECT @id #", "SELECT {id:Int32} #").SetName("trailing hash"),
        // A single / is not a comment either
        new TestCaseData("SELECT 1 / @id", "SELECT 1 / {id:Int32}").SetName("single slash is not a comment"),
        new TestCaseData("SELECT @id /", "SELECT {id:Int32} /").SetName("trailing slash"),
        // A lone $ is not a heredoc and must not swallow the rest of the query
        new TestCaseData("SELECT $1 = @id", "SELECT $1 = {id:Int32}").SetName("dollar sign is not a heredoc"),
        new TestCaseData("SELECT $tag @id", "SELECT $tag {id:Int32}").SetName("unclosed heredoc tag is not a heredoc"),
        // Unterminated regions swallow the rest of the query, so nothing in them is rewritten
        new TestCaseData("SELECT '@id", "SELECT '@id").SetName("unterminated literal"),
        new TestCaseData("SELECT \"@id", "SELECT \"@id").SetName("unterminated double-quoted identifier"),
        new TestCaseData("SELECT $$@id", "SELECT $$@id").SetName("unterminated heredoc"),
        new TestCaseData("SELECT $tag$@id", "SELECT $tag$@id").SetName("unterminated tagged heredoc"),
        new TestCaseData("SELECT $tag$@id$other$", "SELECT $tag$@id$other$").SetName("heredoc closed by a different tag"),
        new TestCaseData("SELECT /* @id", "SELECT /* @id").SetName("unterminated block comment"),
    ];

    [Test]
    [TestCaseSource(nameof(Placeholders))]
    public void ReplacePlaceholders_PlaceholderInGivenContext_RewritesOnlyCodePositions(string sql, string expected)
    {
        var collection = new ClickHouseParameterCollection
        {
            new ClickHouseDbParameter { ParameterName = "id", ClickHouseType = "Int32", Value = 42 },
            new ClickHouseDbParameter { ParameterName = "id_2", ClickHouseType = "String", Value = "x" },
        };

        Assert.That(collection.ReplacePlaceholders(sql, ResolvedTypes), Is.EqualTo(expected));
    }
}
