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
        // An opening tag that is never closed does not start a heredoc either: the server lexes it as
        // an ordinary token and keeps substituting parameters after it, so
        // WITH 1 AS `$tag$` SELECT $tag$ AS a, {id:Int32} AS v returns a row
        new TestCaseData("SELECT $$@id", "SELECT $${id:Int32}").SetName("tag never closed is not a heredoc"),
        new TestCaseData("SELECT $tag$@id", "SELECT $tag${id:Int32}").SetName("tagged tag never closed is not a heredoc"),
        new TestCaseData("SELECT $tag$ @id $other$", "SELECT $tag$ {id:Int32} $other$").SetName("tag closed by a different tag is not a heredoc"),
        // ... but @id$other$ names the single parameter id$other$, which is not defined here, so the
        // whole run stays as it is: the server lexes $tag$42$other$ as one unknown identifier
        new TestCaseData("SELECT $tag$@id$other$", "SELECT $tag$@id$other$").SetName("unknown dollar name after a tag closed by a different tag"),
        new TestCaseData("SELECT $tag$ AS a, @id", "SELECT $tag$ AS a, {id:Int32}").SetName("code after a tag that is never closed"),
        new TestCaseData("SELECT $tag$a@id$tag$@id", "SELECT $tag$a@id$tag${id:Int32}").SetName("code directly after a closed heredoc"),
        // The server lexes the whole $-and-word-character run of an unclosed tag as one token, so a
        // later occurrence of a tag spelled inside that run does not close anything either:
        // SELECT $a$x$ , {id:Int32} , $x$ , 1 substitutes the parameter
        new TestCaseData("SELECT $a$x$ @id $x$", "SELECT $a$x$ {id:Int32} $x$").SetName("tag spelled inside an unclosed token is not a heredoc"),
        new TestCaseData("SELECT $a$$x$ @id $x$", "SELECT $a$$x$ {id:Int32} $x$").SetName("doubled dollar inside an unclosed token is not a heredoc"),
        new TestCaseData("SELECT $a$ @id $b$ @id $b$", "SELECT $a$ {id:Int32} $b$ @id $b$").SetName("unclosed tag followed by a real heredoc"),
        new TestCaseData("SELECT $$@id$$@id", "SELECT $$@id$${id:Int32}").SetName("code directly after a closed untagged heredoc"),
        // A $ that continues a token belongs to it and cannot open a heredoc: the server lexes b$c$
        // as a single identifier, so WITH 1 AS b$c$ SELECT {id:Int32}, b$c$ substitutes the parameter
        new TestCaseData("SELECT b$c$, @id, b$c$", "SELECT b$c$, {id:Int32}, b$c$").SetName("dollar inside an identifier is not a heredoc"),
        new TestCaseData("SELECT a1$c$, @id, a1$c$", "SELECT a1$c$, {id:Int32}, a1$c$").SetName("dollar after a digit is not a heredoc"),
        new TestCaseData("SELECT a_$c$, @id, a_$c$", "SELECT a_$c$, {id:Int32}, a_$c$").SetName("dollar after an underscore is not a heredoc"),
        new TestCaseData("SELECT a$$c$ @id $c$", "SELECT a$$c$ {id:Int32} $c$").SetName("dollar after a dollar is not a heredoc"),
        new TestCaseData("WITH 1 AS b$c$ SELECT b$c$, @id, $t$a@id$t$", "WITH 1 AS b$c$ SELECT b$c$, {id:Int32}, $t$a@id$t$").SetName("identifier with a dollar followed by a real heredoc"),
        // A heredoc still opens where a token starts, including at the very beginning of the query
        // and directly after a quoted token
        new TestCaseData("$$a@id$$ @id", "$$a@id$$ {id:Int32}").SetName("heredoc at the start of the query"),
        new TestCaseData("SELECT 1,$t$a@id$t$, @id", "SELECT 1,$t$a@id$t$, {id:Int32}").SetName("heredoc after a comma"),
        new TestCaseData("SELECT\n$t$a@id$t$, @id", "SELECT\n$t$a@id$t$, {id:Int32}").SetName("heredoc after a newline"),
        new TestCaseData("SELECT 1 AS `x`$t$a@id$t$, @id", "SELECT 1 AS `x`$t$a@id$t$, {id:Int32}").SetName("heredoc after a quoted identifier"),
        new TestCaseData("SELECT @id $", "SELECT {id:Int32} $").SetName("trailing dollar sign"),
        new TestCaseData("SELECT @id $tag$", "SELECT {id:Int32} $tag$").SetName("trailing unclosed tag"),
        // A $ continues a placeholder name, matching the server's word lexer and the $ it accepts in
        // a query parameter name, so a placeholder is not recognized inside a longer dollar name
        new TestCaseData("SELECT @id$x", "SELECT @id$x").SetName("name continued by a dollar sign"),
        new TestCaseData("SELECT @id$", "SELECT @id$").SetName("name continued by a trailing dollar sign"),
        new TestCaseData("SELECT @id$x, @id", "SELECT @id$x, {id:Int32}").SetName("unknown dollar name and code position"),
        new TestCaseData("SELECT @id_2$x", "SELECT @id_2$x").SetName("longer name continued by a dollar sign"),
        new TestCaseData("SELECT @id$$x$$", "SELECT @id$$x$$").SetName("name continued by doubled dollar signs"),
        // Contrast: the other characters that continue a name keep behaving as they did
        new TestCaseData("SELECT @id_x", "SELECT @id_x").SetName("name continued by an underscore"),
        new TestCaseData("SELECT @id2", "SELECT @id2").SetName("name continued by a digit"),
        // Unterminated quoted regions and block comments do swallow the rest of the query, so
        // nothing in them is rewritten
        new TestCaseData("SELECT '@id", "SELECT '@id").SetName("unterminated literal"),
        new TestCaseData("SELECT \"@id", "SELECT \"@id").SetName("unterminated double-quoted identifier"),
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

    private static readonly IReadOnlyDictionary<string, string> DollarResolvedTypes =
        new Dictionary<string, string>
        {
            ["id"] = "Int32",
            ["id$x"] = "Int64",
            ["$x"] = "String",
            ["id$"] = "UInt8",
            ["id$$y"] = "Int16",
            ["$1"] = "Date",
        };

    public static IEnumerable<TestCaseData> DollarNamedPlaceholders =>
    [
        // A parameter name may contain a $ anywhere, just like the native {id$x:Int64} syntax
        new TestCaseData("SELECT @id$x", "SELECT {id$x:Int64}").SetName("dollar inside the name"),
        new TestCaseData("SELECT @$x", "SELECT {$x:String}").SetName("name starting with a dollar"),
        new TestCaseData("SELECT @id$", "SELECT {id$:UInt8}").SetName("name ending with a dollar"),
        new TestCaseData("SELECT @id$$y", "SELECT {id$$y:Int16}").SetName("doubled dollar inside the name"),
        new TestCaseData("SELECT @$1", "SELECT {$1:Date}").SetName("name of a dollar and a digit"),
        new TestCaseData("SELECT @id$x AS a, @id AS b", "SELECT {id$x:Int64} AS a, {id:Int32} AS b").SetName("dollar name does not shadow the shorter one"),
        new TestCaseData("SELECT @id, @id$, @$x, @id$x", "SELECT {id:Int32}, {id$:UInt8}, {$x:String}, {id$x:Int64}").SetName("all dollar names"),
        // Still not recognized inside a longer name, whichever character continues it
        new TestCaseData("SELECT @id$x$y", "SELECT @id$x$y").SetName("dollar name continued by a dollar sign"),
        new TestCaseData("SELECT @id$xy", "SELECT @id$xy").SetName("dollar name continued by a letter"),
        new TestCaseData("SELECT @$xy", "SELECT @$xy").SetName("name starting with a dollar continued by a letter"),
        // A dollar name is still only rewritten in a code position
        new TestCaseData("SELECT 'a@id$x', @id$x", "SELECT 'a@id$x', {id$x:Int64}").SetName("dollar name in a string literal"),
        new TestCaseData("SELECT 1 AS `a@$x`, @$x", "SELECT 1 AS `a@$x`, {$x:String}").SetName("dollar name in a quoted identifier"),
        new TestCaseData("SELECT $t$a@id$x$t$, @id$x", "SELECT $t$a@id$x$t$, {id$x:Int64}").SetName("dollar name in a heredoc"),
        new TestCaseData("SELECT @id$ -- @id$x", "SELECT {id$:UInt8} -- @id$x").SetName("dollar name in a line comment"),
        // A name ending with a $ is matched before that $ can be taken for a heredoc opener, and a
        // real heredoc still opens right after it
        new TestCaseData("SELECT @id$ $t$a@id$x$t$, @$x", "SELECT {id$:UInt8} $t$a@id$x$t$, {$x:String}").SetName("dollar name before a heredoc"),
        // The text between a tag and a different tag is code, so a dollar name in it is rewritten
        new TestCaseData("SELECT $tag$@id$ AS a, $other$", "SELECT $tag${id$:UInt8} AS a, $other$").SetName("dollar name after a tag closed by a different tag"),
    ];

    [Test]
    [TestCaseSource(nameof(DollarNamedPlaceholders))]
    public void ReplacePlaceholders_DollarInParameterName_MatchesTheWholeName(string sql, string expected)
    {
        var collection = new ClickHouseParameterCollection
        {
            new ClickHouseDbParameter { ParameterName = "id", ClickHouseType = "Int32", Value = 42 },
            new ClickHouseDbParameter { ParameterName = "id$x", ClickHouseType = "Int64", Value = 43L },
            new ClickHouseDbParameter { ParameterName = "$x", ClickHouseType = "String", Value = "x" },
            new ClickHouseDbParameter { ParameterName = "id$", ClickHouseType = "UInt8", Value = (byte)1 },
            new ClickHouseDbParameter { ParameterName = "id$$y", ClickHouseType = "Int16", Value = (short)2 },
            new ClickHouseDbParameter { ParameterName = "$1", ClickHouseType = "Date", Value = new System.DateTime(2020, 1, 2) },
        };

        Assert.That(collection.ReplacePlaceholders(sql, DollarResolvedTypes), Is.EqualTo(expected));
    }
}
