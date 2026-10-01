using System.Collections.Generic;
using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

[TestFixture]
public class RowLimitAppenderTests
{
    public static IEnumerable<TestCaseData> AppendCases()
    {
        // The clause is orthogonal to trailing-text handling; use "LIMIT 0" unless the case is about the clause.
        yield return new TestCaseData("SELECT 1", "LIMIT 0").Returns("SELECT 1\nLIMIT 0").SetName("plain");
        yield return new TestCaseData("SELECT 1", "LIMIT 1").Returns("SELECT 1\nLIMIT 1").SetName("clause_passthrough");
        yield return new TestCaseData("SELECT 1;", "LIMIT 0").Returns("SELECT 1\nLIMIT 0").SetName("trailing_semicolon_stripped");
        yield return new TestCaseData("SELECT 1 -- note", "LIMIT 0").Returns("SELECT 1 -- note\nLIMIT 0").SetName("line_comment_not_swallowed");
        yield return new TestCaseData("SELECT 1 # note", "LIMIT 0").Returns("SELECT 1 # note\nLIMIT 0").SetName("hash_comment_not_swallowed");
        yield return new TestCaseData("SELECT 1 /* note */", "LIMIT 0").Returns("SELECT 1 /* note */\nLIMIT 0").SetName("block_comment");
        yield return new TestCaseData("SELECT 1 /* a ; b */", "LIMIT 0").Returns("SELECT 1 /* a ; b */\nLIMIT 0").SetName("semicolon_in_block_comment_preserved");
        yield return new TestCaseData("SELECT 1; -- note", "LIMIT 0").Returns("SELECT 1 -- note\nLIMIT 0").SetName("semicolon_then_comment");
        yield return new TestCaseData("SELECT 1; /* note */", "LIMIT 0").Returns("SELECT 1 /* note */\nLIMIT 0").SetName("semicolon_then_block_comment");
        yield return new TestCaseData("SELECT ';' AS a", "LIMIT 0").Returns("SELECT ';' AS a\nLIMIT 0").SetName("semicolon_in_string_preserved");
        yield return new TestCaseData("SELECT '-- x' AS a", "LIMIT 0").Returns("SELECT '-- x' AS a\nLIMIT 0").SetName("comment_marker_in_string_preserved");
        yield return new TestCaseData("SELECT 1 -- a; b", "LIMIT 0").Returns("SELECT 1 -- a; b\nLIMIT 0").SetName("semicolon_in_line_comment_preserved");
        yield return new TestCaseData("SELECT 'a''b;c'", "LIMIT 0").Returns("SELECT 'a''b;c'\nLIMIT 0").SetName("doubled_quote_escape_semicolon_preserved");
        yield return new TestCaseData("SELECT 1; SELECT 2", "LIMIT 0").Returns("SELECT 1; SELECT 2\nLIMIT 0").SetName("non_trailing_semicolon_preserved");
        yield return new TestCaseData("", "LIMIT 0").Returns("\nLIMIT 0").SetName("empty_command_text");

        // The token rules must match the server's, so every comment, quote and heredoc form the
        // server understands is recognized here too (all shapes below are queries the server accepts).
        yield return new TestCaseData("SELECT 1 // note", "LIMIT 0").Returns("SELECT 1 // note\nLIMIT 0").SetName("slash_comment_not_swallowed");
        yield return new TestCaseData("SELECT 1; // note", "LIMIT 0").Returns("SELECT 1 // note\nLIMIT 0").SetName("semicolon_then_slash_comment");
        yield return new TestCaseData("SELECT 1 // a; b", "LIMIT 0").Returns("SELECT 1 // a; b\nLIMIT 0").SetName("semicolon_in_slash_comment_preserved");
        yield return new TestCaseData("SELECT 1; #!note", "LIMIT 0").Returns("SELECT 1 #!note\nLIMIT 0").SetName("semicolon_then_hashbang_comment");
        yield return new TestCaseData("SELECT 1; /* outer /* inner */ */", "LIMIT 0").Returns("SELECT 1 /* outer /* inner */ */\nLIMIT 0").SetName("semicolon_then_nested_block_comment");
        yield return new TestCaseData("SELECT 1 /* a /* ; */ b */", "LIMIT 0").Returns("SELECT 1 /* a /* ; */ b */\nLIMIT 0").SetName("semicolon_in_nested_block_comment_preserved");
        yield return new TestCaseData("SELECT 'a\\'' AS x;", "LIMIT 0").Returns("SELECT 'a\\'' AS x\nLIMIT 0").SetName("backslash_escaped_quote_then_semicolon");
        yield return new TestCaseData("SELECT 'a\\';b'", "LIMIT 0").Returns("SELECT 'a\\';b'\nLIMIT 0").SetName("semicolon_in_backslash_escaped_string_preserved");
        yield return new TestCaseData("SELECT 1 AS \"a#b\";", "LIMIT 0").Returns("SELECT 1 AS \"a#b\"\nLIMIT 0").SetName("quoted_identifier_then_semicolon");
        yield return new TestCaseData("SELECT 1 AS \"a;b\"", "LIMIT 0").Returns("SELECT 1 AS \"a;b\"\nLIMIT 0").SetName("semicolon_in_quoted_identifier_preserved");
        yield return new TestCaseData("SELECT 1 AS `a;b`", "LIMIT 0").Returns("SELECT 1 AS `a;b`\nLIMIT 0").SetName("semicolon_in_backtick_identifier_preserved");
        yield return new TestCaseData("SELECT $$it's$$;", "LIMIT 0").Returns("SELECT $$it's$$\nLIMIT 0").SetName("heredoc_then_semicolon");
        yield return new TestCaseData("SELECT $$a;b$$", "LIMIT 0").Returns("SELECT $$a;b$$\nLIMIT 0").SetName("semicolon_in_heredoc_preserved");
        yield return new TestCaseData("SELECT $t$x$t$;", "LIMIT 0").Returns("SELECT $t$x$t$\nLIMIT 0").SetName("tagged_heredoc_then_semicolon");
        yield return new TestCaseData("SELECT $t$a;b$t$", "LIMIT 0").Returns("SELECT $t$a;b$t$\nLIMIT 0").SetName("semicolon_in_tagged_heredoc_preserved");
        yield return new TestCaseData("SELECT 1; -- note\r\n", "LIMIT 0").Returns("SELECT 1 -- note\r\n\nLIMIT 0").SetName("crlf_line_comment");

        // Contrast: a bare '#' is not a comment marker for the server (it rejects the query), so it is
        // code here as well and cancels the pending terminator instead of hiding it.
        yield return new TestCaseData("SELECT 1; #x", "LIMIT 0").Returns("SELECT 1; #x\nLIMIT 0").SetName("bare_hash_is_code_not_comment");
    }

    [TestCaseSource(nameof(AppendCases))]
    public string Append_TrailingCommentOrTerminator_KeepsLimitClauseEffective(string commandText, string limitClause)
        => RowLimitAppender.Append(commandText, limitClause);
}
