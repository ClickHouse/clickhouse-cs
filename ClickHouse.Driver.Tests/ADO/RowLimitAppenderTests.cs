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
    }

    [TestCaseSource(nameof(AppendCases))]
    public string Append_TrailingCommentOrTerminator_KeepsLimitClauseEffective(string commandText, string limitClause)
        => RowLimitAppender.Append(commandText, limitClause);
}
