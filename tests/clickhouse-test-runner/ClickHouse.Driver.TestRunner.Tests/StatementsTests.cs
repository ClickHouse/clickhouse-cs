using NUnit.Framework;

namespace ClickHouse.Driver.TestRunner.Tests;

// Ported from split_test.go of the clickhouse-go harness.
public class StatementsTests
{
    private static readonly object[] SplitCases =
    [
        new object[] { "single without semicolon", "SELECT 1", new[] { "SELECT 1" } },
        new object[] { "several", "SELECT 1;\nSELECT 2;\n", new[] { "SELECT 1;", "SELECT 2;" } },
        new object[] { "semicolon in string", "SELECT 'a;b'; SELECT 2", new[] { "SELECT 'a;b';", "SELECT 2" } },
        new object[] { "escaped quote", "SELECT 'a\\';b'; SELECT 2", new[] { "SELECT 'a\\';b';", "SELECT 2" } },
        new object[] { "doubled quote", "SELECT 'a'';b'; SELECT 2", new[] { "SELECT 'a'';b';", "SELECT 2" } },
        new object[] { "semicolon in identifier", "SELECT 1 AS `a;b`; SELECT 2", new[] { "SELECT 1 AS `a;b`;", "SELECT 2" } },
        new object[] { "semicolon in line comment", "SELECT 1 -- a;b\n; SELECT 2", new[] { "SELECT 1 -- a;b\n;", "SELECT 2" } },
        new object[] { "semicolon in a // comment", "SELECT 1 // a;b\n; SELECT 2", new[] { "SELECT 1 // a;b\n;", "SELECT 2" } },
        new object[] { "same-line // comment belongs to the statement", "SELECT 1; // a\nSELECT 2;", new[] { "SELECT 1;", "SELECT 2;" } },
        new object[] { "semicolon in block comment", "SELECT /* a;b */ 1; SELECT 2", new[] { "SELECT /* a;b */ 1;", "SELECT 2" } },
        new object[] { "nested block comment", "SELECT /* a /* b; */ c; */ 1; SELECT 2", new[] { "SELECT /* a /* b; */ c; */ 1;", "SELECT 2" } },
        new object[] { "heredoc", "SELECT $$a;'b$$; SELECT $x$;$x$", new[] { "SELECT $$a;'b$$;", "SELECT $x$;$x$" } },
        new object[] { "trailing comment only", "SELECT 1;\n-- the end\n", new[] { "SELECT 1;" } },
        new object[] { "empty", "  \n-- only a comment\n", Array.Empty<string>() },
        new object[] { "tags header is skipped", "-- Tags: no-fasttest\n-- Tag no-fasttest: reason\n\nSELECT 1;", new[] { "SELECT 1;" } },
        new object[] { "leading comment stays with its statement", "SELECT 1;\n-- next\nSELECT 2;", new[] { "SELECT 1;", "-- next\nSELECT 2;" } },
        new object[] { "insert data ends at an empty line", "INSERT INTO t FORMAT CSV\n1,'a;b'\n2,c;\n\nSELECT 1;", new[] { "INSERT INTO t FORMAT CSV\n1,'a;b'\n2,c;", "SELECT 1;" } },
        new object[] { "insert values ends at a semicolon", "INSERT INTO t FORMAT Values (1);\nSELECT 1;", new[] { "INSERT INTO t FORMAT Values (1);", "SELECT 1;" } },
        new object[] { "format of a select is not insert data", "SELECT 1 FORMAT TSV;\nSELECT 2;", new[] { "SELECT 1 FORMAT TSV;", "SELECT 2;" } },
        new object[] { "insert select with a column named format", "INSERT INTO t SELECT format FROM s;\nSELECT 1;", new[] { "INSERT INTO t SELECT format FROM s;", "SELECT 1;" } },
        new object[] { "insert select from input has inline data", "INSERT INTO t SELECT x FROM input('x UInt8') FORMAT TSV\n1;\n2\n\nSELECT 1;", new[] { "INSERT INTO t SELECT x FROM input('x UInt8') FORMAT TSV\n1;\n2", "SELECT 1;" } },
        new object[] { "insert select with a format alias", "INSERT INTO t SELECT 1 AS format FROM numbers(1);\nSELECT 1;", new[] { "INSERT INTO t SELECT 1 AS format FROM numbers(1);", "SELECT 1;" } },
        new object[] { "insert select with an implicit alias after format", "INSERT INTO t SELECT format x FROM s;\nSELECT 1;", new[] { "INSERT INTO t SELECT format x FROM s;", "SELECT 1;" } },
        new object[] { "insert select with format DIV", "INSERT INTO t SELECT format DIV 2 FROM s;\nSELECT 1;", new[] { "INSERT INTO t SELECT format DIV 2 FROM s;", "SELECT 1;" } },
        new object[] { "insert select with format GLOBAL IN after FROM", "INSERT INTO t SELECT x FROM s WHERE format GLOBAL IN (1);\nSELECT 1;", new[] { "INSERT INTO t SELECT x FROM s WHERE format GLOBAL IN (1);", "SELECT 1;" } },
        new object[] { "insert with format before the data", "INSERT INTO t (a) SETTINGS async_insert = 0 FORMAT CSV\n1\n\nSELECT 1;", new[] { "INSERT INTO t (a) SETTINGS async_insert = 0 FORMAT CSV\n1", "SELECT 1;" } },
        new object[] { "insert select from a table named format", "INSERT INTO t SELECT * FROM format f;\nSELECT 1;", new[] { "INSERT INTO t SELECT * FROM format f;", "SELECT 1;" } },
        new object[] { "insert into a table named format", "INSERT INTO format FORMAT CSV\n1\n\nSELECT 1;", new[] { "INSERT INTO format FORMAT CSV\n1", "SELECT 1;" } },
        new object[] { "double semicolon", "SELECT 1;;\nSELECT 2", new[] { "SELECT 1;", "SELECT 2" } },
    ];

    [TestCaseSource(nameof(SplitCases))]
    public void Split_Script_GivesTheStatementsOfClickHouseClient(string name, string input, string[] expected)
    {
        Assert.That(Statements.Split(input).Select(s => s.Sql), Is.EqualTo(expected), name);
    }

    [Test]
    public void Split_TrailingHint_BelongsToItsStatement()
    {
        var stmts = Statements.Split("SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }\nSELECT 2;");
        Assert.That(stmts, Has.Count.EqualTo(2));
        Assert.That(stmts[0].Full, Is.EqualTo("SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }"));
        Assert.That(stmts[0].Hint.ServerErrors, Is.EqualTo(new[] { 395 }));
        Assert.That(stmts[1].Hint.ExpectsError, Is.False);
    }

    [Test]
    public void Split_HintOnNextLine_IsLeadingAndIgnored()
    {
        // clickhouse-client takes only the same-line comment as the trailing comment. A hint on the next line
        // is a leading comment of the next statement, and an error hint in a leading comment is ignored.
        var stmts = Statements.Split("SELECT 1;\n-- { serverError 60 }\nSELECT 2;");
        Assert.That(stmts, Has.Count.EqualTo(2));
        Assert.That(stmts[0].Hint.ExpectsError, Is.False);
        Assert.That(stmts[1].Hint.ExpectsError, Is.False);
    }

    [Test]
    public void Split_HintInsideStatement_Counts()
    {
        var stmts = Statements.Split("SELECT x -- { serverError UNKNOWN_IDENTIFIER }\n;");
        Assert.That(stmts, Has.Count.EqualTo(1));
        Assert.That(stmts[0].Hint.ServerErrors, Is.EqualTo(new[] { 47 }));
    }

    [Test]
    public void Split_EchoHints_SetEchoOfTheStatements()
    {
        var stmts = Statements.Split("SELECT 1;\n-- { echoOn }\nSELECT 2; -- two\nSELECT 3;\n-- { echoOff }\nSELECT 4;\nSELECT 5; -- { echo }\n");
        Assert.That(stmts.Select(s => s.Echo), Is.EqualTo(new[] { false, true, true, false, true }));
        Assert.That(stmts[1].Full, Is.EqualTo("-- { echoOn }\nSELECT 2; -- two"));
    }

    [Test]
    public void Split_HintInDoubleSlashComment_Counts()
    {
        // `//` starts a comment in ClickHouse SQL, so the comment after the `;` belongs to the statement.
        var stmts = Statements.Split("SELECT 1; // { serverError 60 }\n// x\nSELECT 2;");
        Assert.That(stmts, Has.Count.EqualTo(2));
        Assert.That(stmts[0].Full, Is.EqualTo("SELECT 1; // { serverError 60 }"));
        Assert.That(stmts[0].Hint.ServerErrors, Is.EqualTo(new[] { 60 }));
        Assert.That(Statements.LeadingKeyword(stmts[1].Sql), Is.EqualTo("SELECT"));
    }

    [Test]
    public void Hint_UnicodeWhitespace_IsLexerWhitespace()
    {
        // Unicode whitespace before a leading comment keeps it leading (its error hint is ignored), and it separates
        // the words of a hint.
        Assert.That(TestHint.Parse("\u00A0-- { serverError 60 }\nSELECT 1").ExpectsError, Is.False);
        Assert.That(TestHint.Parse("SELECT x; -- {serverError\u00A0UNKNOWN_IDENTIFIER}").ServerErrors, Is.EqualTo(new[] { 47 }));
    }

    [Test]
    public void LeadingKeyword_UnicodeWhitespace_IsSkipped()
    {
        // The whitespace of ClickHouse skipWhitespacesUTF8 before the keyword, and inside the parentheses.
        Assert.That(Statements.LeadingKeyword("\u0085\u00A0\u2000\u200A\u2028SELECT\u2029 1"), Is.EqualTo("SELECT"));
        Assert.That(Statements.LeadingKeyword("\uFEFF(\u3000\u180E\u200D\u202F\u205F\u2060select 1)"), Is.EqualTo("SELECT"));
        // U+2010 (a hyphen) is not whitespace for the lexer.
        Assert.That(Statements.LeadingKeyword("\u2010SELECT 1"), Is.EqualTo(string.Empty));
    }

    [Test]
    public void LeadingKeyword_CommentsAndParentheses_AreSkipped()
    {
        Assert.That(Statements.LeadingKeyword("  -- c\n/* d */ (select 1)"), Is.EqualTo("SELECT"));
        Assert.That(Statements.LeadingKeyword("// c\nselect 1"), Is.EqualTo("SELECT"));
        Assert.That(Statements.LeadingKeyword("insert into t values (1)"), Is.EqualTo("INSERT"));
        Assert.That(Statements.LeadingKeyword("-- only a comment"), Is.EqualTo(string.Empty));
    }

    [TestCase("SELECT {a:UInt8}, { b : String }, {a:UInt16}", new[] { "a", "b" })]
    [TestCase("SELECT '{a:String}', `{b:String}`, \"{c:String}\", $$ {d:String} $$, $t${e:String}$t$", new string[0])]
    [TestCase("-- {a:String}\nSELECT 1 # {b:String}\n/* {c:String} /* {d:String} */ */", new string[0])]
    [TestCase("SELECT 'it''s {a:String}', {b:Identifier} -- {c:String}", new[] { "b" })]
    [TestCase("INSERT INTO t VALUES (1, {1 : 'a'}), (2, {_x : 'b'})", new[] { "_x" })]
    [TestCase("SELECT {a}, {1a:String}, {a b:String}, {:String}", new string[0])]
    public void ParameterNames_Sql_GivesThePlaceholdersInCode(string sql, string[] expected) =>
        Assert.That(Statements.ParameterNames(sql), Is.EquivalentTo(expected));

    [TestCase("SELECT a, 'b c', `d`, \"e\", $$f g$$, $t$h$t$ -- i\nFROM (SELECT j) /* k */", false, "SELECT a FROM")]
    [TestCase("INSERT INTO t SELECT $$x FROM y$$ FROM (SELECT z)", true, "INSERT INTO t SELECT FROM SELECT z")]
    public void TopLevelWords_Sql_SkipsLiteralsAndComments(string sql, bool nested, string expected) =>
        Assert.That(string.Join(' ', Statements.TopLevelWords(sql, nested)), Is.EqualTo(expected));

    [Test]
    public void Split_Statement_KeepsItsSemicolon()
    {
        // clickhouse-client sends the semicolon; system.query_log shows it.
        var stmts = Statements.Split("SELECT 1; -- c\nSELECT 2\n");
        Assert.That(stmts.Select(s => s.Sql), Is.EqualTo(new[] { "SELECT 1;", "SELECT 2" }));
    }

    [Test]
    public void Split_LastStatementWithoutSemicolon_EchoesTheRestOfTheText()
    {
        // clickhouse-client echoes the rest of the text, with the newlines.
        var stmts = Statements.Split("SELECT 1;\nSELECT 2\n\n");
        Assert.That(stmts[0].Full, Is.EqualTo("SELECT 1;"));
        Assert.That(stmts[1].Full, Is.EqualTo("SELECT 2\n\n"));
        Assert.That(stmts[1].Sql, Is.EqualTo("SELECT 2"));
    }
}
