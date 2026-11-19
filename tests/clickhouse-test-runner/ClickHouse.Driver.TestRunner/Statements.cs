namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// One query of a multi-query script, split the way <c>clickhouse-client</c> splits it
/// (see <c>ClientBase::executeMultiQuery</c>).
/// </summary>
internal sealed class Statement
{
    public Statement(string sql, string full)
    {
        Sql = sql;
        Full = full;
        Hint = TestHint.Parse(full);
    }

    /// <summary>
    /// Gets the text to send to the server: from the first non-whitespace character (leading comments
    /// included) to the end of the query, with the semicolon.
    /// </summary>
    public string Sql { get; }

    /// <summary>
    /// Gets <see cref="Sql"/> plus the semicolon and the same-line trailing comment. The client reads the test
    /// hints from this text and prints it for <c>echo</c>.
    /// </summary>
    public string Full { get; }

    public TestHint Hint { get; }

    /// <summary>Gets or sets a value indicating whether the statement is printed before it runs.</summary>
    public bool Echo { get; set; }
}

/// <summary>Splits a script into statements and finds the parts of a statement.</summary>
internal static class Statements
{
    // SQL words that can follow a column or an alias named `format`, as in `INSERT INTO t SELECT format
    // FROM s`. No input or output format has such a name, so after FORMAT they show an identifier, not the
    // FORMAT clause.
    private static readonly HashSet<string> ClauseKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "FROM", "WHERE", "PREWHERE", "AS", "AND", "OR", "NOT", "IN", "IS", "LIKE", "ILIKE", "BETWEEN",
        "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "QUALIFY", "WINDOW", "UNION", "EXCEPT", "INTERSECT",
        "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "ARRAY", "ON", "USING", "SETTINGS", "INTO",
        "ASC", "DESC", "NULLS", "COLLATE", "WITH", "THEN", "ELSE", "END", "WHEN", "OVER",
        "GLOBAL", "DIV", "MOD", "REGEXP", "FINAL", "SAMPLE", "FILL", "TIES", "BY",
    };

    // SQL keywords that an operand follows (see OperandExpected).
    private static readonly HashSet<string> OperandKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT", "DISTINCT", "FROM", "JOIN", "WHERE", "PREWHERE", "BY", "ON", "AS", "AND", "OR", "NOT", "IN",
        "IS", "LIKE", "ILIKE", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "LIMIT", "OFFSET", "HAVING", "QUALIFY",
        "USING", "INTO", "WITH", "DIV", "MOD",
    };

    /// <summary>
    /// Splits a script into statements, as <c>clickhouse-client</c> does.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    /// <item>The <c>-- Tags:</c> header (the first line and the <c>--</c> lines after it) is not part of any
    /// statement.</item>
    /// <item>A statement ends at a top-level <c>;</c>. A comment on the same line after the <c>;</c> belongs to
    /// that statement; this is where test hints are.</item>
    /// <item>The inline data of <c>INSERT ... FORMAT &lt;name&gt;</c> (other than <c>Values</c>) ends at an empty
    /// line, not at <c>;</c>.</item>
    /// <item>Comment-only text after the last statement is not a statement.</item>
    /// </list>
    /// </remarks>
    public static List<Statement> Split(string text)
    {
        var output = new List<Statement>();
        var pos = TestTagsLength(text);
        var echo = false;
        while (true)
        {
            pos = SkipWhitespace(text, pos);
            if (pos >= text.Length || !HasSignificantToken(text, pos))
            {
                return output;
            }
            var begin = pos;
            var end = ScanStatementEnd(text, pos);
            var sqlEnd = end;
            // Consume the semicolons after the statement.
            pos = end;
            while (pos < text.Length && text[pos] == ';')
            {
                pos++;
                var next = SkipWhitespaceAndComments(text, pos);
                if (next < text.Length && text[next] == ';')
                {
                    pos = next;
                }
            }
            // Add the same-line trailing comment when the next statement starts after the end of this line.
            var newline = text.IndexOf('\n', pos);
            if (newline < 0)
            {
                newline = text.Length;
            }
            if (newline <= SkipWhitespaceAndComments(text, pos))
            {
                pos = newline;
            }
            // clickhouse-client sends the statement with its semicolon. Tests that read system.query_log
            // see it.
            var sql = text[begin..sqlEnd].TrimEnd(' ', '\t', '\r', '\n');
            if (sqlEnd < text.Length && text[sqlEnd] == ';')
            {
                sql = text[begin..(sqlEnd + 1)];
            }
            // The last statement has no semicolon: clickhouse-client echoes all the rest of the text, with its
            // trailing newlines.
            var full = sqlEnd == text.Length ? text[begin..] : text[begin..pos].TrimEnd(' ', '\t', '\r', '\n');
            var st = new Statement(sql, full);
            if (st.Hint.Echo is { } e)
            {
                echo = e;
            }
            st.Echo = echo;
            output.Add(st);
        }
    }

    /// <summary>
    /// Returns the first word of a statement in upper case, after comments, whitespace and opening
    /// parentheses.
    /// </summary>
    public static string LeadingKeyword(string sql)
    {
        var i = SkipWhitespaceAndComments(sql, 0);
        while (i < sql.Length && (sql[i] == '(' || IsLexerSpace(sql[i])))
        {
            i = SkipWhitespaceAndComments(sql, i + 1);
        }
        var j = i;
        while (j < sql.Length && ChType.IsWordChar(sql[j]))
        {
            j++;
        }
        return sql[i..j].ToUpperInvariant();
    }

    public static bool IsClauseKeyword(string word) => ClauseKeywords.Contains(word);

    /// <summary>
    /// Tells if the SQL parser expects an operand (a column, a table, an expression) at <paramref name="pos"/>
    /// of the statement that begins at <paramref name="start"/>. There, <c>format</c> is a name, not the
    /// <c>FORMAT</c> clause: <c>SELECT * FROM format f</c>, <c>SELECT format TSV</c> and <c>SELECT 2 * format x</c>
    /// read a table or a column <c>format</c> with an implicit alias.
    /// </summary>
    /// <remarks>
    /// An operand is expected at the start of a statement or a parenthesis, and after a keyword of
    /// <see cref="OperandKeywords"/>, a comma, a dot or an operator. A keyword where an operand is expected is a name (<c>AS mod</c>,
    /// <c>ORDER BY offset</c>), so an operand follows it. <c>*</c> after an operand is the multiplication,
    /// otherwise the asterisk (an operand).
    /// </remarks>
    public static bool OperandExpected(string text, int start, int pos)
    {
        // At the start of the statement, a keyword is a keyword.
        bool atStart = true, expect = false;
        for (var i = start; i < pos;)
        {
            var c = text[i];
            if (IsLexerSpace(c))
            {
                i++;
                continue;
            }
            if (StartsLineComment(text, i))
            {
                i = SkipLineComment(text, i);
                continue;
            }
            if (StartsBlockComment(text, i))
            {
                i = SkipBlockComment(text, i);
                continue;
            }
            var wasStart = atStart;
            atStart = false;
            if (c is '\'' or '"' or '`')
            {
                i = SkipQuoted(text, i);
                expect = false;
            }
            else if (c == '$' && HeredocEnd(text, i) is var e && e > 0)
            {
                i = e;
                expect = false;
            }
            else if (ChType.IsWordChar(c) || c == '$')
            {
                // A `$` that does not start a heredoc is a part of an identifier: `$alias$name$`.
                var j = i;
                while (j < text.Length && (ChType.IsWordChar(text[j]) || text[j] == '$'))
                {
                    j++;
                }
                expect = OperandKeywords.Contains(text[i..j]) && (wasStart || !expect);
                i = j;
            }
            else if (c is ')' or ']' or '}')
            {
                // The end of a parenthesis, an array, or a query parameter `{p:String}`.
                i++;
                expect = false;
            }
            else if (c == '*')
            {
                i++;
                expect = !wasStart && !expect;
            }
            else if (c is ';' or '(')
            {
                // A statement, or a subquery: `(SELECT ...`.
                i++;
                atStart = true;
                expect = false;
            }
            else
            {
                // After `[`, `{`, `,`, `.` and an operator, an operand follows. Any other character ends an
                // operand, so that FORMAT after it stays the clause.
                i++;
                expect = c is '[' or '{' or ',' or '.' or '+' or '-' or '/' or '%' or '=' or '<' or '>' or '!'
                    or '|' or '&' or '^' or '~' or '?' or ':';
            }
        }
        return atStart || expect;
    }

    public static bool IsSpace(char c) => c is ' ' or '\t' or '\n' or '\r' or '\f' or '\v';

    /// <summary>
    /// Tells if the ClickHouse lexer skips the character as whitespace: the ASCII whitespace and the Unicode
    /// whitespace of <c>skipWhitespacesUTF8</c> (U+0085, U+00A0, U+180E, U+2000..U+200D, U+2028, U+2029, U+202F,
    /// U+205F, U+2060, U+3000, U+FEFF).
    /// </summary>
    public static bool IsLexerSpace(char c) =>
        IsSpace(c) || c is '\u0085' or '\u00A0' or '\u180E' or (>= '\u2000' and <= '\u200D') or '\u2028'
            or '\u2029' or '\u202F' or '\u205F' or '\u2060' or '\u3000' or '\uFEFF';

    public static int SkipQuoted(string text, int i)
    {
        var q = text[i];
        for (var j = i + 1; j < text.Length; j++)
        {
            if (text[j] == '\\')
            {
                j++;
            }
            else if (text[j] == q)
            {
                // A doubled quote is an escaped quote.
                if (j + 1 < text.Length && text[j + 1] == q)
                {
                    j++;
                    continue;
                }
                return j + 1;
            }
        }
        return text.Length;
    }

    public static int SkipLineComment(string text, int i)
    {
        var nl = text.IndexOf('\n', i);
        return nl >= 0 ? nl : text.Length;
    }

    public static int SkipBlockComment(string text, int i)
    {
        // Block comments nest in ClickHouse.
        var depth = 0;
        for (var j = i; j < text.Length - 1;)
        {
            if (text[j] == '/' && text[j + 1] == '*')
            {
                depth++;
                j += 2;
            }
            else if (text[j] == '*' && text[j + 1] == '/')
            {
                depth--;
                j += 2;
                if (depth == 0)
                {
                    return j;
                }
            }
            else
            {
                j++;
            }
        }
        return text.Length;
    }

    /// <summary>
    /// Returns the end offset of the <c>$tag$ ... $tag$</c> heredoc that starts at <paramref name="i"/>, or 0
    /// when the <c>$</c> at <paramref name="i"/> does not start one.
    /// </summary>
    public static int HeredocEnd(string text, int i)
    {
        var tagEnd = text.IndexOf('$', i + 1);
        if (tagEnd < 0)
        {
            return 0;
        }
        for (var k = i + 1; k < tagEnd; k++)
        {
            if (!ChType.IsWordChar(text[k]))
            {
                return 0;
            }
        }
        var delim = text[i..(tagEnd + 1)];
        var end = text.IndexOf(delim, tagEnd + 1, StringComparison.Ordinal);
        return end < 0 ? 0 : end + delim.Length;
    }

    /// <summary>Tells if a single-line comment starts at <paramref name="i"/>: <c>--</c>, <c>//</c>, <c># </c> or
    /// <c>#!</c>, as in the ClickHouse lexer (<c>Lexer::nextTokenImpl</c>).</summary>
    public static bool StartsLineComment(string text, int i) =>
        string.CompareOrdinal(text, i, "--", 0, 2) == 0 ||
        string.CompareOrdinal(text, i, "//", 0, 2) == 0 ||
        string.CompareOrdinal(text, i, "#!", 0, 2) == 0 ||
        string.CompareOrdinal(text, i, "# ", 0, 2) == 0;

    public static bool StartsBlockComment(string text, int i) => string.CompareOrdinal(text, i, "/*", 0, 2) == 0;

    public static int SkipWhitespaceAndComments(string text, int i)
    {
        while (i < text.Length)
        {
            if (IsLexerSpace(text[i]))
            {
                i++;
            }
            else if (StartsLineComment(text, i))
            {
                i = SkipLineComment(text, i);
            }
            else if (StartsBlockComment(text, i))
            {
                i = SkipBlockComment(text, i);
            }
            else
            {
                return i;
            }
        }
        return i;
    }

    /// <summary>
    /// Returns the names of the <c>{name:Type}</c> query parameters of <paramref name="sql"/>. As in the server's
    /// lexer, a placeholder counts only outside string literals, quoted identifiers, heredocs and comments.
    /// </summary>
    public static HashSet<string> ParameterNames(string sql)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < sql.Length;)
        {
            var c = sql[i];
            if (c is '\'' or '"' or '`')
            {
                i = SkipQuoted(sql, i);
            }
            else if (c == '$' && HeredocEnd(sql, i) is var end and > 0)
            {
                i = end;
            }
            else if (StartsLineComment(sql, i))
            {
                i = SkipLineComment(sql, i);
            }
            else if (StartsBlockComment(sql, i))
            {
                i = SkipBlockComment(sql, i);
            }
            else if (c == '{' && ParameterName(sql, i) is { } name)
            {
                names.Add(name);
                i++;
            }
            else
            {
                i++;
            }
        }
        return names;
    }

    // The name of the `{name:Type}` placeholder that starts at `i`, or null.
    private static string? ParameterName(string sql, int i)
    {
        var j = SkipWhitespace(sql, i + 1);
        var start = j;
        if (j >= sql.Length || !(char.IsAsciiLetter(sql[j]) || sql[j] == '_'))
        {
            return null;
        }
        while (j < sql.Length && ChType.IsWordChar(sql[j]))
        {
            j++;
        }
        var name = sql[start..j];
        j = SkipWhitespace(sql, j);
        return j < sql.Length && sql[j] == ':' ? name : null;
    }

    /// <summary>
    /// Returns the text after the last <c>SETTINGS</c> keyword outside quotes, heredocs, comments and parentheses
    /// (the settings clause of the query), up to a following top-level <c>FORMAT</c> or the end, or null when there
    /// is none.
    /// </summary>
    public static string? TrailingSettingsClause(string sql)
    {
        int depth = 0, start = -1, end = -1;
        for (var i = 0; i < sql.Length;)
        {
            var c = sql[i];
            if (c is '\'' or '"' or '`')
            {
                i = SkipQuoted(sql, i);
            }
            else if (c == '$' && HeredocEnd(sql, i) is var heredocEnd and > 0)
            {
                i = heredocEnd;
            }
            else if (StartsLineComment(sql, i))
            {
                i = SkipLineComment(sql, i);
            }
            else if (StartsBlockComment(sql, i))
            {
                i = SkipBlockComment(sql, i);
            }
            else if (c is '(' or '[')
            {
                depth++;
                i++;
            }
            else if (c is ')' or ']')
            {
                depth--;
                i++;
            }
            else if (ChType.IsWordChar(c))
            {
                var j = i;
                while (j < sql.Length && ChType.IsWordChar(sql[j]))
                {
                    j++;
                }
                if (depth == 0 && (i == 0 || sql[i - 1] != '.'))
                {
                    if (string.Equals(sql[i..j], "SETTINGS", StringComparison.OrdinalIgnoreCase))
                    {
                        start = j;
                        end = -1;
                    }
                    else if (start >= 0 && end < 0 && string.Equals(sql[i..j], "FORMAT", StringComparison.OrdinalIgnoreCase))
                    {
                        end = i;
                    }
                }
                i = j;
            }
            else
            {
                i++;
            }
        }
        if (start < 0)
        {
            return null;
        }
        var clause = sql[start..(end < 0 ? sql.Length : end)].Trim();
        return clause.EndsWith(';') ? clause[..^1] : clause;
    }

    /// <summary>Returns the words of <paramref name="sql"/> outside quotes, heredocs, comments and (unless
    /// <paramref name="nested"/>) parentheses.</summary>
    public static List<string> TopLevelWords(string sql, bool nested = false)
    {
        var words = new List<string>();
        var depth = 0;
        for (var i = 0; i < sql.Length;)
        {
            var c = sql[i];
            if (c is '\'' or '"' or '`')
            {
                i = SkipQuoted(sql, i);
            }
            else if (c == '$' && HeredocEnd(sql, i) is var heredocEnd and > 0)
            {
                i = heredocEnd;
            }
            else if (StartsLineComment(sql, i))
            {
                i = SkipLineComment(sql, i);
            }
            else if (StartsBlockComment(sql, i))
            {
                i = SkipBlockComment(sql, i);
            }
            else if (c is '(' or '[')
            {
                depth++;
                i++;
            }
            else if (c is ')' or ']')
            {
                depth--;
                i++;
            }
            else if (ChType.IsWordChar(c))
            {
                var j = i;
                while (j < sql.Length && ChType.IsWordChar(sql[j]))
                {
                    j++;
                }
                if (depth == 0 || nested)
                {
                    words.Add(sql[i..j]);
                }
                i = j;
            }
            else
            {
                i++;
            }
        }
        return words;
    }

    /// <summary>
    /// Mirrors <c>getTestTagsLength</c>: the length of the leading <c>-- Tags: ...</c> line and the <c>--</c>
    /// comment lines right after it.
    /// </summary>
    private static int TestTagsLength(string text)
    {
        var pos = 0;
        var first = true;
        while (true)
        {
            var lineStart = pos;
            while (pos < text.Length && text[pos] is ' ' or '\t')
            {
                pos++;
            }
            if (string.CompareOrdinal(text, pos, "--", 0, 2) != 0)
            {
                return lineStart;
            }
            pos += 2;
            if (first)
            {
                while (pos < text.Length && text[pos] is ' ' or '\t')
                {
                    pos++;
                }
                if (string.CompareOrdinal(text, pos, "Tags:", 0, 5) != 0)
                {
                    return 0;
                }
                pos += 5;
                first = false;
            }
            var eol = text.IndexOfAny(['\r', '\n'], pos);
            if (eol < 0)
            {
                return text.Length;
            }
            var eolLen = 1;
            if (eol + 1 < text.Length && text[eol + 1] is '\r' or '\n' && text[eol + 1] != text[eol])
            {
                eolLen = 2;
            }
            pos = eol + eolLen;
        }
    }

    // Returns the offset of the `;` that ends the statement at start, or text.Length.
    private static int ScanStatementEnd(string text, int start)
    {
        var parens = 0;
        for (var i = start; i < text.Length;)
        {
            var c = text[i];
            if (c is '\'' or '"' or '`')
            {
                i = SkipQuoted(text, i);
            }
            else if (StartsLineComment(text, i))
            {
                i = SkipLineComment(text, i);
            }
            else if (c == '/' && StartsBlockComment(text, i))
            {
                i = SkipBlockComment(text, i);
            }
            else if (c == '$')
            {
                var end = HeredocEnd(text, i);
                i = end > 0 ? end : i + 1;
            }
            else if (c is '(' or '[')
            {
                parens++;
                i++;
            }
            else if (c is ')' or ']')
            {
                parens--;
                i++;
            }
            else if (c == ';' && parens <= 0)
            {
                return i;
            }
            else if (ChType.IsWordChar(c))
            {
                var j = i;
                while (j < text.Length && ChType.IsWordChar(text[j]))
                {
                    j++;
                }
                if (parens <= 0 && string.Equals(text[i..j], "FORMAT", StringComparison.OrdinalIgnoreCase) &&
                    InsertDataStart(text, start, j) is { } dataStart)
                {
                    // The inline data ends at an empty line.
                    var k = text.IndexOf("\n\n", dataStart, StringComparison.Ordinal);
                    return k >= 0 ? k : text.Length;
                }
                i = j;
            }
            else
            {
                i++;
            }
        }
        return text.Length;
    }

    // Tells whether the FORMAT keyword that ends at formatEnd starts the inline data of an INSERT statement
    // that begins at start, and returns the offset of the data after the format name. The `Values` format is
    // excluded: its data ends at `;`, like a query.
    private static int? InsertDataStart(string text, int start, int formatEnd)
    {
        if (LeadingKeyword(text[start..formatEnd]) != "INSERT" || OperandExpected(text, start, formatEnd - "FORMAT".Length))
        {
            return null;
        }
        var i = formatEnd;
        if (i >= text.Length || text[i] is not (' ' or '\t' or '\n' or '\r'))
        {
            return null;
        }
        while (i < text.Length && text[i] is ' ' or '\t' or '\n' or '\r')
        {
            i++;
        }
        var j = i;
        while (j < text.Length && ChType.IsWordChar(text[j]))
        {
            j++;
        }
        var word = text[i..j];
        if (j == i || string.Equals(word, "Values", StringComparison.OrdinalIgnoreCase) || IsClauseKeyword(word))
        {
            return null;
        }
        // After a SELECT, the FORMAT clause comes at the end of the SELECT, so after its top-level FROM:
        // `INSERT INTO t SELECT * FROM input('x UInt8') FORMAT TSV`. Before that FROM, `format` is in the
        // select list (`SELECT format x FROM s`).
        if (!FormatAfterSelectFrom(text[start..(formatEnd - "FORMAT".Length)]))
        {
            return null;
        }
        return j;
    }

    // Tells if a FORMAT that follows head can be the FORMAT clause of an INSERT: head has no top-level
    // SELECT, or a top-level FROM follows its last top-level SELECT.
    private static bool FormatAfterSelectFrom(string head)
    {
        bool afterSelect = false, sawFrom = false;
        foreach (var w in TopLevelWords(head))
        {
            if (string.Equals(w, "SELECT", StringComparison.OrdinalIgnoreCase))
            {
                afterSelect = true;
                sawFrom = false;
            }
            else if (string.Equals(w, "FROM", StringComparison.OrdinalIgnoreCase))
            {
                sawFrom = true;
            }
        }
        return !afterSelect || sawFrom;
    }

    private static int SkipWhitespace(string text, int i)
    {
        while (i < text.Length && IsSpace(text[i]))
        {
            i++;
        }
        return i;
    }

    private static bool HasSignificantToken(string text, int pos) => SkipWhitespaceAndComments(text, pos) < text.Length;
}
