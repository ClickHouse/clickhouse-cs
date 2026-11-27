namespace ClickHouse.Driver.TestRunner;

/// <summary>How the shim writes a result set.</summary>
internal sealed record OutputFormat(
    string Name,
    bool Supported,
    bool Names = false,
    bool Types = false,
    bool Raw = false,
    bool Discard = false)
{
    public static readonly OutputFormat Default = new("TabSeparated", Supported: true);

    // The leading keywords of the statements that return a result set. The shim reads their rows, and runs
    // the other statements without a result.
    private static readonly HashSet<string> ResultKeywords = new(StringComparer.Ordinal)
    {
        "SELECT", "WITH", "SHOW", "DESC", "DESCRIBE", "EXISTS", "EXPLAIN", "CHECK",
        // KILL QUERY and KILL MUTATION return a row for each query or mutation that they stop.
        "KILL",
        // FROM-first syntax: FROM t SELECT x, and pipe syntax: FROM t |> ...
        "FROM",
    };

    public static bool ReturnsRows(string sql) => ResultKeywords.Contains(Statements.LeadingKeyword(sql));

    /// <summary>
    /// Tells if the trailing <c>FORMAT</c> clause of an <c>EXPLAIN</c> statement belongs to the explained query, as in
    /// <c>ParserExplainQuery</c>. Then the format is part of the query text, and the output format of the
    /// <c>EXPLAIN</c> is the default one.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    /// <item>An explained <c>INSERT</c> keeps the format of its own data: an <c>INSERT</c> without <c>SELECT</c>, with
    /// <c>FROM INFILE</c>, or with the <c>input</c> table function.</item>
    /// <item><c>EXPLAIN AST</c> parses the explained query with its output clauses. A query in parentheses is parsed
    /// with them only when it is a <c>SELECT</c> (the shim checks the first word in the parentheses); for another
    /// query in parentheses the <c>FORMAT</c> after the parentheses is the output format of the <c>EXPLAIN</c>.</item>
    /// </list>
    /// </remarks>
    public static bool ExplainedQueryOwnsFormat(string sql)
    {
        var words = Statements.TopLevelWords(sql);
        if (words.Count < 2 || !Is(words[0], "EXPLAIN"))
        {
            return false;
        }
        var insert = words.FindIndex(w => Is(w, "INSERT"));
        var query = words.FindIndex(w => Is(w, "SELECT") || Is(w, "WITH") || Is(w, "FROM"));
        if (insert >= 0 && (query < 0 || insert < query))
        {
            // The words after INSERT, also in parentheses: INSERT INTO t (SELECT 1), FROM (SELECT * FROM input(...)).
            var all = Statements.TopLevelWords(sql, nested: true);
            var rest = all.Skip(all.FindIndex(w => Is(w, "INSERT"))).ToList();
            var select = rest.Exists(w => Is(w, "SELECT"));
            var infile = rest.Exists(w => Is(w, "INFILE"));
            var input = CallsInput(sql);
            return !select || infile || input;
        }
        if (!Is(words[1], "AST"))
        {
            return false;
        }
        // The first token of the explained query.
        var i = SkipWord(sql, SkipWord(sql, Statements.SkipWhitespaceAndComments(sql, 0)));
        if (i >= sql.Length || sql[i] != '(')
        {
            return true;
        }
        // A SELECT in parentheses. In `((SELECT 1) SETTINGS max_threads = 1) FORMAT TSV` the explained query is not a
        // SELECT, so the FORMAT is the output format of the EXPLAIN.
        i = Statements.SkipWhitespaceAndComments(sql, i + 1);
        var j = i;
        while (j < sql.Length && ChType.IsWordChar(sql[j]))
        {
            j++;
        }
        return Is(sql[i..j], "SELECT") || Is(sql[i..j], "WITH");

        static bool Is(string word, string keyword) => string.Equals(word, keyword, StringComparison.OrdinalIgnoreCase);

        // Tells if the text calls the `input` table function: the word `input` before `(`, outside quotes and
        // comments, at any depth (a column named `input` does not count).
        static bool CallsInput(string text)
        {
            for (var i = 0; i < text.Length;)
            {
                var c = text[i];
                if (c is '\'' or '"' or '`')
                {
                    i = Statements.SkipQuoted(text, i);
                }
                else if (Statements.StartsLineComment(text, i))
                {
                    i = Statements.SkipLineComment(text, i);
                }
                else if (Statements.StartsBlockComment(text, i))
                {
                    i = Statements.SkipBlockComment(text, i);
                }
                else if (ChType.IsWordChar(c))
                {
                    var j = i;
                    while (j < text.Length && ChType.IsWordChar(text[j]))
                    {
                        j++;
                    }
                    var k = Statements.SkipWhitespaceAndComments(text, j);
                    if (Is(text[i..j], "input") && k < text.Length && text[k] == '(')
                    {
                        return true;
                    }
                    i = j;
                }
                else
                {
                    i++;
                }
            }
            return false;
        }

        // Returns the position after the word at i and the whitespace and comments after it.
        static int SkipWord(string text, int i)
        {
            while (i < text.Length && ChType.IsWordChar(text[i]))
            {
                i++;
            }
            return Statements.SkipWhitespaceAndComments(text, i);
        }
    }

    /// <summary>
    /// Removes a top-level <c>FORMAT &lt;name&gt;</c> clause from a result-set statement and returns the
    /// statement and the format.
    /// </summary>
    /// <remarks>
    /// The clients read results as RowBinary (HTTP) or Native (TCP) and the shim writes them as TabSeparated,
    /// so only the TabSeparated family and <c>Null</c> can be written. A statement with another format fails.
    /// </remarks>
    public static (string Sql, OutputFormat Format) Split(string sql)
    {
        var (start, end, name) = FindFormatClause(sql);
        if (start < 0)
        {
            return (sql, Default);
        }
        var f = name.ToLowerInvariant() switch
        {
            "tabseparated" or "tsv" => new OutputFormat(name, true),
            "tabseparatedwithnames" or "tsvwithnames" => new OutputFormat(name, true, Names: true),
            "tabseparatedwithnamesandtypes" or "tsvwithnamesandtypes" => new OutputFormat(name, true, Names: true, Types: true),
            "tabseparatedraw" or "tsvraw" or "raw" => new OutputFormat(name, true, Raw: true),
            "tabseparatedrawwithnames" or "tsvrawwithnames" or "rawwithnames" => new OutputFormat(name, true, Names: true, Raw: true),
            "tabseparatedrawwithnamesandtypes" or "tsvrawwithnamesandtypes" or "rawwithnamesandtypes" => new OutputFormat(name, true, Names: true, Types: true, Raw: true),
            "null" => new OutputFormat(name, true, Discard: true),
            _ => new OutputFormat(name, false),
        };
        return (sql[..start] + sql[end..], f);
    }

    // Returns the position of a top-level `FORMAT <name>` clause, or start = -1.
    private static (int Start, int End, string Name) FindFormatClause(string sql)
    {
        var depth = 0;
        for (var i = 0; i < sql.Length;)
        {
            var c = sql[i];
            if (c is '\'' or '"' or '`')
            {
                i = Statements.SkipQuoted(sql, i);
            }
            else if (Statements.StartsLineComment(sql, i))
            {
                i = Statements.SkipLineComment(sql, i);
            }
            else if (Statements.StartsBlockComment(sql, i))
            {
                i = Statements.SkipBlockComment(sql, i);
            }
            else if (c == '$')
            {
                var e = Statements.HeredocEnd(sql, i);
                i = e > 0 ? e : i + 1;
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
                if (depth == 0 && string.Equals(sql[i..j], "FORMAT", StringComparison.OrdinalIgnoreCase) && (i == 0 || sql[i - 1] != '.'))
                {
                    // `ORDER BY format DESC` ends the query too: a clause keyword after FORMAT shows an
                    // identifier named `format`. Where an operand is expected, `format` is a name with an
                    // implicit alias: `SELECT * FROM format f`.
                    if (FormatName(sql, j) is { } fn && !Statements.IsClauseKeyword(fn.Name) && EndsQuery(sql, fn.End) &&
                        !Statements.OperandExpected(sql, 0, i))
                    {
                        return (i, fn.End, fn.Name);
                    }
                }
                i = j;
            }
            else
            {
                i++;
            }
        }
        return (-1, -1, string.Empty);
    }

    // Reads the format name after the FORMAT keyword that ends at j.
    private static (int End, string Name)? FormatName(string sql, int j)
    {
        var k = Statements.SkipWhitespaceAndComments(sql, j);
        if (k == j || k >= sql.Length)
        {
            return null;
        }
        if (sql[k] is '`' or '"')
        {
            // A quoted format name: FORMAT `Null`.
            var m = Statements.SkipQuoted(sql, k);
            return Identifiers.Unquote(sql[k..m]) is { } unquoted ? (m, unquoted) : null;
        }
        var e = k;
        while (e < sql.Length && ChType.IsWordChar(sql[e]))
        {
            e++;
        }
        return e == k ? null : (e, sql[k..e]);
    }

    // Tells if the text from i on can follow the FORMAT clause of a query: nothing, a semicolon, or a
    // SETTINGS clause. Otherwise FORMAT is an identifier, as in `SELECT format FROM t`.
    private static bool EndsQuery(string sql, int i)
    {
        var k = Statements.SkipWhitespaceAndComments(sql, i);
        if (k >= sql.Length || sql[k] == ';')
        {
            return true;
        }
        var m = k;
        while (m < sql.Length && ChType.IsWordChar(sql[m]))
        {
            m++;
        }
        return string.Equals(sql[k..m], "SETTINGS", StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>Quoted SQL identifiers.</summary>
internal static class Identifiers
{
    /// <summary>Removes the quotes (<c>`</c> or <c>"</c>) of an identifier and resolves its escapes.</summary>
    public static string? Unquote(string s)
    {
        if (s.Length < 2 || s[^1] != s[0])
        {
            return null;
        }
        var q = s[0];
        var sb = new System.Text.StringBuilder();
        for (var i = 1; i < s.Length - 1; i++)
        {
            var c = s[i];
            if (c == '\\' && i + 1 < s.Length - 1)
            {
                sb.Append(s[++i]);
            }
            else if (c == q && i + 1 < s.Length - 1 && s[i + 1] == q)
            {
                sb.Append(q);
                i++;
            }
            else
            {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }
}
