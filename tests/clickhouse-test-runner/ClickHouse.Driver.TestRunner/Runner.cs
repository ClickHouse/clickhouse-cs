using System.Runtime.ExceptionServices;

namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// Runs the statements of one shim invocation on one session.
/// </summary>
/// <remarks>
/// Session state (<c>SET</c>, <c>USE</c>, temporary tables) must survive from one statement to the next, as in
/// <c>clickhouse-client</c>. When the backend loses the session (the TCP client closes the connection after
/// a server exception), the runner opens a new session and runs the <c>USE</c> and <c>SET</c> statements of
/// the invocation again. Other session state, such as temporary tables, is lost.
///
/// The shim sends nothing that <c>clickhouse-client</c> does not send, because some tests read their own
/// queries from <c>system.query_log</c>.
/// </remarks>
internal sealed class Runner
{
    private readonly Options opts;
    private readonly IBackend backend;
    private readonly Protocol protocol;
    private readonly Stream stdout;
    private readonly InvocationLog log;
    private readonly ValueRenderer renderer;

    // The command-line settings that go with each query, as clickhouse-client sends them. A `SET` of the
    // same setting in the test removes it from here, because the session then holds the value.
    private readonly Dictionary<string, string> settings = new(StringComparer.Ordinal);

    // The session values of the output settings (OutputSettings.Names) from the `SET` statements of the test.
    private readonly Dictionary<string, string> outputSettings = new(StringComparer.Ordinal);

    // The `USE` and `SET` statements of the test. They rebuild the session state on a new session.
    private readonly List<string> sessionInit = [];

    // The output of the current statement, until the runner knows whether it echoes the statement first.
    private readonly Utf8Buffer result = new();
    private readonly Utf8Buffer echoBuffer = new();

    // The value of the last `SET dialect`.
    private string dialect = string.Empty;

    // Set by `SET implicit_select = 1`, with which an expression alone (`1 + 2`) is a query.
    private bool implicitSelect;

    public Runner(Options opts, IBackend backend, Protocol protocol, Stream stdout, InvocationLog log, ValueRenderer renderer)
    {
        this.opts = opts;
        this.backend = backend;
        this.protocol = protocol;
        this.stdout = stdout;
        this.log = log;
        this.renderer = renderer;
        foreach (var (name, value) in opts.Settings)
        {
            settings[name] = value;
        }
    }

    /// <summary>
    /// Runs the statements in order. It stops at the first statement whose result does not match its test
    /// hint, like <c>clickhouse-client</c>.
    /// </summary>
    public async Task RunAsync(IReadOnlyList<Statement> statements)
    {
        await backend.OpenAsync().ConfigureAwait(false);
        foreach (var st in statements)
        {
            await RunOneAsync(st).ConfigureAwait(false);
        }
    }

    /// <summary>Returns the database name of a <c>USE</c> statement; a <c>{name:Identifier}</c> query
    /// parameter is replaced by its value.</summary>
    internal static string? ParseUseDatabase(string sql, IReadOnlyDictionary<string, string> parameters)
    {
        var i = Statements.SkipWhitespaceAndComments(sql, 0) + "USE".Length;
        var rest = sql[i..].Trim();
        rest = rest.EndsWith(';') ? rest[..^1].Trim() : rest;
        var space = rest.IndexOf(' ', StringComparison.Ordinal);
        if (space > 0 && string.Equals(rest[..space], "DATABASE", StringComparison.OrdinalIgnoreCase))
        {
            rest = rest[space..].Trim();
        }
        if (rest.Length == 0)
        {
            return null;
        }
        switch (rest[0])
        {
            case '`' or '"':
                return Identifiers.Unquote(rest);
            case '{':
            {
                var inner = rest.Trim('{', '}');
                var colon = inner.IndexOf(':', StringComparison.Ordinal);
                return colon < 0 ? null : parameters.GetValueOrDefault(inner[..colon].Trim());
            }
        }
        return rest.All(ChType.IsWordChar) ? rest : null;
    }

    /// <summary>Returns the settings that a <c>SET a = 1, b = 'x'</c> statement assigns, with their values
    /// (string values without quotes).</summary>
    internal static Dictionary<string, string> ParseSetAssignments(string sql)
    {
        var i = Statements.SkipWhitespaceAndComments(sql, 0) + "SET".Length;
        var body = sql[i..].Trim();
        body = body.EndsWith(';') ? body[..^1] : body;
        var output = new Dictionary<string, string>(StringComparer.Ordinal);
        void Add(string part)
        {
            var eq = part.IndexOf('=', StringComparison.Ordinal);
            if (eq < 0)
            {
                return;
            }
            var value = part[(eq + 1)..].Trim();
            if (value.Length >= 2 && value[0] == '\'' && value[^1] == '\'')
            {
                value = value[1..^1];
            }
            output[part[..eq].Trim()] = value;
        }
        int depth = 0, start = 0;
        for (var j = 0; j < body.Length; j++)
        {
            switch (body[j])
            {
                case '\'' or '"' or '`':
                    j = Statements.SkipQuoted(body, j) - 1;
                    break;
                case '(' or '[':
                    depth++;
                    break;
                case ')' or ']':
                    depth--;
                    break;
                case ',' when depth == 0:
                    Add(body[start..j]);
                    start = j + 1;
                    break;
            }
        }
        Add(body[start..]);
        return output;
    }

    /// <summary>
    /// Tells if a <c>SET</c> statement changes the state of the session. <c>SET DEFAULT ROLE ... TO user</c> is
    /// an access-control statement: it changes the user, so a new session must not run it again.
    /// </summary>
    internal static bool IsSessionSet(string sql)
    {
        var words = Statements.TopLevelWords(sql);
        return !(words.Count >= 3 &&
            string.Equals(words[1], "DEFAULT", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(words[2], "ROLE", StringComparison.OrdinalIgnoreCase));
    }

    private async Task RunOneAsync(Statement st)
    {
        if (st.Hint.ParseError is { } parseError)
        {
            WriteEcho(st);
            Flush();
            throw new HintException(parseError, 0);
        }
        if (backend.SessionLost)
        {
            await InitSessionAsync().ConfigureAwait(false);
        }

        log.Add("query", st.Sql);
        result.Clear();
        Exception? error = null;
        try
        {
            await ExecuteAsync(st.Sql).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            error = e;
        }
        var serverError = error is null ? null : ServerError.From(error);

        // clickhouse-client parses a statement before it echoes it, so it does not echo a statement with a
        // syntax error. A parse error fails a hint that expects only server errors (ClientBase::executeMultiQuery),
        // so with such a hint the client parsed the statement and the server found the syntax error at run time,
        // as in formatQuery('...') or a table function argument: then the statement is echoed.
        var serverOnlyHint = st.Hint.ServerErrors.Count > 0 && st.Hint.ClientErrors.Count == 0;
        if (serverError?.Code != ErrorCodes.SyntaxError || serverOnlyHint)
        {
            WriteEcho(st);
        }
        stdout.Write(result.Span);
        Flush();

        if (st.Hint.ExpectsError && error is null)
        {
            throw new HintException($"expected {st.Hint.Label}, but the query succeeded (query: {st.Full})", 0);
        }
        if (st.Hint.ExpectsError && serverError is not null)
        {
            if (!st.Hint.Matches(serverError.Code))
            {
                throw new HintException($"expected {st.Hint.Label}, but got {serverError.Format()} (query: {st.Full})", serverError.Code);
            }
            log.Add("expected_error_matched", st.Hint.Label);
            return;
        }
        if (error is not null)
        {
            // Keep the stack trace of the throw site (client or renderer) for the invocation log.
            ExceptionDispatchInfo.Throw(error);
        }
        AfterSuccess(st.Sql);
    }

    // Runs the statements that rebuild the session state on a new session.
    private async Task InitSessionAsync()
    {
        await backend.ReopenAsync().ConfigureAwait(false);
        foreach (var q in sessionInit)
        {
            log.Add("session_init", q);
            try
            {
                await backend.ExecuteAsync(q, Context(q)).ConfigureAwait(false);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                throw new InvalidOperationException($"initialize the session again with \"{q}\": {e.Message}", e);
            }
        }
    }

    private void WriteEcho(Statement st)
    {
        if (st.Echo)
        {
            echoBuffer.Clear();
            echoBuffer.Append(st.Full);
            echoBuffer.Append((byte)'\n');
            stdout.Write(echoBuffer.Span);
        }
    }

    private void Flush() => stdout.Flush();

    // Updates the session state after a USE or SET statement.
    private void AfterSuccess(string sql)
    {
        switch (Statements.LeadingKeyword(sql))
        {
            case "USE":
            {
                var db = ParseUseDatabase(sql, opts.Params) ?? throw new InvalidOperationException($"clickhouse-test-runner: cannot read the database name of \"{sql}\"");
                sessionInit.Add(sql);
                backend.UseDatabase(db);
                break;
            }
            case "SET" when IsSessionSet(sql):
                sessionInit.Add(sql);
                foreach (var (name, value) in ParseSetAssignments(sql))
                {
                    settings.Remove(name);
                    if (OutputSettings.Names.Contains(name))
                    {
                        outputSettings[name] = value;
                    }
                    switch (name)
                    {
                        case "dialect":
                            dialect = value;
                            break;
                        case "implicit_select":
                            implicitSelect = value != "0" && !string.Equals(value, "false", StringComparison.OrdinalIgnoreCase);
                            break;
                    }
                }
                break;
        }
    }

    // The settings and the query parameters of a statement. Only the parameters that the statement uses go
    // with it: the TCP client needs the type of each parameter from its placeholder, and it does not see a
    // placeholder in a string literal or a comment (the server does not substitute one there either).
    private QueryContext Context(string sql)
    {
        var parameters = new Dictionary<string, string>(StringComparer.Ordinal);
        if (opts.Params.Count > 0)
        {
            foreach (var name in Statements.ParameterNames(sql))
            {
                if (opts.Params.TryGetValue(name, out var value))
                {
                    parameters[name] = value;
                }
            }
        }
        return new QueryContext(new Dictionary<string, string>(settings, StringComparer.Ordinal), parameters);
    }

    private async Task ExecuteAsync(string sql)
    {
        var other = OtherDialect(sql);
        if (other && protocol == Protocol.Http)
        {
            // Over HTTP the server parses the request body as one query, and the parsers of other dialects
            // do not accept a trailing semicolon.
            sql = sql.TrimEnd(' ', '\t', '\r', '\n');
            sql = sql.EndsWith(';') ? sql[..^1] : sql;
        }
        var context = Context(sql);
        if (!OutputFormat.ReturnsRows(sql) && !other && !IsImplicitSelect(sql))
        {
            await backend.ExecuteAsync(sql, context).ConfigureAwait(false);
            return;
        }
        var (query, format) = (sql, OutputFormat.Default);
        if (!other && !OutputFormat.ExplainedQueryOwnsFormat(sql))
        {
            // The FORMAT clause is SQL; other dialects can use the word differently (the `format` pipe of
            // LogsQL).
            (query, format) = OutputFormat.Split(sql);
        }
        if (!format.Supported)
        {
            throw new NotSupportedException($"clickhouse-test-runner: output format {format.Name} is not supported: the clients read results as RowBinary (HTTP) or Native (TCP), and the shim writes them only as TabSeparated");
        }
        var explainNull = format.Discard && Statements.LeadingKeyword(sql) == "EXPLAIN";
        if (explainNull)
        {
            // With ignore_format_null_for_explain (on by default), the server ignores `FORMAT Null` for
            // EXPLAIN and sends the plan. The server decides, so write the rows that it sends.
            format = OutputFormat.Default;
        }
        if (protocol == Protocol.Tcp || format.Discard || explainNull)
        {
            // Send the text as clickhouse-client does; tests that read system.query_log see it. Over TCP the
            // server ignores the FORMAT clause (the client reads Native blocks). With `FORMAT Null`, the
            // server sends no rows over either protocol.
            query = sql;
        }
        renderer.Output = StatementOutputSettings(sql);
        var writer = new ResultWriter(result, format, renderer);
        await backend.QueryAsync(query, format, writer, context).ConfigureAwait(false);
    }

    // The output settings of a query: the session values, then the SETTINGS clause of the query.
    internal OutputSettings StatementOutputSettings(string sql)
    {
        var clause = Statements.TrailingSettingsClause(sql);
        if (outputSettings.Count == 0 && clause is null)
        {
            return OutputSettings.Default;
        }
        var values = new Dictionary<string, string>(outputSettings, StringComparer.Ordinal);
        if (clause is not null)
        {
            foreach (var (name, value) in ParseSetAssignments("SET " + clause))
            {
                if (OutputSettings.Names.Contains(name))
                {
                    values[name] = value;
                }
            }
        }
        return OutputSettings.From(values);
    }

    // Tells if the session uses another query language than SQL (`SET dialect = 'kusto'`, 'prql', ...). The
    // shim cannot see if a statement in such a language returns rows, so it reads a result for it.
    private bool OtherDialect(string sql) =>
        dialect.Length != 0 && !string.Equals(dialect, "clickhouse", StringComparison.OrdinalIgnoreCase) && Statements.LeadingKeyword(sql) != "SET";

    // Tells if the statement is an expression alone, which `SET implicit_select = 1` makes a query.
    private bool IsImplicitSelect(string sql) => implicitSelect && Statements.LeadingKeyword(sql) is not (
        "INSERT" or "CREATE" or "DROP" or "ALTER" or "SET" or "USE" or "TRUNCATE" or "RENAME" or "OPTIMIZE" or
        "SYSTEM" or "GRANT" or "REVOKE" or "ATTACH" or "DETACH" or "KILL" or "EXCHANGE" or "UNDROP" or "BACKUP" or "RESTORE");
}

/// <summary>A result that does not match the test hint.</summary>
internal sealed class HintException : Exception
{
    public HintException(string message, int code)
        : base(message)
    {
        Code = code;
    }

    /// <summary>Gets the server error code, or 0.</summary>
    public int Code { get; }
}

/// <summary>The protocol (client) that the shim uses.</summary>
public enum Protocol
{
    Http,
    Tcp,
}
