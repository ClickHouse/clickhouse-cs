namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// The test hints of one statement, as <c>clickhouse-client</c> parses them (see
/// <c>src/Client/TestHint.cpp</c>).
/// </summary>
/// <remarks>
/// <code>
/// SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
/// SELECT 1 / 0;      -- { serverError 153, 6 }
/// -- { echoOn }
/// </code>
/// An error hint counts only when it is not in a leading comment. An <c>echo</c> hint counts in any comment.
/// </remarks>
internal sealed class TestHint
{
    public List<int> ServerErrors { get; private set; } = [];

    public List<int> ClientErrors { get; private set; } = [];

    /// <summary>Gets the value that <c>echo</c>, <c>echoOn</c> (true) or <c>echoOff</c> (false) set.</summary>
    public bool? Echo { get; private set; }

    /// <summary>Gets the error of a malformed hint or of a hint that names an unknown error.</summary>
    public string? ParseError { get; private set; }

    public bool ExpectsError => ServerErrors.Count > 0 || ClientErrors.Count > 0;

    public string Label
    {
        get
        {
            static string Codes(List<int> list) => string.Join(", ", list.Select(c => ErrorCodes.NameOf(c) ?? c.ToString(System.Globalization.CultureInfo.InvariantCulture)));
            if (ServerErrors.Count > 0 && ClientErrors.Count > 0)
            {
                return "error " + Codes(ServerErrors);
            }
            return ServerErrors.Count > 0 ? "serverError " + Codes(ServerErrors) : "clientError " + Codes(ClientErrors);
        }
    }

    public static TestHint Parse(string full)
    {
        var h = new TestHint();
        var leading = true;
        for (var i = 0; i < full.Length;)
        {
            int end;
            var c = full[i];
            if (Statements.IsLexerSpace(c))
            {
                i++;
                continue;
            }
            if (Statements.StartsLineComment(full, i))
            {
                end = Statements.SkipLineComment(full, i);
            }
            else if (Statements.StartsBlockComment(full, i))
            {
                end = Statements.SkipBlockComment(full, i);
            }
            else if (c is '\'' or '"' or '`')
            {
                leading = false;
                i = Statements.SkipQuoted(full, i);
                continue;
            }
            else if (c == '$')
            {
                leading = false;
                var e = Statements.HeredocEnd(full, i);
                i = e > 0 ? e : i + 1;
                continue;
            }
            else
            {
                leading = false;
                i++;
                continue;
            }
            var comment = full[i..end];
            var open = comment.IndexOf('{', StringComparison.Ordinal);
            if (open >= 0)
            {
                var closing = comment.IndexOf('}', open);
                if (closing >= 0)
                {
                    h.ParseBody(comment[(open + 1)..closing], leading);
                }
            }
            i = end;
        }
        return h;
    }

    /// <summary>
    /// Tells if the error code satisfies the hint. Through a driver the server finds every error, also the
    /// syntax errors that <c>clickhouse-client</c> finds itself, so a server error can satisfy a
    /// <c>clientError</c> hint too.
    /// </summary>
    public bool Matches(int code) => ServerErrors.Contains(code) || ClientErrors.Contains(code);

    private static List<string> Tokenize(string body)
    {
        var output = new List<string>();
        for (var i = 0; i < body.Length;)
        {
            var c = body[i];
            if (Statements.IsLexerSpace(c))
            {
                i++;
            }
            else if (c == ',')
            {
                output.Add(",");
                i++;
            }
            else
            {
                var j = i;
                while (j < body.Length && !Statements.IsLexerSpace(body[j]) && body[j] != ',')
                {
                    j++;
                }
                output.Add(body[i..j]);
                i = j;
            }
        }
        return output;
    }

    private void ParseBody(string body, bool leading)
    {
        var tokens = Tokenize(body);
        for (var i = 0; i < tokens.Count; i++)
        {
            switch (tokens[i])
            {
                case "echo" or "echoOn":
                    Echo = true;
                    break;
                case "echoOff":
                    Echo = false;
                    break;
                case "retry":
                    // `retry N until|while serverError X` is used by a few tests that wait for a background
                    // process. It is not supported.
                    ParseError = "the \"retry\" test hint is not supported";
                    return;
                case "serverError" or "clientError" or "error":
                    if (leading)
                    {
                        continue;
                    }
                    var codes = new List<int>();
                    foreach (var t in tokens.Skip(i + 1))
                    {
                        if (t == ",")
                        {
                            continue;
                        }
                        if (int.TryParse(t, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var n))
                        {
                            codes.Add(n);
                            continue;
                        }
                        if (ErrorCodes.CodeOf(t) is not { } code)
                        {
                            ParseError = $"unknown error name \"{t}\" in test hint";
                            return;
                        }
                        codes.Add(code);
                    }
                    switch (tokens[i])
                    {
                        case "serverError":
                            ServerErrors = codes;
                            break;
                        case "clientError":
                            ClientErrors = codes;
                            break;
                        default:
                            ServerErrors = codes;
                            ClientErrors = [.. codes];
                            break;
                    }
                    return;
            }
        }
    }
}
