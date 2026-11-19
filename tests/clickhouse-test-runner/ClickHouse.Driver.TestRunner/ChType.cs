using System.Text;

namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// A parsed ClickHouse data type, for example <c>Array(Nullable(DateTime64(3, 'UTC')))</c>.
/// </summary>
/// <remarks>
/// <see cref="Args"/> holds the parameters inside the parentheses. A parameter is a nested type
/// (<see cref="ChTypeArg.Type"/> is set) or a literal, such as the <c>3</c> and <c>'UTC'</c> above or the
/// <c>'a' = 1</c> of an <c>Enum8</c>. <see cref="ChTypeArg.Name"/> is set for the named elements of a
/// <c>Tuple</c> or <c>Nested</c>, as in <c>Tuple(a Int32, b String)</c>.
/// </remarks>
internal sealed class ChType
{
    public ChType(string baseName, IReadOnlyList<ChTypeArg>? args = null)
    {
        Base = baseName;
        Args = args ?? [];
    }

    public string Base { get; }

    public IReadOnlyList<ChTypeArg> Args { get; }

    /// <summary>Parses the type name that the server or the client gives for a result column.</summary>
    public static ChType Parse(string text)
    {
        var s = text.Trim();
        var (type, rest) = ParseAt(s);
        if (rest.Trim().Length != 0)
        {
            throw new FormatException($"parse type \"{text}\": unexpected trailing text \"{rest}\"");
        }
        return type;
    }

    /// <summary>Returns the nested type of argument <paramref name="index"/>, or null.</summary>
    public ChType? TypeArg(int index) => index < Args.Count ? Args[index].Type : null;

    /// <summary>Returns the literal of argument <paramref name="index"/> without its single quotes, or "".</summary>
    public string LiteralArg(int index) => index < Args.Count ? Args[index].Literal.Trim('\'') : string.Empty;

    public override string ToString()
    {
        if (Args.Count == 0)
        {
            return Base;
        }
        var parts = Args.Select(a => a switch
        {
            { Type: not null, Name.Length: > 0 } => a.Name + " " + a.Type,
            { Type: not null } => a.Type.ToString(),
            _ => a.Literal,
        });
        return Base + "(" + string.Join(", ", parts) + ")";
    }

    internal static bool IsWordChar(char c) => c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');

    private static (ChType Type, string Remainder) ParseAt(string s)
    {
        var i = 0;
        while (i < s.Length && (IsWordChar(s[i]) || s[i] == '.'))
        {
            i++;
        }
        if (i == 0)
        {
            throw new FormatException($"expected a type name at \"{s}\"");
        }
        var baseName = s[..i];
        var rest = s[i..].TrimStart(' ');
        if (!rest.StartsWith('('))
        {
            return (new ChType(baseName), rest);
        }
        var (parts, after) = SplitArgs(rest);
        var args = parts.Select(p => ParseArg(baseName, p)).ToList();
        return (new ChType(baseName, args), after);
    }

    private static bool TryParseWhole(string s, out ChType type)
    {
        try
        {
            var (t, rest) = ParseAt(s);
            type = t;
            return rest.Trim().Length == 0;
        }
        catch (FormatException)
        {
            type = null!;
            return false;
        }
    }

    // Turns one raw parameter into a nested type, a named nested type, or a literal.
    private static ChTypeArg ParseArg(string baseName, string raw)
    {
        raw = raw.Trim();
        if (raw.Length == 0 || raw[0] == '\'' || raw[0] == '-' || char.IsAsciiDigit(raw[0]))
        {
            return new ChTypeArg(Literal: raw);
        }
        switch (baseName)
        {
            case "Enum8" or "Enum16" or "Enum":
                return new ChTypeArg(Literal: raw);
            case "AggregateFunction" or "SimpleAggregateFunction":
                // The first argument is a function name, such as `sum` or `quantiles(0.5, 0.9)`.
                if (TryParseWhole(raw, out var fnType) && IsKnownTypeName(fnType.Base))
                {
                    return new ChTypeArg(Type: fnType);
                }
                return new ChTypeArg(Literal: raw);
            case "Tuple" or "Nested" or "JSON" or "Object" or "Variant":
                // A named element: `name Type`; the name can be quoted with backticks.
                if (SplitNamedElement(raw, out var name, out var typeText) && TryParseWhole(typeText, out var named))
                {
                    return new ChTypeArg(Name: name, Type: named);
                }
                break;
        }
        return TryParseWhole(raw, out var t) ? new ChTypeArg(Type: t) : new ChTypeArg(Literal: raw);
    }

    // Splits `name Type` into its two parts. Returns false for an element that is only a type, such as
    // `Int32` or `Array(String)`.
    private static bool SplitNamedElement(string raw, out string name, out string type)
    {
        name = type = string.Empty;
        if (raw.StartsWith('`'))
        {
            var sb = new StringBuilder();
            for (var j = 1; j < raw.Length; j++)
            {
                if (raw[j] == '\\' && j + 1 < raw.Length)
                {
                    sb.Append(raw[++j]);
                }
                else if (raw[j] == '`')
                {
                    if (j + 1 < raw.Length && raw[j + 1] == '`')
                    {
                        sb.Append('`');
                        j++;
                        continue;
                    }
                    name = sb.ToString();
                    type = raw[(j + 1)..].Trim();
                    return type.Length > 0;
                }
                else
                {
                    sb.Append(raw[j]);
                }
            }
            return false;
        }
        var i = 0;
        while (i < raw.Length && (IsWordChar(raw[i]) || raw[i] == '.'))
        {
            i++;
        }
        if (i == 0 || i >= raw.Length || raw[i] != ' ')
        {
            return false;
        }
        name = raw[..i];
        type = raw[i..].Trim();
        return true;
    }

    // Splits `(a, b(c, d), 'e,f')rest` into its top-level parameters and the text after the closing
    // parenthesis.
    private static (List<string> Parts, string Remainder) SplitArgs(string s)
    {
        var parts = new List<string>();
        var depth = 0;
        var start = 1;
        var quote = '\0';
        for (var i = 0; i < s.Length; i++)
        {
            var c = s[i];
            if (quote != '\0')
            {
                if (c == '\\')
                {
                    i++;
                }
                else if (c == quote)
                {
                    quote = '\0';
                }
                continue;
            }
            switch (c)
            {
                case '\'' or '`' or '"':
                    quote = c;
                    break;
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    if (depth == 0)
                    {
                        var last = s[start..i].Trim();
                        if (last.Length != 0 || parts.Count > 0)
                        {
                            parts.Add(last);
                        }
                        return (parts, s[(i + 1)..]);
                    }
                    break;
                case ',' when depth == 1:
                    parts.Add(s[start..i].Trim());
                    start = i + 1;
                    break;
            }
        }
        throw new FormatException($"unbalanced parentheses in \"{s}\"");
    }

    // Used only to tell `SimpleAggregateFunction(sum, UInt64)` (function name, then type) apart; a false
    // negative only makes the renderer report an unsupported type.
    private static bool IsKnownTypeName(string name) => name is
        "Int8" or "Int16" or "Int32" or "Int64" or "Int128" or "Int256" or
        "UInt8" or "UInt16" or "UInt32" or "UInt64" or "UInt128" or "UInt256" or
        "Float32" or "Float64" or "BFloat16" or "Bool" or "String" or "FixedString" or
        "Date" or "Date32" or "DateTime" or "DateTime64" or "Time" or "Time64" or "UUID" or "IPv4" or "IPv6" or
        "Decimal" or "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256" or
        "Enum8" or "Enum16" or "Nullable" or "LowCardinality" or "Array" or "Tuple" or "Map" or "Nothing" or
        "Point" or "Ring" or "LineString" or "MultiLineString" or "Polygon" or "MultiPolygon" or "Geometry" or
        "JSON" or "Json" or "Object" or "Dynamic" or "Variant" or "Nested" or "QBit" ||
        name.StartsWith("Interval", StringComparison.Ordinal);
}

/// <summary>One parameter of a <see cref="ChType"/>.</summary>
internal sealed record ChTypeArg(string Name = "", ChType? Type = null, string Literal = "");
