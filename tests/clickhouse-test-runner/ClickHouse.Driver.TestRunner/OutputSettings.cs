namespace ClickHouse.Driver.TestRunner;

/// <summary>The forms of <c>date_time_output_format</c>.</summary>
internal enum DateTimeOutputFormat
{
    /// <summary><c>simple</c> (the default): <c>2020-10-15 00:00:00</c> in the zone of the type.</summary>
    Simple,

    /// <summary><c>iso</c>: <c>2020-10-14T21:00:00Z</c>, always UTC.</summary>
    Iso,

    /// <summary><c>unix_timestamp</c>: <c>1602709200</c>, or <c>1602709200.12</c> for a <c>DateTime64(2)</c>.</summary>
    UnixTimestamp,
}

/// <summary>
/// The settings that change the server's text output and that the renderer follows. The shim writes the values
/// itself, so it must write them as the server would with these settings. Their values come from the
/// <c>SET</c> statements of the session and the <c>SETTINGS</c> clause of the query.
/// </summary>
/// <param name="DateTimeFormat"><c>date_time_output_format</c>.</param>
/// <param name="NullText"><c>format_tsv_null_representation</c>: a NULL at the top level.</param>
/// <param name="DecimalTrailingZeros"><c>output_format_decimal_trailing_zeros</c>.</param>
/// <param name="CutDateTime64TrailingZeros"><c>date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands</c>.</param>
internal sealed record OutputSettings(DateTimeOutputFormat DateTimeFormat, string NullText, bool DecimalTrailingZeros, bool CutDateTime64TrailingZeros)
{
    /// <summary>The server defaults.</summary>
    public static readonly OutputSettings Default = new(DateTimeOutputFormat.Simple, "\\N", false, false);

    /// <summary>The names of the settings that this record holds.</summary>
    public static readonly IReadOnlySet<string> Names = new HashSet<string>(StringComparer.Ordinal)
    {
        "date_time_output_format", "format_tsv_null_representation", "output_format_decimal_trailing_zeros",
        "date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands",
    };

    /// <summary>Makes the settings from setting values (string values without quotes, escapes not resolved).</summary>
    /// <exception cref="NotSupportedException">A <c>date_time_output_format</c> that the shim does not know.</exception>
    public static OutputSettings From(IReadOnlyDictionary<string, string> values)
    {
        var s = Default;
        if (values.TryGetValue("date_time_output_format", out var f))
        {
            s = s with
            {
                DateTimeFormat = f.ToLowerInvariant() switch
                {
                    "simple" => DateTimeOutputFormat.Simple,
                    "iso" => DateTimeOutputFormat.Iso,
                    "unix_timestamp" => DateTimeOutputFormat.UnixTimestamp,
                    _ => throw new NotSupportedException($"clickhouse-test-runner: date_time_output_format '{f}' is not supported"),
                },
            };
        }
        if (values.TryGetValue("format_tsv_null_representation", out var n))
        {
            s = s with { NullText = Unescape(n) };
        }
        if (values.TryGetValue("output_format_decimal_trailing_zeros", out var z))
        {
            s = s with { DecimalTrailingZeros = IsTrue(z) };
        }
        if (values.TryGetValue("date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", out var c))
        {
            s = s with { CutDateTime64TrailingZeros = IsTrue(c) };
        }
        return s;
    }

    private static bool IsTrue(string value) => value == "1" || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    // Resolves the backslash escapes of a SQL string literal (the quotes are already removed).
    internal static string Unescape(string literal)
    {
        if (!literal.Contains('\\', StringComparison.Ordinal))
        {
            return literal;
        }
        var sb = new System.Text.StringBuilder(literal.Length);
        for (var i = 0; i < literal.Length; i++)
        {
            var c = literal[i];
            if (c != '\\' || i + 1 == literal.Length)
            {
                sb.Append(c);
                continue;
            }
            sb.Append(literal[++i] switch
            {
                't' => '\t',
                'n' => '\n',
                'r' => '\r',
                '0' => '\0',
                'b' => '\b',
                'f' => '\f',
                var other => other,
            });
        }
        return sb.ToString();
    }
}
