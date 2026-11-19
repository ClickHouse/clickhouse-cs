using System.Collections;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using ClickHouse.Driver.Tcp;
using NodaTime;

namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// Writes the .NET values that a client of this repository read as the text that the server writes for the
/// <c>TabSeparated</c> format. The upstream <c>.reference</c> files hold the server's text, so a test passes
/// only when the client decoded every value correctly.
/// </summary>
/// <remarks>
/// <para>
/// The rendering is type-directed: the .NET value alone is not sufficient (the precision of a
/// <c>DateTime64</c> is in the type only, and the TCP client gives a <c>DateTime</c> as its raw count).
/// </para>
/// <para>
/// The two clients give different .NET types for the same ClickHouse type, so the renderer takes the values
/// of both. HTTP: <c>BigInteger</c>, <c>Tuple&lt;...&gt;</c>, <c>List&lt;KeyValuePair&gt;</c>, calendar
/// <c>DateTime</c>, enum labels. TCP: <c>Int128</c>, <c>ValueTuple</c>, <c>KeyValuePair[]</c>, raw
/// <c>uint</c>/<c>long</c> counts for <c>DateTime</c>/<c>DateTime64</c>/<c>Time64</c>, enum ordinals,
/// <c>byte[]</c> for <c>FixedString</c>, <c>DateOnly</c>. For TCP, the renderer also gets the column that
/// holds the value, and takes the time zone of a <c>DateTime</c> (<see cref="IDateTimeColumn.TimeZone"/>), the
/// label of an enum (<see cref="IEnumColumn.TryGetLabel"/>) and the bytes of a <c>String</c>
/// (<see cref="IStringColumn.GetBytes"/>; the value is their UTF-8 decoding, with U+FFFD for bytes that are not
/// UTF-8) from the client. The HTTP client gives a <c>String</c> as that decoding only.
/// </para>
/// <para>
/// ClickHouse uses two text forms: at the top level (<c>serializeTextEscaped</c>) strings are escaped, not
/// quoted, and NULL is <c>\N</c>; nested in <c>Array</c>, <c>Tuple</c> and <c>Map</c>
/// (<c>serializeTextQuoted</c>) strings are quoted, and NULL is <c>NULL</c>.
/// </para>
/// </remarks>
internal sealed class ValueRenderer
{
    // The server writes a Time value above 999:59:59 as 999:59:59.
    private const long MaxTimeSeconds = (999 * 3600) + (59 * 60) + 59;

    private static readonly ConcurrentDictionary<Type, (PropertyInfo Key, PropertyInfo Value)> PairProperties = new();
    private static readonly ConcurrentDictionary<string, TimeZoneInfo> Zones = new(StringComparer.Ordinal);

    private readonly Func<TimeZoneInfo>? sessionTimeZone;

    // True for the values of the HTTP client. A Variant or Dynamic value of the TCP client is written only from
    // its column: without its type, a raw count (DateTime, DateTime64, Time64) is not different from an integer.
    private readonly bool valuesFromHttp;

    /// <param name="sessionTimeZone">TCP only (null for HTTP): the time zone of a <c>DateTime</c> raw count without
    /// a time zone in its type, when no column gives one.</param>
    public ValueRenderer(Func<TimeZoneInfo>? sessionTimeZone = null)
    {
        this.sessionTimeZone = sessionTimeZone;
        valuesFromHttp = sessionTimeZone is null;
    }

    /// <summary>
    /// HTTP only: the time zone of the current result, <c>ClickHouseCommand.ServerTimezone</c> (the
    /// <c>X-ClickHouse-Timezone</c> header, which gives the session time zone: <c>SET session_timezone</c> or
    /// the setting of the query). The HTTP client gives a <c>DateTime</c> or <c>DateTime64</c> without a time zone
    /// in its type as its UTC wall clock (<c>DateTimeKind.Unspecified</c>), and its changelog tells the user to
    /// apply the time zone after reading. The renderer applies this zone with the time-zone database that the
    /// client uses for the zones in a type (NodaTime TZDB). Null or UTC: the value is written as it is.
    /// </summary>
    public string? HttpServerTimezone { get; set; }

    /// <summary>The output settings of the current query (<c>date_time_output_format</c>, ...).</summary>
    public OutputSettings Output { get; set; } = OutputSettings.Default;

    /// <summary>Tells if a top-level value of the type is written without escapes by the <c>TSVRaw</c> formats.</summary>
    public static bool IsStringType(ChType t)
    {
        while (Unwrapped(t) is { } inner)
        {
            t = inner;
        }
        return t.Base is "String" or "FixedString" or "Enum8" or "Enum16";
    }

    // The value type of Nullable(T), LowCardinality(T) and SimpleAggregateFunction(f, T), or null.
    private static ChType? Unwrapped(ChType t) => t.Base switch
    {
        "Nullable" or "LowCardinality" => t.TypeArg(0),
        "SimpleAggregateFunction" when t.Args.Count > 0 => t.TypeArg(t.Args.Count - 1),
        _ => null,
    };

    /// <summary>Renders one value.</summary>
    /// <param name="b">The output.</param>
    /// <param name="t">The ClickHouse type of the value.</param>
    /// <param name="v">The value that the client gave.</param>
    /// <param name="nested">True inside <c>Array</c>, <c>Tuple</c> and <c>Map</c>.</param>
    /// <param name="column">TCP only: the column that holds the value, or null.</param>
    /// <param name="row">TCP only: the row of <paramref name="v"/> in <paramref name="column"/>, or -1 when it is
    /// not known (inside a composite value).</param>
    public void Render(Utf8Buffer b, ChType t, object? v, bool nested, IColumn? column = null, int row = -1)
    {
        switch (t.Base)
        {
            case "Nullable":
                Render(b, Arg(t, 0), v, nested, column is INullableColumn n ? n.Inner : column, column is INullableColumn ? row : -1);
                return;
            case "LowCardinality":
                if (column is ILowCardinalityColumn lc)
                {
                    // TCP: row r is row Keys[r] of the dictionary. The dictionary holds the bare inner type, also
                    // for LowCardinality(Nullable(T)) (key 0 is NULL, and then the value is null).
                    var inner = Arg(t, 0);
                    var key = row >= 0 ? lc.Keys[row] : -1;
                    Render(b, inner.Base == "Nullable" ? Arg(inner, 0) : inner, v, nested, lc.Dictionary, key);
                    return;
                }
                Render(b, Arg(t, 0), v, nested, column, -1);
                return;
            case "SimpleAggregateFunction":
                Render(b, Arg(t, t.Args.Count - 1), v, nested, column, row);
                return;
        }

        if (v is null or DBNull)
        {
            b.Append(nested ? "NULL" : Output.NullText);
            return;
        }

        switch (t.Base)
        {
            case "Int8" or "Int16" or "Int32" or "Int64" or "UInt8" or "UInt16" or "UInt32" or "UInt64" or
                "Int128" or "Int256" or "UInt128" or "UInt256":
                b.Append(IntegerText(t, v));
                return;
            case "Float32" or "BFloat16":
                b.Append(v is float f ? FormatFloat(f) : throw Mismatch(t, v));
                return;
            case "Float64":
                b.Append(v is double d ? FormatFloat(d) : throw Mismatch(t, v));
                return;
            case "Bool":
                b.Append(v is bool flag ? (flag ? "true" : "false") : throw Mismatch(t, v));
                return;
            case "String" or "FixedString" or "JSON" or "Json":
                if (t.Base is "JSON" or "Json" && v is JsonNode)
                {
                    throw Unsupported(t, "the client gives " + v.GetType().Name + "; only JSON as String text is supported");
                }
                if (t.Base is "JSON" or "Json" && v is not string)
                {
                    throw Mismatch(t, v);
                }
                // The HTTP client of the shim reads a String as string (no ReadAsByteArray), so a byte[] is an
                // Array(UInt8) there. (The TCP client gives a FixedString as byte[].)
                if (valuesFromHttp && t.Base == "String" && v is byte[])
                {
                    throw Mismatch(t, v);
                }
                // TCP: the bytes of the String from its column, also when they are not UTF-8.
                if (TryColumnBytes(t, v, column, row, out var fromColumn))
                {
                    b.AppendEscaped(fromColumn, nested);
                    return;
                }
                RenderStringLike(b, t, v, nested);
                return;
            case "Enum8" or "Enum16":
                b.AppendEscaped(EnumLabel(t, v, column), nested);
                return;
            case "Decimal" or "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256":
                b.Append(DecimalText(t, v));
                return;
            case "Date" or "Date32":
                b.AppendQuotedIf(DateText(t, v), nested);
                return;
            case "DateTime":
                b.AppendQuotedIf(DateTimeText(t, v, column, 0), nested);
                return;
            case "DateTime64":
                b.AppendQuotedIf(DateTimeText(t, v, column, Precision(t, 0)), nested);
                return;
            case "Time":
                b.AppendQuotedIf(TimeText(t, v, 0), nested);
                return;
            case "Time64":
                b.AppendQuotedIf(TimeText(t, v, Precision(t, 0)), nested);
                return;
            case "UUID":
                b.AppendQuotedIf(v is Guid g ? g.ToString("D") : throw Mismatch(t, v), nested);
                return;
            case "IPv4":
                b.AppendQuotedIf(v is IPAddress { AddressFamily: AddressFamily.InterNetwork } ip4 ? ip4.ToString() : throw Mismatch(t, v), nested);
                return;
            case "IPv6":
                b.AppendQuotedIf(v is IPAddress ip6 ? FormatIPv6(ip6.MapToIPv6().GetAddressBytes()) : throw Mismatch(t, v), nested);
                return;
            case "Nothing":
                // A `Nothing` column has no values; a value here is a NULL.
                b.Append(nested ? "NULL" : Output.NullText);
                return;
            case "Array":
                // Row i of an array column holds the inner rows [Offsets[i], Offsets[i + 1]).
                RenderSequence(b, Arg(t, 0), v, t, column is IArrayColumn a ? a.Inner : null, column is IArrayColumn ar && row >= 0 ? ar.Offsets[row] : -1);
                return;
            case "QBit":
                RenderSequence(b, Arg(t, 0), v, t, null, -1);
                return;
            case "Tuple":
                RenderTuple(b, t, v, column as ITupleColumn, row);
                return;
            case "Map":
                RenderMap(b, t, v, column as IMapColumn, row);
                return;
            case "Nested":
                if (column is INestedColumn nestedColumn && row >= 0)
                {
                    RenderNested(b, t, v, nestedColumn, row);
                    return;
                }
                // Nested(a T1, b T2) is an Array(Tuple(T1, T2)).
                RenderSequence(b, new ChType("Tuple", t.Args), v, t, null, -1);
                return;
            case "Point":
                RenderTuple(b, new ChType("Tuple", [new(Type: new ChType("Float64")), new(Type: new ChType("Float64"))]), v, null, -1);
                return;
            case "Ring" or "LineString":
                RenderSequence(b, new ChType("Point"), v, t, null, -1);
                return;
            case "Polygon" or "MultiLineString":
                RenderSequence(b, new ChType("Ring"), v, t, null, -1);
                return;
            case "MultiPolygon":
                RenderSequence(b, new ChType("Polygon"), v, t, null, -1);
                return;
            case "Variant":
                RenderVariant(b, t, VariantAlternatives(t), v, nested, column, row);
                return;
            case "Geometry":
                // `Geometry` is a Variant of the six geometry types, in the server's discriminator order.
                RenderVariant(b, t, GeometryAlternatives, v, nested, column, row);
                return;
            case "Dynamic":
                RenderDynamic(b, t, v, nested, column, row);
                return;
        }
        if (t.Base.StartsWith("Interval", StringComparison.Ordinal))
        {
            b.Append(IntegerText(t, v));
            return;
        }
        throw Unsupported(t);
    }

    /// <summary>
    /// Writes <paramref name="f"/> as ClickHouse does: the shortest text that reads back as the same value, in
    /// positional notation when the decimal exponent is in [-6, 21), else in scientific notation without a
    /// <c>+</c> sign or exponent padding (<c>1e21</c>, <c>1.5e-7</c>). This is the same layout as
    /// JavaScript's <c>Number.prototype.toString</c>.
    /// </summary>
    public static string FormatFloat(double f) => FormatFloat(f, f.ToString("R", CultureInfo.InvariantCulture));

    /// <inheritdoc cref="FormatFloat(double)"/>
    public static string FormatFloat(float f) => FormatFloat(f, f.ToString("R", CultureInfo.InvariantCulture));

    /// <summary>
    /// Follows ClickHouse's <c>formatIPv6</c>: the longest run of two or more zero words becomes <c>::</c>, and
    /// an IPv4-mapped (<c>::ffff:a.b.c.d</c>) or IPv4-compatible (<c>::a.b.c.d</c>) address ends in dotted
    /// form.
    /// </summary>
    public static string FormatIPv6(byte[] ip)
    {
        var words = new int[8];
        for (var i = 0; i < 8; i++)
        {
            words[i] = (ip[2 * i] << 8) | ip[(2 * i) + 1];
        }
        int bestBase = -1, bestLen = 0;
        for (var i = 0; i < 8;)
        {
            if (words[i] != 0)
            {
                i++;
                continue;
            }
            var j = i;
            while (j < 8 && words[j] == 0)
            {
                j++;
            }
            if (j - i > bestLen)
            {
                bestBase = i;
                bestLen = j - i;
            }
            i = j;
        }
        if (bestLen < 2)
        {
            bestBase = -1;
        }
        var ipv4Compatible = bestBase == 0 && bestLen == 6;
        var ipv4Mapped = bestBase == 0 && bestLen == 5 && words[5] == 0xffff;
        var sb = new System.Text.StringBuilder();
        for (var i = 0; i < 8; i++)
        {
            if (bestBase != -1 && i >= bestBase && i < bestBase + bestLen)
            {
                if (i == bestBase)
                {
                    sb.Append(':');
                }
                continue;
            }
            if (i != 0)
            {
                sb.Append(':');
            }
            if (i == 6 && (ipv4Compatible || ipv4Mapped))
            {
                sb.Append(CultureInfo.InvariantCulture, $"{ip[12]}.{ip[13]}.{ip[14]}.{ip[15]}");
                return sb.ToString();
            }
            sb.Append(words[i].ToString("x", CultureInfo.InvariantCulture));
        }
        if (bestBase != -1 && bestBase + bestLen == 8)
        {
            sb.Append(':');
        }
        return sb.ToString();
    }

    /// <summary>
    /// Writes a <c>Time</c> / <c>Time64</c> value given as seconds and a fraction of <paramref name="precision"/>
    /// digits: <c>[-]HH:MM:SS[.fff]</c>. The hours can be more than 24.
    /// </summary>
    public static string FormatTime(bool negative, long seconds, long fraction, int precision)
    {
        // The server writes a value above 999:59:59 as 999:59:59, with a zero fraction; the stored value is
        // not changed.
        if (seconds > MaxTimeSeconds)
        {
            seconds = MaxTimeSeconds;
            fraction = 0;
        }
        var text = string.Create(CultureInfo.InvariantCulture, $"{(negative ? "-" : string.Empty)}{seconds / 3600:00}:{seconds / 60 % 60:00}:{seconds % 60:00}");
        return precision > 0 ? text + "." + fraction.ToString(CultureInfo.InvariantCulture).PadLeft(precision, '0') : text;
    }

    internal static TimeZoneInfo FindZone(string name) => Zones.GetOrAdd(name, TimeZoneInfo.FindSystemTimeZoneById);

    private static string FormatFloat(double f, string shortest)
    {
        if (double.IsNaN(f))
        {
            return "nan";
        }
        if (double.IsPositiveInfinity(f))
        {
            return "inf";
        }
        if (double.IsNegativeInfinity(f))
        {
            return "-inf";
        }
        if (f == 0)
        {
            return double.IsNegative(f) ? "-0" : "0";
        }
        // The shortest round-trip text of .NET, such as 123.45, 1E+21, 1.5E-07 or 0.0001.
        var neg = shortest.StartsWith('-');
        var s = neg ? shortest[1..] : shortest;
        var ePos = s.IndexOfAny(['E', 'e']);
        var mantissa = ePos >= 0 ? s[..ePos] : s;
        var exp = ePos >= 0 ? int.Parse(s[(ePos + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture) : 0;
        var point = mantissa.IndexOf('.', StringComparison.Ordinal);
        var intLen = point >= 0 ? point : mantissa.Length;
        var digits = mantissa.Replace(".", string.Empty, StringComparison.Ordinal);
        var leadingZeros = digits.Length - digits.TrimStart('0').Length;
        digits = digits.Trim('0');
        // The value is 0.digits × 10^n.
        var n = intLen + exp - leadingZeros;
        var k = digits.Length;
        string text;
        if (k <= n && n <= 21)
        {
            text = digits + new string('0', n - k);
        }
        else if (n > 0 && n <= 21)
        {
            text = digits[..n] + "." + digits[n..];
        }
        else if (n > -6 && n <= 0)
        {
            text = "0." + new string('0', -n) + digits;
        }
        else
        {
            text = digits[..1] + (k > 1 ? "." + digits[1..] : string.Empty) + "e" + (n - 1).ToString(CultureInfo.InvariantCulture);
        }
        return neg ? "-" + text : text;
    }

    private static ChType Arg(ChType t, int index) => t.TypeArg(index) ?? throw Unsupported(t);

    private static int Precision(ChType t, int index) =>
        int.TryParse(t.LiteralArg(index), NumberStyles.Integer, CultureInfo.InvariantCulture, out var p) && p is >= 0 and <= 9 ? p : throw Unsupported(t);

    private static NotSupportedException Unsupported(ChType t, string? why = null) =>
        new("clickhouse-test-runner: no TabSeparated renderer for type " + t + (why is null ? string.Empty : " (" + why + ")"));

    private static InvalidOperationException Mismatch(ChType t, object v) =>
        new($"clickhouse-test-runner: unexpected .NET value {v.GetType()} for type {t}");

    private static string IntegerText(ChType t, object v) => v switch
    {
        sbyte or byte or short or ushort or int or uint or long or ulong or Int128 or UInt128 or BigInteger =>
            ((IFormattable)v).ToString(null, CultureInfo.InvariantCulture),
        Int256 or UInt256 => v.ToString()!,
        _ => throw Mismatch(t, v),
    };

    private string DecimalText(ChType t, object v)
    {
        if (v is not (decimal or IFormattable))
        {
            throw Mismatch(t, v);
        }
        var s = ((IFormattable)v).ToString(null, CultureInfo.InvariantCulture);
        if (v is not (decimal or ClickHouse.Driver.Numerics.ClickHouseDecimal or ClickHouseTcpDecimal) ||
            s.Contains('E', StringComparison.OrdinalIgnoreCase))
        {
            throw Mismatch(t, v);
        }
        // The server writes no trailing zeros (output_format_decimal_trailing_zeros = 0).
        if (s.Contains('.', StringComparison.Ordinal))
        {
            s = s.TrimEnd('0').TrimEnd('.');
        }
        s = s == "-0" ? "0" : s;
        if (Output.DecimalTrailingZeros && DecimalScale(t) is int scale and > 0)
        {
            // output_format_decimal_trailing_zeros = 1: exactly `scale` fractional digits.
            var dot = s.IndexOf('.', StringComparison.Ordinal);
            s = dot < 0 ? s + "." + new string('0', scale) : s + new string('0', scale - (s.Length - dot - 1));
        }
        return s;
    }

    // The scale of Decimal(P, S) or DecimalN(S), or null for another type.
    private static int? DecimalScale(ChType t) => t.Base switch
    {
        "Decimal" when int.TryParse(t.LiteralArg(1), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s) => s,
        "Decimal" => 0,
        "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256" when int.TryParse(t.LiteralArg(0), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s) => s,
        _ => null,
    };

    private static string DateText(ChType t, object v) => v switch
    {
        DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        DateTime dt => dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        _ => throw Mismatch(t, v),
    };

    private static string FormatWallClock(DateTime dt, long fraction, int precision)
    {
        var s = dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        return precision > 0 ? s + "." + fraction.ToString(CultureInfo.InvariantCulture).PadLeft(precision, '0') : s;
    }

    private static long Pow10(int p)
    {
        long r = 1;
        for (var i = 0; i < p; i++)
        {
            r *= 10;
        }
        return r;
    }

    private static string TimeText(ChType t, object v, int precision)
    {
        switch (v)
        {
            case TimeSpan ts:
            {
                // The HTTP client: 100-nanosecond ticks.
                var ticks = ts.Ticks;
                var negative = ticks < 0;
                var abs = negative ? -(BigInteger)ticks : ticks;
                var seconds = (long)(abs / TimeSpan.TicksPerSecond);
                var fraction7 = (long)(abs % TimeSpan.TicksPerSecond);
                return FormatTime(negative, seconds, ScaleFraction(fraction7, 7, precision), precision);
            }
            case int or long:
            {
                // The TCP client: the raw count at the scale of the type.
                var count = Convert.ToInt64(v, CultureInfo.InvariantCulture);
                var negative = count < 0;
                var abs = BigInteger.Abs(count);
                var scale = Pow10(precision);
                return FormatTime(negative, (long)(abs / scale), (long)(abs % scale), precision);
            }
            default:
                throw Mismatch(t, v);
        }
    }

    // Converts a fraction of `from` digits to `to` digits (cutting or padding with zeros).
    private static long ScaleFraction(long fraction, int from, int to) =>
        to >= from ? fraction * Pow10(to - from) : fraction / Pow10(from - to);

    /// <summary>
    /// Writes a top-level value of a string type (<see cref="IsStringType"/>) without escapes, for the
    /// <c>TSVRaw</c> formats: the bytes of a <c>String</c> (TCP: from the column), a <c>FixedString</c> or the label
    /// of an enum. Returns false when the value is not a string or a <c>byte[]</c> (the TCP client gives an enum as
    /// its ordinal); then write it with <see cref="Render"/>.
    /// </summary>
    public bool TryRenderRaw(Utf8Buffer b, ChType t, object? v, IColumn? column, int row)
    {
        // The same column and row as Render takes for the inner type.
        while (Unwrapped(t) is { } inner)
        {
            if (column is ILowCardinalityColumn lc && t.Base == "LowCardinality")
            {
                row = row >= 0 ? lc.Keys[row] : -1;
                column = lc.Dictionary;
                inner = inner.Base == "Nullable" ? inner.TypeArg(0) ?? inner : inner;
            }
            else if (column is INullableColumn n && t.Base == "Nullable")
            {
                column = n.Inner;
            }
            else if (t.Base != "SimpleAggregateFunction")
            {
                row = -1;
            }
            t = inner;
        }
        switch (v)
        {
            case string when TryColumnBytes(t, v, column, row, out var fromColumn):
                b.Append(fromColumn);
                return true;
            case sbyte or short when t.Base is "Enum8" or "Enum16":
                // The TCP client gives an enum as its ordinal.
                b.Append(EnumLabel(t, v, column));
                return true;
            case string s:
                b.Append(s);
                return true;
            case byte[] bytes:
                b.Append(bytes);
                return true;
            default:
                return false;
        }
    }

    // TCP: the bytes of a String value (also of JSON read as text) from its column (IStringColumn.GetBytes), when
    // the value has U+FFFD: the value of the column is the UTF-8 decoding of the bytes, with U+FFFD for a byte that
    // is not UTF-8, so only such a value can differ from its bytes. The value must be the decoding of the bytes
    // (a check of the row; it cannot tell two rows apart that decode to the same text).
    private static bool TryColumnBytes(ChType t, object v, IColumn? column, int row, out ReadOnlySpan<byte> bytes)
    {
        if (t.Base is not ("String" or "JSON" or "Json") || column is not IStringColumn strings || row < 0 ||
            v is not string s || !s.Contains('\uFFFD', StringComparison.Ordinal))
        {
            bytes = default;
            return false;
        }
        bytes = strings.GetBytes(row);
        if (System.Text.Encoding.UTF8.GetString(bytes) != s)
        {
            throw new InvalidOperationException($"clickhouse-test-runner: row {row} of the String column does not hold the value of the client");
        }
        return true;
    }

    private static void RenderStringLike(Utf8Buffer b, ChType t, object v, bool nested)
    {
        switch (v)
        {
            case string s:
                b.AppendEscaped(s, nested);
                break;
            case byte[] bytes:
                b.AppendEscaped(bytes, nested);
                break;
            default:
                throw Mismatch(t, v);
        }
    }

    private static string EnumLabel(ChType t, object v, IColumn? column)
    {
        if (v is string label)
        {
            return label;
        }
        if (v is not (sbyte or short or int or long))
        {
            throw Mismatch(t, v);
        }
        var ordinal = Convert.ToInt64(v, CultureInfo.InvariantCulture);
        if (column is IEnumColumn e)
        {
            return e.TryGetLabel(ordinal, out var fromClient) ? fromClient : throw new InvalidOperationException($"clickhouse-test-runner: the client has no label for {ordinal} in {t}");
        }
        // No column (inside a composite value that the client gave without its column): use the type.
        foreach (var arg in t.Args)
        {
            var eq = arg.Literal.LastIndexOf('=');
            if (eq > 0 && long.TryParse(arg.Literal[(eq + 1)..].Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var n) && n == ordinal)
            {
                var quoted = arg.Literal[..eq].Trim();
                return Identifiers.Unquote(quoted.Replace("\\'", "''", StringComparison.Ordinal)) ?? quoted;
            }
        }
        throw new InvalidOperationException($"clickhouse-test-runner: no label for {ordinal} in {t}");
    }

    private string DateTimeText(ChType t, object v, IColumn? column, int precision)
    {
        var text = DateTimeTextAtPrecision(t, v, column, precision);
        if (!Output.CutDateTime64TrailingZeros || precision == 0 || Output.DateTimeFormat != DateTimeOutputFormat.Simple)
        {
            return text;
        }
        // date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands: the fraction without its
        // trailing zeros, padded to a multiple of three digits (`.120000` -> `.120`, `.1` of DateTime64(1) ->
        // `.100`, `.000000` -> nothing).
        var digits = text[^precision..].TrimEnd('0');
        var whole = text[..^(precision + 1)];
        return digits.Length == 0 ? whole : whole + "." + digits.PadRight((digits.Length + 2) / 3 * 3, '0');
    }

    private string DateTimeTextAtPrecision(ChType t, object v, IColumn? column, int precision)
    {
        if (Output.DateTimeFormat != DateTimeOutputFormat.Simple)
        {
            var (utcSeconds, fraction) = UtcParts(t, v, precision);
            return Output.DateTimeFormat == DateTimeOutputFormat.Iso
                ? WallClockText(utcSeconds, TimeZoneInfo.Utc).Replace(' ', 'T') + FractionText(fraction, precision) + "Z"
                : UnixTimestampText(utcSeconds, fraction, precision);
        }
        switch (v)
        {
            case DateTime dt:
            {
                // The HTTP client gives the wall-clock time of the column's time zone, or the UTC wall clock
                // when the type has no time zone.
                if (valuesFromHttp && TypeZoneName(t).Length == 0)
                {
                    dt = InHttpServerZone(dt);
                }
                var fraction7 = dt.Ticks % TimeSpan.TicksPerSecond;
                return FormatWallClock(new DateTime(dt.Ticks - fraction7, DateTimeKind.Unspecified), ScaleFraction(fraction7, 7, precision), precision);
            }
            case DateTimeOffset dto:
            {
                var fraction7 = dto.Ticks % TimeSpan.TicksPerSecond;
                return FormatWallClock(new DateTime(dto.DateTime.Ticks - fraction7, DateTimeKind.Unspecified), ScaleFraction(fraction7, 7, precision), precision);
            }
            case uint or long:
            {
                // The TCP client gives the raw count: seconds (DateTime) or units of 10^-precision seconds
                // (DateTime64), in the time zone that the client resolved for the column.
                var (seconds, fraction) = CountParts(v, precision);
                var zone = (column as IDateTimeColumn)?.TimeZone ?? TypeZone(t) ?? sessionTimeZone?.Invoke() ?? TimeZoneInfo.Utc;
                return WallClockText(seconds, zone) + FractionText(fraction, precision);
            }
            default:
                throw Mismatch(t, v);
        }
    }

    private static string FractionText(long fraction, int precision) =>
        precision > 0 ? "." + fraction.ToString(CultureInfo.InvariantCulture).PadLeft(precision, '0') : string.Empty;

    // A raw count of the TCP client as whole seconds since the epoch and the fraction (0 <= fraction < 10^precision).
    private static (long Seconds, long Fraction) CountParts(object v, int precision)
    {
        var count = Convert.ToInt64(v, CultureInfo.InvariantCulture);
        var scale = Pow10(precision);
        var seconds = count / scale;
        var fraction = count % scale;
        if (fraction < 0)
        {
            seconds--;
            fraction += scale;
        }
        return (seconds, fraction);
    }

    // The instant of a value as whole seconds since the epoch and the fraction at the precision of the type.
    private static (long Seconds, long Fraction) UtcParts(ChType t, object v, int precision)
    {
        DateTime utc;
        switch (v)
        {
            case uint or long:
                return CountParts(v, precision);
            case DateTimeOffset dto:
                utc = dto.UtcDateTime;
                break;
            case DateTime dt when dt.Kind == DateTimeKind.Utc || TypeZoneName(t).Length == 0:
                // HTTP: a type without a time zone gives the UTC wall clock.
                utc = dt;
                break;
            case DateTime dt:
            {
                // HTTP: the wall clock in the zone of the type, with the time-zone database of the client.
                var name = TypeZoneName(t);
                var zone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(name)
                    ?? throw new NotSupportedException($"clickhouse-test-runner: the time zone database of the client has no zone {name}");
                utc = LocalDateTime.FromDateTime(dt).InZoneLeniently(zone).ToDateTimeUtc();
                break;
            }
            default:
                throw Mismatch(t, v);
        }
        var ticks = utc.Ticks - DateTime.UnixEpoch.Ticks;
        var seconds = Math.DivRem(ticks, TimeSpan.TicksPerSecond, out var fraction7);
        if (fraction7 < 0)
        {
            seconds--;
            fraction7 += TimeSpan.TicksPerSecond;
        }
        return (seconds, ScaleFraction(fraction7, 7, precision));
    }

    // date_time_output_format = 'unix_timestamp': the count as a decimal number with `precision` digits.
    private static string UnixTimestampText(long seconds, long fraction, int precision)
    {
        if (precision == 0)
        {
            return seconds.ToString(CultureInfo.InvariantCulture);
        }
        var scale = Pow10(precision);
        var count = ((Int128)seconds * scale) + fraction;
        var negative = count < 0;
        var abs = negative ? -count : count;
        var whole = abs / scale;
        var frac = (long)(abs % scale);
        return (negative ? "-" : string.Empty) + whole.ToString(CultureInfo.InvariantCulture) + FractionText(frac, precision);
    }

    // The range of the server's text: 0000-01-01 00:00:00 .. 9999-12-31 23:59:59 UTC
    // (DateLUTImpl::min_representable_time, max_representable_time).
    private const long MinRepresentableTime = -62167219200;
    private const long MaxRepresentableTime = 253402300799;
    private const long SecondsPerDay = 86400;

    /// <summary>
    /// Writes the wall clock of a Unix time in a zone as the server does (DateLUTImpl::toDateTimeComponents and
    /// its out-of-range path): a DateTime64 count can be outside the range of the .NET date types (year 0000, or
    /// after 9999-12-31). The server bounds the instant to the representable window and two days, converts it
    /// to the local civil time (proleptic Gregorian calendar, with a year 0000), and then clamps a year outside
    /// [0000, 9999] to 0000-01-01 00:00:00 or 9999-12-31 23:59:59.
    /// </summary>
    private static string WallClockText(long seconds, TimeZoneInfo zone)
    {
        seconds = Math.Clamp(seconds, MinRepresentableTime - (2 * SecondsPerDay), MaxRepresentableTime + (2 * SecondsPerDay));
        // The offset at the instant; outside the .NET range, the offset at the nearest end of that range.
        var probe = DateTimeOffset.FromUnixTimeSeconds(Math.Clamp(seconds, DateTimeOffset.MinValue.ToUnixTimeSeconds(), DateTimeOffset.MaxValue.ToUnixTimeSeconds()));
        var local = seconds + (long)zone.GetUtcOffset(probe).TotalSeconds;
        var days = Math.DivRem(local, SecondsPerDay, out var secondOfDay);
        if (secondOfDay < 0)
        {
            days--;
            secondOfDay += SecondsPerDay;
        }
        var (year, month, day) = CivilFromDays(days);
        if (year > 9999)
        {
            return "9999-12-31 23:59:59";
        }
        if (year < 0)
        {
            return "0000-01-01 00:00:00";
        }
        return string.Create(CultureInfo.InvariantCulture, $"{year:D4}-{month:D2}-{day:D2} {secondOfDay / 3600:D2}:{secondOfDay / 60 % 60:D2}:{secondOfDay % 60:D2}");
    }

    // Days since 1970-01-01 -> proleptic Gregorian date with a year 0 (H. Hinnant, "civil_from_days").
    private static (long Year, int Month, int Day) CivilFromDays(long z)
    {
        z += 719468;
        var era = (z >= 0 ? z : z - 146096) / 146097;
        var doe = z - (era * 146097);
        var yoe = (doe - (doe / 1460) + (doe / 36524) - (doe / 146096)) / 365;
        var doy = doe - ((365 * yoe) + (yoe / 4) - (yoe / 100));
        var mp = ((5 * doy) + 2) / 153;
        var day = (int)(doy - (((153 * mp) + 2) / 5) + 1);
        var month = (int)(mp < 10 ? mp + 3 : mp - 9);
        var year = yoe + (era * 400) + (month <= 2 ? 1 : 0);
        return (year, month, day);
    }

    private static TimeZoneInfo? TypeZone(ChType t)
    {
        var name = TypeZoneName(t);
        return name.Length == 0 ? null : FindZone(name);
    }

    private static string TypeZoneName(ChType t) => t.LiteralArg(t.Base == "DateTime64" ? 1 : 0);

    // The wall clock of a UTC wall clock in the zone of HttpServerTimezone.
    private DateTime InHttpServerZone(DateTime utcWallClock)
    {
        var name = HttpServerTimezone;
        if (string.IsNullOrEmpty(name) || name == "UTC")
        {
            return utcWallClock;
        }
        var zone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(name)
            ?? throw new NotSupportedException($"clickhouse-test-runner: the time zone database of the client has no zone {name}");
        return Instant.FromDateTimeUtc(DateTime.SpecifyKind(utcWallClock, DateTimeKind.Utc)).InZone(zone).ToDateTimeUnspecified();
    }

    // The alternatives of `Geometry`, in the order of the server's discriminators (sorted by name).
    private static readonly IReadOnlyList<ChType> GeometryAlternatives =
        [new("LineString"), new("MultiLineString"), new("MultiPolygon"), new("Point"), new("Polygon"), new("Ring")];

    private static readonly ConcurrentDictionary<Type, bool> PairSequenceTypes = new();

    private static IReadOnlyList<ChType> VariantAlternatives(ChType t) =>
        t.Args.Select(a => a.Type ?? throw Unsupported(t)).ToList();

    // Tells if the value is a sequence of KeyValuePair: the map form of both clients (MapReadMode.KeyValuePairs,
    // and KeyValuePair[] of the TCP client). The element type tells it also for an empty map.
    private static bool IsPairSequence(object v) => PairSequenceTypes.GetOrAdd(v.GetType(), static type =>
        type.GetInterfaces().Append(type).Any(static i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>) &&
            i.GetGenericArguments()[0] is { IsGenericType: true } e && e.GetGenericTypeDefinition() == typeof(KeyValuePair<,>)));

    private (object? Key, object? Value) Pair(object? pair, ChType t, object v)
    {
        if (pair is null)
        {
            throw Mismatch(t, v);
        }
        var (key, value) = PairProperties.GetOrAdd(pair.GetType(), static type =>
            (type.GetProperty("Key") ?? throw new InvalidOperationException("no Key in " + type),
             type.GetProperty("Value") ?? throw new InvalidOperationException("no Value in " + type)));
        return (key.GetValue(pair), value.GetValue(pair));
    }

    private void RenderSequence(Utf8Buffer b, ChType inner, object v, ChType t, IColumn? innerColumn, int firstRow)
    {
        // A map (a Dictionary, or a sequence of key-value pairs) and a JSON value are not arrays, also when empty.
        if (v is string or IDictionary or JsonNode || v is not IEnumerable items || IsPairSequence(v))
        {
            throw Mismatch(t, v);
        }
        b.Append((byte)'[');
        var index = 0;
        foreach (var item in items)
        {
            if (index > 0)
            {
                b.Append((byte)',');
            }
            // TCP: element i of the row is row firstRow + i of the inner column.
            Render(b, inner, item, true, innerColumn, innerColumn is not null && firstRow >= 0 ? firstRow + index : -1);
            index++;
        }
        b.Append((byte)']');
    }

    private void RenderTuple(Utf8Buffer b, ChType t, object v, ITupleColumn? column, int row)
    {
        IReadOnlyList<object?> items = v switch
        {
            ITuple tuple => Enumerable.Range(0, tuple.Length).Select(i => tuple[i]).ToList(),
            object?[] array => array,
            _ => throw Mismatch(t, v),
        };
        if (items.Count != t.Args.Count)
        {
            throw Mismatch(t, v);
        }
        b.Append((byte)'(');
        for (var i = 0; i < items.Count; i++)
        {
            if (i > 0)
            {
                b.Append((byte)',');
            }
            var elementType = t.Args[i].Type ?? throw Unsupported(t);
            // TCP: the element columns have the rows of the tuple column.
            var child = column is not null && i < column.Children.Count ? column.Children[i] : null;
            Render(b, elementType, items[i], true, child, child is null ? -1 : row);
        }
        b.Append((byte)')');
    }

    // TCP: Nested(a T1, b T2) row r holds the records Offsets[r] .. Offsets[r + 1] - 1, one row of each field
    // column; the client gives the row as object[][] (a record is an object[] of its fields).
    private void RenderNested(Utf8Buffer b, ChType t, object v, INestedColumn column, int row)
    {
        if (v is not object?[][] records || column.FieldCount != t.Args.Count)
        {
            throw Mismatch(t, v);
        }
        var first = column.Offsets[row];
        b.Append((byte)'[');
        for (var i = 0; i < records.Length; i++)
        {
            if (i > 0)
            {
                b.Append((byte)',');
            }
            if (records[i].Length != t.Args.Count)
            {
                throw Mismatch(t, v);
            }
            b.Append((byte)'(');
            for (var j = 0; j < records[i].Length; j++)
            {
                if (j > 0)
                {
                    b.Append((byte)',');
                }
                Render(b, t.Args[j].Type ?? throw Unsupported(t), records[i][j], true, column.GetField(j), first + i);
            }
            b.Append((byte)')');
        }
        b.Append((byte)']');
    }

    private void RenderMap(Utf8Buffer b, ChType t, object v, IMapColumn? column, int row)
    {
        var kt = Arg(t, 0);
        var vt = Arg(t, 1);
        // TCP: entry i of the row is row Offsets[row] + i of the key and value columns.
        var entryRow = column is not null && row >= 0 ? column.Offsets[row] : -1;
        b.Append((byte)'{');
        var first = true;
        void Entry(object? key, object? value)
        {
            if (!first)
            {
                b.Append((byte)',');
            }
            first = false;
            Render(b, kt, key, true, column?.KeyColumn, entryRow);
            b.Append((byte)':');
            Render(b, vt, value, true, column?.ValueColumn, entryRow);
            if (entryRow >= 0)
            {
                entryRow++;
            }
        }
        switch (v)
        {
            case IDictionary dictionary:
                // A Dictionary has no defined order: only a map with less than two entries has one text.
                if (dictionary.Count > 1)
                {
                    throw Unsupported(t, "the client gives a Dictionary, which does not keep the server order");
                }
                foreach (DictionaryEntry e in dictionary)
                {
                    Entry(e.Key, e.Value);
                }
                break;
            case IEnumerable pairs when v is not JsonNode && IsPairSequence(v):
                // MapReadMode.KeyValuePairs (HTTP) and the TCP client keep the server order.
                foreach (var pair in pairs)
                {
                    var (key, value) = Pair(pair, t, v);
                    Entry(key, value);
                }
                break;
            default:
                throw Mismatch(t, v);
        }
        b.Append((byte)'}');
    }

    private void RenderVariant(Utf8Buffer b, ChType t, IReadOnlyList<ChType> alternatives, object v, bool nested, IColumn? column, int row)
    {
        if (column is IVariantColumn variant && row >= 0)
        {
            // TCP: the column tells the alternative of each row.
            var discriminator = variant.Discriminators[row];
            if (discriminator == IVariantColumn.NullDiscriminator)
            {
                b.Append(nested ? "NULL" : Output.NullText);
                return;
            }
            Render(b, ChType.Parse(variant.TypeNames[discriminator]), v, nested, variant.GetTypeColumn(discriminator), variant.LocalIndices[row]);
            return;
        }
        if (column is not null || !valuesFromHttp)
        {
            throw Unsupported(t, "the client does not give the alternative of a Variant value here");
        }
        RenderByAlternatives(b, t, alternatives, v, nested);
    }

    /// <summary>
    /// Writes a Variant value of the HTTP client, which gives the value of the alternative, but not the
    /// alternative. The value has one text when each alternative that accepts its .NET type writes the same text
    /// for it: a string fits <c>String</c> and an enum with the same text, a list of points fits
    /// <c>LineString</c> and <c>Ring</c>. When two alternatives that accept it write different texts (a
    /// <c>DateTime</c> fits <c>Date</c> and <c>DateTime</c>), or when the shim has no renderer for an alternative,
    /// the text is not known, and the shim reports it.
    /// </summary>
    private void RenderByAlternatives(Utf8Buffer b, ChType t, IReadOnlyList<ChType> alternatives, object v, bool nested)
    {
        Utf8Buffer? text = null;
        ChType? fit = null;
        NotSupportedException? gap = null;
        foreach (var alternative in alternatives)
        {
            var candidate = new Utf8Buffer();
            try
            {
                Render(candidate, alternative, v, nested);
            }
            catch (NotSupportedException e)
            {
                // The shim cannot write this alternative, so it cannot tell if the value is of it.
                gap ??= e;
                continue;
            }
            catch (Exception e) when (e is InvalidOperationException or InvalidCastException or ArgumentException or FormatException or OverflowException)
            {
                // The .NET value is not of this alternative.
                continue;
            }
            if (text is null)
            {
                text = candidate;
                fit = alternative;
            }
            else if (!text.Span.SequenceEqual(candidate.Span))
            {
                throw Unsupported(t, $"the client does not give the alternative; a {v.GetType().Name} fits {fit} and {alternative}, which have different texts");
            }
        }
        if (text is null)
        {
            throw gap ?? (Exception)Mismatch(t, v);
        }
        if (gap is not null)
        {
            throw Unsupported(t, $"the client does not give the alternative; a {v.GetType().Name} fits {fit}, and the shim cannot write all alternatives ({gap.Message})");
        }
        b.Append(text);
    }

    private void RenderDynamic(Utf8Buffer b, ChType t, object v, bool nested, IColumn? column, int row)
    {
        if (column is IDynamicColumn dynamic && row >= 0)
        {
            // TCP: the column tells the type of each row.
            var discriminator = dynamic.Discriminators[row];
            if (discriminator < 0 || discriminator >= dynamic.TypeCount)
            {
                b.Append(nested ? "NULL" : Output.NullText);
                return;
            }
            Render(b, ChType.Parse(dynamic.TypeNames[discriminator]), v, nested, dynamic.GetTypeColumn(discriminator), dynamic.LocalIndices[row]);
            return;
        }
        if (column is not null || !valuesFromHttp)
        {
            throw Unsupported(t, "the client does not give the type of a Dynamic value here");
        }
        RenderByValue(b, t, v, nested);
    }

    /// <summary>
    /// Writes a Dynamic value of the HTTP client, which gives the value without its type. The .NET type gives the
    /// text when each ClickHouse type that the client reads as that .NET type has the same text for the value:
    /// all integer types, <c>Float32</c> (float), <c>Float64</c> (double), <c>Bool</c>, <c>String</c>,
    /// <c>FixedString</c> and the enums (string), <c>UUID</c>, <c>IPv4</c> and <c>IPv6</c>, the decimals, and
    /// arrays, tuples and maps of these. A <c>DateTime</c> (<c>Date</c>, <c>Date32</c>, <c>DateTime</c>,
    /// <c>DateTime64(P)</c>), a <c>TimeSpan</c> (<c>Time</c>, <c>Time64(P)</c>) and a JSON value have no single
    /// text, and the shim reports them.
    /// </summary>
    private void RenderByValue(Utf8Buffer b, ChType t, object? v, bool nested)
    {
        switch (v)
        {
            case null or DBNull:
                b.Append(nested ? "NULL" : Output.NullText);
                return;
            case JsonNode:
                throw Unsupported(t, "the client gives the value without its type, as " + v.GetType().Name);
            case string:
                Render(b, new ChType("String"), v, nested);
                return;
            case bool:
                Render(b, new ChType("Bool"), v, nested);
                return;
            case sbyte or byte or short or ushort or int or uint or long or ulong or Int128 or UInt128 or BigInteger or Int256 or UInt256:
                b.Append(IntegerText(t, v));
                return;
            case float:
                Render(b, new ChType("Float32"), v, nested);
                return;
            case double:
                Render(b, new ChType("Float64"), v, nested);
                return;
            case decimal or ClickHouse.Driver.Numerics.ClickHouseDecimal:
                b.Append(DecimalText(t, v));
                return;
            case Guid:
                Render(b, new ChType("UUID"), v, nested);
                return;
            case IPAddress ip:
                Render(b, new ChType(ip.AddressFamily == AddressFamily.InterNetwork ? "IPv4" : "IPv6"), v, nested);
                return;
            case ITuple tuple:
                b.Append((byte)'(');
                for (var i = 0; i < tuple.Length; i++)
                {
                    if (i > 0)
                    {
                        b.Append((byte)',');
                    }
                    RenderByValue(b, t, tuple[i], true);
                }
                b.Append((byte)')');
                return;
            case IDictionary dictionary:
                if (dictionary.Count > 1)
                {
                    throw Unsupported(t, "the client gives a Dictionary, which does not keep the server order");
                }
                b.Append((byte)'{');
                foreach (DictionaryEntry e in dictionary)
                {
                    RenderByValue(b, t, e.Key, true);
                    b.Append((byte)':');
                    RenderByValue(b, t, e.Value, true);
                }
                b.Append((byte)'}');
                return;
            case IEnumerable pairs when IsPairSequence(v):
            {
                b.Append((byte)'{');
                var first = true;
                foreach (var pair in pairs)
                {
                    if (!first)
                    {
                        b.Append((byte)',');
                    }
                    first = false;
                    var (key, value) = Pair(pair, t, v);
                    RenderByValue(b, t, key, true);
                    b.Append((byte)':');
                    RenderByValue(b, t, value, true);
                }
                b.Append((byte)'}');
                return;
            }
            case IEnumerable items:
            {
                b.Append((byte)'[');
                var first = true;
                foreach (var item in items)
                {
                    if (!first)
                    {
                        b.Append((byte)',');
                    }
                    first = false;
                    RenderByValue(b, t, item, true);
                }
                b.Append((byte)']');
                return;
            }
            default:
                throw Unsupported(t, $"the client gives the value without its type, and a {v.GetType().Name} has more than one text");
        }
    }
}
