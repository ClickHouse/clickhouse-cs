using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// Formats a parameter value as the text the native protocol's parameter list carries.
/// </summary>
/// <remarks>
/// <para>
/// The inner text is the same SQL representation the HTTP transport sends as <c>param_&lt;name&gt;</c>, so this
/// mirrors <c>HttpParameterFormatter</c> arm for arm. It cannot call it: the project reference runs from
/// <c>ClickHouse.Driver</c> to <c>ClickHouse.Driver.Tcp</c>, so a reference back would be circular, and the HTTP
/// formatter is written against the RowBinary <c>ClickHouseType</c> tree, which this assembly does not have. The
/// two must be changed together — see the parity-check entry in the TCP TODO.
/// </para>
/// <para>
/// The transport difference is the outer <see cref="ParameterText.Escape"/> and
/// <see cref="ParameterText.QuoteSingle"/> in <see cref="Format"/>. HTTP puts the value in the query string,
/// where the server reads it directly. The native protocol carries it in the settings list as a custom entry,
/// which the server first restores as a Field, so the whole value must arrive as a quoted SQL literal — for
/// every type, not only for strings. An unquoted <c>42</c> for an <c>Int32</c> parameter is rejected.
/// </para>
/// <para>
/// The rest of the differences are places where this formatter is ahead. <c>Interval&lt;Unit&gt;</c>,
/// <c>QBit</c> and the six geo names format here and throw there, and <c>Json</c> in its lowercase spelling
/// works here only. Interval is a type-system difference rather than a formatter one: the RowBinary tree has
/// no Interval type, so an HTTP query fails while resolving the name. These are defects on the HTTP side:
/// a <c>byte[]</c> that is not valid UTF-8 is decoded there and loses its bytes, a Variant alternative is
/// matched on its outer name alone, a Map cannot be given as key/value pairs, and a value naming an instant
/// is accepted for a type with no timezone and silently moved. Each is fixable, and each changes a shipped
/// client, so none is fixed here — see the parity-check entry in the TCP TODO.
/// </para>
/// </remarks>
internal static class TcpParameterFormatter
{
    private const string NullValueString = "\\N";

    private static readonly string[] IntegerTypeNames =
    [
        "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256",
        "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
    ];

    private static readonly string[] FloatTypeNames = ["Float32", "Float64", "BFloat16"];

    private static readonly string[] DecimalTypeNames = ["Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256"];

    /// <summary>
    /// The <c>Interval&lt;Unit&gt;</c> family, which the server reads as its underlying <c>Int64</c> count. The
    /// list mirrors the codec registry's; a unit missing from both is reported as an unformattable type.
    /// </summary>
    private static readonly string[] IntervalTypeNames =
    [
        "IntervalNanosecond", "IntervalMicrosecond", "IntervalMillisecond", "IntervalSecond", "IntervalMinute",
        "IntervalHour", "IntervalDay", "IntervalWeek", "IntervalMonth", "IntervalQuarter", "IntervalYear",
    ];

    /// <summary>Decodes UTF-8 and throws on a byte sequence that is not valid UTF-8, rather than substituting.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>Formats a parameter value for the Query packet's parameter list.</summary>
    /// <param name="value">The parameter value.</param>
    /// <param name="typeName">The resolved ClickHouse type name (e.g. <c>DateTime64(3)</c>).</param>
    /// <param name="parameterName">The parameter name, for error messages.</param>
    /// <returns>The wire value: the SQL representation, escaped and quoted for the Field stage.</returns>
    /// <exception cref="ArgumentException">The value cannot be formatted as the type.</exception>
    /// <exception cref="FormatException"><paramref name="typeName"/> is malformed.</exception>
    public static string Format(object value, string typeName, string parameterName)
        => FormatSqlText(value, typeName, parameterName).Escape().QuoteSingle();

    /// <summary>
    /// Formats the SQL representation alone, without the native protocol's outer escape and quote. This is the
    /// exact text the HTTP transport sends, and is the level the two formatters are compared at.
    /// </summary>
    /// <param name="value">The parameter value.</param>
    /// <param name="typeName">The resolved ClickHouse type name.</param>
    /// <param name="parameterName">The parameter name, for error messages.</param>
    /// <returns>The SQL representation of the value.</returns>
    internal static string FormatSqlText(object value, string typeName, string parameterName)
    {
        if (value is null or DBNull)
        {
            return NullValueString;
        }

        TypeNode parsed = TypeParser.Parse(typeName);
        try
        {
            return Format(parsed, value, quote: false);
        }
        catch (ArgumentException ex) when (ex.GetType() == typeof(ArgumentException))
        {
            // Inner arms describe only the leaf type and value they saw. This is the one place that knows the
            // parameter name and the outer type. Filter to the exact base type so a subclass propagates as
            // itself, and forward ParamName so it is not lost.
            throw new ArgumentException(
                $"Parameter '{parameterName}' (type {parsed}): {ex.Message}",
                ex.ParamName,
                ex);
        }
    }

    /// <summary>Formats a value as a type, quoting it when it sits inside a composite literal.</summary>
    /// <param name="type">The parsed type.</param>
    /// <param name="value">The value.</param>
    /// <param name="quote">True when the caller is a composite, which needs its string-like elements quoted.</param>
    /// <returns>The formatted value.</returns>
    internal static string Format(TypeNode type, object value, bool quote)
    {
        string name = type.Name;

        if (Array.IndexOf(IntegerTypeNames, name) >= 0
            || Array.IndexOf(FloatTypeNames, name) >= 0
            || Array.IndexOf(IntervalTypeNames, name) >= 0)
        {
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        if (Array.IndexOf(DecimalTypeNames, name) >= 0)
        {
            return FormatDecimal(value);
        }

        switch (name)
        {
            case "Nothing":
                return NullValueString;

            case "Bool":
                return (bool)value ? "true" : "false";

            case "Date" or "Date32":
                return FormatDate(value, quote);

            // Must precede the arm below, which would otherwise print the CLR type name for a byte array.
            case "String" or "FixedString" when value is byte[] bytes:
                return QuoteIfNeeded(BytesToSqlText(bytes), quote);

            case "String" or "FixedString" or "Enum8" or "Enum16" or "IPv4" or "IPv6" or "UUID":
                return QuoteIfNeeded(value.ToString().Escape(), quote);

            case "Identifier":
                // The server substitutes this as a bare SQL identifier and applies its own backtick quoting, so
                // the value goes through unescaped. Escaping here would corrupt an identifier that contains a
                // quote or a backslash, and is not needed for safety because the value is never parsed as SQL.
                return value.ToString();

            case "LowCardinality" when type.Arguments.Count == 1:
                return Format(type.Arguments[0], value, quote);

            case "DateTime":
                return QuoteIfNeeded(FormatDateTime(type, value), quote);

            case "DateTime64":
                return QuoteIfNeeded(FormatDateTime64(type, value), quote);

            case "Time":
                return value is TimeSpan timeSpan
                    ? FormatTime(timeSpan)
                    : FormatTime(Convert.ToInt32(value, CultureInfo.InvariantCulture));

            case "Time64" when value is TimeSpan time64:
                return FormatTime64(time64, ScaleOf(type, defaultScale: 3));

            case "Nullable" when type.Arguments.Count == 1:
                return value is null or DBNull
                    ? quote ? "null" : NullValueString
                    : Format(type.Arguments[0], value, quote);

            // Must precede the IEnumerable arm. A rank>1 CLR array iterates flattened, so that arm would
            // serialise [[1,2],[3,4]] as [1,2,3,4].
            case "Array" when value is Array multidimensional && multidimensional.Rank > 1:
                return FormatMultidimensional(type, multidimensional);

            case "Array" when value is IEnumerable elements && type.Arguments.Count == 1:
                return "[" + string.Join(",", elements.Cast<object>().Select(e => Format(type.Arguments[0], e, quote: true))) + "]";

            case "Nested":
                return FormatNested(type, value);

            case "Tuple":
                return FormatTuple(type, value);

            case "Map" when value is IDictionary dictionary && type.Arguments.Count == 2:
                return FormatMap(type, dictionary);

            // Must follow the dictionary arm, which is the common shape, and precede the Array arm below,
            // which would otherwise take a pair sequence and write it as a list of tuples.
            case "Map" when type.Arguments.Count == 2 && MapPairs.IsPairSequence(value):
                return FormatMapPairs(type, (IEnumerable)value);

            case "Variant":
                return FormatVariant(type, value, quote);

            // The server takes either spelling and reports the type as JSON, so both must format.
            case "JSON" or "Json":
                return value is string json ? json : JsonSerializer.Serialize(value);

            // A QBit is a fixed-width vector of its element type, written as an array.
            case "QBit" when value is IEnumerable components && type.Arguments.Count == 2:
                return "[" + string.Join(",", components.Cast<object>().Select(c => Format(type.Arguments[0], c, quote: true))) + "]";

            // The geo types are named shapes over Point, which is itself a pair of Float64. Formatting them as
            // the shape they stand for is what the HTTP formatter does, where they subclass Tuple and Array.
            case "Point" or "Ring" or "LineString" or "Polygon" or "MultiLineString" or "MultiPolygon":
                return Format(GeoShapeOf(name), value, quote);

            default:
                throw new ArgumentException(
                    $"Cannot convert value of type '{value.GetType().FullName}' ({value}) to ClickHouse type {type}");
        }
    }

    /// <summary>Expands a geo type name into the Tuple/Array shape it stands for.</summary>
    /// <param name="name">The geo type name.</param>
    /// <returns>The parsed structural equivalent.</returns>
    private static TypeNode GeoShapeOf(string name) => TypeParser.Parse(name switch
    {
        "Point" => "Tuple(Float64, Float64)",
        "Ring" or "LineString" => "Array(Point)",
        "Polygon" or "MultiLineString" => "Array(Ring)",
        "MultiPolygon" => "Array(Polygon)",
        _ => throw new ArgumentException($"'{name}' is not a geo type"),
    });

    /// <summary>Writes raw bytes as escaped SQL text without changing any of them.</summary>
    /// <param name="bytes">The bytes.</param>
    /// <returns>The escaped text the server reads back as those exact bytes.</returns>
    /// <remarks>
    /// A ClickHouse <c>String</c> holds bytes, not characters, so a value can hold a byte sequence that is not
    /// UTF-8 — the read path returns one whenever <c>ReadAsByteArray</c> is used. Decoding such a sequence
    /// turns every bad byte into U+FFFD and sends <c>EF BF BD</c>, which changes the value with no error. Text
    /// that is valid UTF-8 keeps the readable form; anything else goes out as <c>\xHH</c> per byte, which the
    /// server's escaped-text reader restores exactly. Probed on 26.6.1. The HTTP formatter still decodes, so
    /// this is a divergence — see the parity-check entry in the TCP TODO.
    /// </remarks>
    private static string BytesToSqlText(byte[] bytes)
    {
        try
        {
            return StrictUtf8.GetString(bytes).Escape();
        }
        catch (DecoderFallbackException)
        {
            var builder = new StringBuilder(bytes.Length * 4);
            foreach (byte b in bytes)
            {
                builder.Append("\\x").Append(b.ToString("x2", CultureInfo.InvariantCulture));
            }

            return builder.ToString();
        }
    }

    private static string FormatDecimal(object value) => value switch
    {
        ClickHouseDecimal chd => chd.ToString(null, CultureInfo.InvariantCulture),
        string s => ParseDecimalText(s).ToString(null, CultureInfo.InvariantCulture),
        _ => Convert.ToDecimal(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
    };

    /// <summary>
    /// Reads a decimal from its text, keeping every digit.
    /// </summary>
    /// <param name="text">The decimal in invariant form.</param>
    /// <returns>The parsed value.</returns>
    /// <exception cref="ArgumentException"><paramref name="text"/> is not a decimal.</exception>
    /// <remarks>
    /// Tries <see cref="decimal"/> first, which accepts the forms the HTTP formatter does — an exponent,
    /// thousands separators, accounting parentheses. Falls back to reading the digits as a
    /// <see cref="BigInteger"/>, because a decimal caps at 29 digits and the wider ClickHouse decimals exceed
    /// that; only the plain form reaches that path, which is the only form that can be that wide.
    /// </remarks>
    private static ClickHouseDecimal ParseDecimalText(string text)
    {
        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal narrow))
        {
            return ClickHouseDecimal.FromDecimal(narrow);
        }

        string trimmed = text.Trim();
        int point = trimmed.IndexOf('.');
        string digits = point < 0 ? trimmed : trimmed.Remove(point, 1);
        int scale = point < 0 ? 0 : trimmed.Length - point - 1;

        if (!BigInteger.TryParse(digits, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out BigInteger mantissa))
        {
            throw new ArgumentException($"Cannot convert value '{text}' to a ClickHouse decimal");
        }

        return new ClickHouseDecimal(mantissa, scale);
    }

    private static string FormatDate(object value, bool quote)
    {
        string text = value switch
        {
            DateTimeOffset dto => dto.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            DateOnly dateOnly => dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        };

        return QuoteIfNeeded(text, quote);
    }

    private static string FormatDateTime(TypeNode type, object value)
    {
        // A DateTime with no kind is already in the parameter's timezone, so it is sent as it stands. A kinded
        // value or an offset names an instant, which is moved into that timezone to keep the instant.
        if (value is DateTime { Kind: DateTimeKind.Unspecified } unspecified)
        {
            return unspecified.ToString("s", CultureInfo.InvariantCulture);
        }

        DateTime local = InTargetTimezone(ToOffset(value), RequireDeclaredTimezone(type, value));
        return local.ToString("s", CultureInfo.InvariantCulture);
    }

    private static string FormatDateTime64(TypeNode type, object value)
    {
        if (value is DateTime { Kind: DateTimeKind.Unspecified } unspecified)
        {
            return unspecified.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        }

        DateTime local = InTargetTimezone(ToOffset(value), RequireDeclaredTimezone(type, value));
        return local.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Returns the timezone the type declares, and rejects the type that declares none.
    /// </summary>
    /// <param name="type">The date-and-time type.</param>
    /// <param name="value">The value, named in the error so the caller can see which kind caused it.</param>
    /// <returns>The declared timezone.</returns>
    /// <exception cref="ArgumentException">The type declares no timezone.</exception>
    /// <remarks>
    /// Only a value that names an instant reaches this. Such a value has a timezone and the type must too,
    /// because the wire carries a wall-clock time and nothing else: the server reads that text in whatever
    /// <c>session_timezone</c> is in force, so if the two disagree the instant moves and no error is raised.
    /// Sending the instant as an epoch count instead would avoid the question, but the server rejects a count
    /// below five digits — it reads it as a year — so that encoding cannot carry the first hours of 1970.
    /// </remarks>
    private static string RequireDeclaredTimezone(TypeNode type, object value)
    {
        string declared = DeclaredTimezone(type);
        if (declared is not null)
        {
            return declared;
        }

        string valueDescription = value is DateTimeOffset
            ? "A DateTimeOffset names an instant"
            : $"A DateTime with Kind={((DateTime)value).Kind} names an instant";

        throw new ArgumentException(
            $"{valueDescription}, but the type declares no timezone, so the instant cannot be sent without " +
            $"loss. The server reads the value in its session timezone, which moves the instant when that is " +
            $"not UTC, and reports no error. Declare the timezone in the type — {type.Name}" +
            $"{(type.Name == "DateTime64" ? "(3, 'UTC')" : "('UTC')")} — or pass a DateTime with " +
            $"Kind=Unspecified to send a wall-clock time for the server to read in its own timezone.");
    }

    /// <summary>Reads the timezone a date-and-time type declares.</summary>
    /// <param name="type">The date-and-time type.</param>
    /// <returns>The timezone name, or null when the type declares none.</returns>
    private static string DeclaredTimezone(TypeNode type)
    {
        // The timezone is the last argument: DateTime('UTC') or DateTime64(3, 'UTC').
        string declared = type.Arguments.Count > 0
            ? DateTimeZones.UnquoteTimezone(type.Arguments[^1])
            : null;

        // A DateTime64 precision digit is not a timezone.
        return declared is not null
            && int.TryParse(declared, NumberStyles.None, CultureInfo.InvariantCulture, out _)
                ? null
                : declared;
    }

    private static DateTimeOffset ToOffset(object value) => value switch
    {
        DateTimeOffset dto => dto,
        DateTime dt => new DateTimeOffset(dt),
        _ => throw new ArgumentException(
            $"Cannot convert value of type '{value.GetType().FullName}' ({value}) to a date and time"),
    };

    /// <summary>Moves an instant into the timezone the type declares.</summary>
    /// <param name="value">The instant.</param>
    /// <param name="declaredTimezone">The timezone the type declares.</param>
    /// <returns>The wall-clock time in that timezone.</returns>
    private static DateTime InTargetTimezone(DateTimeOffset value, string declaredTimezone)
    {
        TimeZoneInfo timeZone = DateTimeZones.Resolve(declaredTimezone, serverTimezone: null);
        return TimeZoneInfo.ConvertTime(value, timeZone).DateTime;
    }

    private static string FormatMultidimensional(TypeNode type, Array value)
    {
        var builder = new StringBuilder();
        AppendAxis(builder, value, new int[value.Rank], dimension: 0, ResolveLeafType(type, value.Rank));
        return builder.ToString();
    }

    /// <summary>
    /// Peels the declared array nesting down to the element type, and checks it is as deep as the CLR array's
    /// rank. A mismatch is reported here, where the two depths can be named, rather than as a value/type error
    /// from the arm that would otherwise be handed the wrong level.
    /// </summary>
    /// <param name="outer">The declared array type.</param>
    /// <param name="rank">The CLR array's rank.</param>
    /// <returns>The element type.</returns>
    /// <exception cref="ArgumentException">The declared depth is not the CLR rank.</exception>
    private static TypeNode ResolveLeafType(TypeNode outer, int rank)
    {
        TypeNode leaf = outer;
        int depth = 0;
        while (leaf.Name == "Array" && leaf.Arguments.Count == 1)
        {
            depth++;
            leaf = leaf.Arguments[0];
        }

        if (depth == rank)
        {
            return leaf;
        }

        string suggestion = rank > depth ? "shallower" : "deeper";
        throw new ArgumentException(
            $"CLR array rank {rank} does not match ClickHouse type '{outer}' " +
            $"(nested array depth {depth}). Provide a {suggestion} array or change the type hint.");
    }

    private static void AppendAxis(StringBuilder builder, Array value, int[] indices, int dimension, TypeNode leaf)
    {
        builder.Append('[');
        int length = value.GetLength(dimension);
        int lower = value.GetLowerBound(dimension);
        for (int i = 0; i < length; i++)
        {
            if (i > 0)
            {
                builder.Append(',');
            }

            indices[dimension] = lower + i;
            if (dimension == value.Rank - 1)
            {
                builder.Append(Format(leaf, value.GetValue(indices), quote: true));
            }
            else
            {
                AppendAxis(builder, value, indices, dimension + 1, leaf);
            }
        }

        builder.Append(']');
    }

    private static string FormatNested(TypeNode type, object value)
    {
        // A Nested value is the whole set of rows, so an enumerable is the rows and each row is a tuple of the
        // declared fields. The result has the same shape as Array(Tuple(...)). A bare tuple is one row.
        if (value is IEnumerable rows and not ITuple)
        {
            return "[" + string.Join(",", rows.Cast<object>().Select(row => FormatTuple(type, row))) + "]";
        }

        return FormatTuple(type, value);
    }

    private static string FormatTuple(TypeNode type, object value)
    {
        TypeNode[] elementTypes = NamedElementParser.Split(type).Select(element => element.Type).ToArray();

        return value switch
        {
            ITuple tuple => "(" + string.Join(",", elementTypes.Select((t, i) => Format(t, tuple[i], quote: true))) + ")",
            IList list => "(" + string.Join(",", elementTypes.Select((t, i) => Format(t, list[i], quote: true))) + ")",
            _ => throw new ArgumentException(
                $"Cannot convert value of type '{value.GetType().FullName}' ({value}) to ClickHouse type {type}"),
        };
    }

    private static string FormatMap(TypeNode type, IDictionary value)
    {
        var pairs = value.Keys.Cast<object>().Select(key =>
            FormatPair(type, key, value[key]));

        return "{" + string.Join(",", pairs) + "}";
    }

    /// <summary>
    /// Formats a Map given as a sequence of key/value pairs, which is how this client reads one back. The
    /// codec surfaces a row as <c>KeyValuePair&lt;K, V&gt;[]</c> rather than a dictionary so that duplicate
    /// keys and pair order survive, so a value read from a Map column must be bindable in that shape.
    /// </summary>
    /// <param name="type">The Map type.</param>
    /// <param name="pairs">The pairs, in order.</param>
    /// <returns>The map literal.</returns>
    private static string FormatMapPairs(TypeNode type, IEnumerable pairs)
    {
        var formatted = pairs.Cast<object>().Select(pair =>
        {
            (object key, object value) = MapPairs.Read(pair);
            return FormatPair(type, key, value);
        });

        return "{" + string.Join(",", formatted) + "}";
    }

    private static string FormatPair(TypeNode type, object key, object value)
        => Format(type.Arguments[0], key, quote: true) + " : " + Format(type.Arguments[1], value, quote: true);


    private static string FormatVariant(TypeNode type, object value, bool quote)
    {
        if (value is null or DBNull)
        {
            return quote ? "null" : NullValueString;
        }

        foreach (TypeNode alternative in type.Arguments)
        {
            if (ParameterTypeInference.Accepts(alternative, value))
            {
                return Format(alternative, value, quote);
            }
        }

        throw new ArgumentException(
            $"Cannot convert value of type '{value.GetType().FullName}' ({value}) to ClickHouse type {type}: " +
            "no alternative of the Variant accepts it");
    }

    private static string FormatTime(TimeSpan value) => FormatTime((int)Math.Round(value.TotalSeconds));

    private static string FormatTime(int totalSeconds)
    {
        int absolute = Math.Abs(totalSeconds);
        string text = $"{absolute / 3600}:{(absolute % 3600) / 60:D2}:{absolute % 60:D2}";
        return totalSeconds < 0 ? "-" + text : text;
    }

    private static string FormatTime64(TimeSpan value, int scale)
    {
        decimal totalSeconds = (decimal)value.Ticks / TimeSpan.TicksPerSecond;
        bool negative = totalSeconds < 0;
        decimal absolute = Math.Abs(totalSeconds);

        int hours = (int)(absolute / 3600m);
        decimal remainder = absolute % 3600m;
        int minutes = (int)(remainder / 60m);
        decimal seconds = remainder % 60m;

        // Round before formatting, and to even, because decimal.ToString rounds away from zero. Without this a
        // midpoint lands one tick above where the HTTP formatter puts it.
        string secondsText = Math.Round(seconds, scale, MidpointRounding.ToEven)
            .ToString("00." + new string('0', scale), CultureInfo.InvariantCulture);
        string text = $"{hours}:{minutes:D2}:{secondsText}";
        return negative ? "-" + text : text;
    }

    private static int ScaleOf(TypeNode type, int defaultScale)
        => type.Arguments.Count > 0
            && int.TryParse(type.Arguments[0].Name, NumberStyles.None, CultureInfo.InvariantCulture, out int scale)
            ? scale
            : defaultScale;

    private static string QuoteIfNeeded(string value, bool quote) => quote ? value.QuoteSingle() : value;
}
