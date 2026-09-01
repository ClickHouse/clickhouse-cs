using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// Formats a parameter value as the text the native protocol's parameter list carries.
/// </summary>
/// <remarks>
/// <para>
/// This duplicates <c>HttpParameterFormatter</c> because the assemblies cannot reference each other and use
/// different type trees. Keep shared cases aligned; known differences are listed in the TCP TODO's
/// parity check.
/// </para>
/// <para>
/// Native parameters are custom settings, so <see cref="Format"/> escapes and quotes every value for the
/// server's outer Field-parsing stage. <see cref="FormatSqlText"/> returns the inner SQL representation.
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

    /// <summary>Interval types, formatted as their underlying <c>Int64</c> count.</summary>
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

    /// <summary>Formats the inner SQL representation before native-protocol escaping and quoting.</summary>
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
            // Add parameter context without wrapping specialized ArgumentException subclasses.
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
        // The arms below are the canonical names, so an alias or a case variant is mapped to one first. Every
        // recursion into a child type comes back through here, which is what makes {p:Array(BIGINT)} format.
        string name = ColumnCodecRegistry.Default.TryCanonicalName(type.Name, out string registered) ? registered : type.Name;

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

            // Must precede the arm below, which would otherwise print the CLR type name for a byte payload.
            case "String" or "FixedString" when value is byte[] bytes:
                return QuoteIfNeeded(BytesToSqlText(bytes), quote);

            case "String" or "FixedString" when value is ReadOnlyMemory<byte> bytesMemory:
                return QuoteIfNeeded(BytesToSqlText(bytesMemory.Span), quote);

            case "String" or "FixedString" or "Enum" or "Enum8" or "Enum16" or "IPv4" or "IPv6" or "UUID":
                return QuoteIfNeeded(value.ToString().Escape(), quote);

            case "Identifier":
                // The server quotes Identifier values; escaping here would change the name.
                return value.ToString();

            case "LowCardinality" when type.Arguments.Count == 1:
                return Format(type.Arguments[0], value, quote);

            case "DateTime":
                return QuoteIfNeeded(FormatDateTime(type, value), quote);

            case "DateTime64":
                return QuoteIfNeeded(FormatDateTime64(type, value), quote);

            // A TimeOnly is a time of day and a TimeSpan an elapsed time, but both print as one clock reading.
            case "Time" when value is TimeOnly timeOfDay:
                return FormatTime(timeOfDay.ToTimeSpan());

            case "Time":
                return value is TimeSpan timeSpan
                    ? FormatTime(timeSpan)
                    : FormatTime(Convert.ToInt32(value, CultureInfo.InvariantCulture));

            case "Time64" when value is TimeOnly time64OfDay:
                return FormatTime64(time64OfDay.ToTimeSpan(), ScaleOf(type, defaultScale: 3));

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

            // Keep pair sequences out of the general Array arm while preferring dictionaries.
            case "Map" when type.Arguments.Count == 2 && MapPairs.IsPairSequence(value):
                return FormatMapPairs(type, (IEnumerable)value);

            case "Variant":
                return FormatVariant(type, value, quote);

            case "JSON":
                return (value is string json ? json : JsonSerializer.Serialize(value)).Escape();

            // A QBit is a fixed-width vector of its element type, written as an array.
            case "QBit" when value is IEnumerable components && type.Arguments.Count == 2:
                return "[" + string.Join(",", components.Cast<object>().Select(c => Format(type.Arguments[0], c, quote: true))) + "]";

            // Geo types format as their underlying tuple/array shapes.
            case "Point" or "Ring" or "LineString" or "Polygon" or "MultiLineString" or "MultiPolygon":
                return Format(GeoShapeOf(name), value, quote);

            case "AggregateFunction":
                throw new ArgumentException(
                    $"ClickHouse type '{type}' holds serialized aggregate states, so no parameter value spells it; " +
                    "the server rejects one too. Pass the arguments the state is built from instead.");

            // The server does take a text value for each of these (checked on 26.6), so the refusal is this
            // client's own: a Dynamic needs the value's type to name itself, and a Geometry value is ambiguous —
            // an array of points is both a Ring and a LineString.
            case "Dynamic" or "Geometry" or "SimpleAggregateFunction":
                throw new ArgumentException(
                    $"This client cannot format a parameter value as ClickHouse type '{type}'. Name the concrete " +
                    "type of the value instead.");

            default:
                throw NotFormattable(type, name, value);
        }
    }

    /// <summary>Explains why a value reached no formatting arm.</summary>
    /// <param name="type">The parsed type.</param>
    /// <param name="name">The type's base name.</param>
    /// <param name="value">The value, which is never null here.</param>
    /// <returns>The exception to throw.</returns>
    private static ArgumentException NotFormattable(TypeNode type, string name, object value)
    {
        // Two unrelated failures arrive here and used to read the same: a type name this client does not know,
        // and a known type an arm declined this value's shape for. Blaming the value for the first sends a
        // caller looking at the value they wrote, which is fine, for a type name that never existed.
        if (!ColumnCodecRegistry.Default.KnowsTypeName(name))
        {
            return new ArgumentException(
                $"'{name}' is not a ClickHouse type name this client knows, so no value formats as '{type}'. " +
                "Write the name the server reports for the column — SELECT toTypeName(expr).");
        }

        return new ArgumentException(
            $"Cannot convert value of type '{value.GetType().FullName}' ({value}) to ClickHouse type {type}");
    }

    /// <summary>Expands a geo type name into the Tuple/Array shape it stands for.</summary>
    /// <param name="name">The geo type name.</param>
    /// <returns>The parsed structural equivalent.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is not a geo type.</exception>
    private static TypeNode GeoShapeOf(string name)
        => TryGeoShapeOf(name, out TypeNode shape) ? shape : throw new ArgumentException($"'{name}' is not a geo type");

    /// <summary>Expands a geo type name into its shape, reporting whether the name is a geo one at all.</summary>
    /// <param name="name">The type name, which need not be a geo one.</param>
    /// <param name="shape">The parsed structural equivalent, or null.</param>
    /// <returns>True when the name is a geo type.</returns>
    internal static bool TryGeoShapeOf(string name, out TypeNode shape)
    {
        string structural = name switch
        {
            "Point" => "Tuple(Float64, Float64)",
            "Ring" or "LineString" => "Array(Point)",
            "Polygon" or "MultiLineString" => "Array(Ring)",
            "MultiPolygon" => "Array(Polygon)",
            _ => null,
        };

        shape = structural is null ? null : TypeParser.Parse(structural);
        return shape is not null;
    }

    /// <summary>Writes raw bytes as escaped SQL text without changing any of them.</summary>
    /// <param name="bytes">The bytes.</param>
    /// <returns>The escaped text the server reads back as those exact bytes.</returns>
    /// <remarks>
    /// ClickHouse strings store bytes. Valid UTF-8 remains readable; invalid UTF-8 uses <c>\xHH</c> escapes to
    /// avoid replacement-character corruption.
    /// </remarks>
    private static string BytesToSqlText(ReadOnlySpan<byte> bytes)
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
        ClickHouseTcpDecimal chd => chd.ToString(null, CultureInfo.InvariantCulture),
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
    /// Uses <see cref="decimal"/> for its supported forms and <see cref="BigInteger"/> for wider plain values.
    /// </remarks>
    private static ClickHouseTcpDecimal ParseDecimalText(string text)
    {
        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal narrow))
        {
            return ClickHouseTcpDecimal.FromDecimal(narrow);
        }

        string trimmed = text.Trim();
        int point = trimmed.IndexOf('.');
        string digits = point < 0 ? trimmed : trimmed.Remove(point, 1);
        int scale = point < 0 ? 0 : trimmed.Length - point - 1;

        if (!BigInteger.TryParse(digits, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out BigInteger mantissa))
        {
            throw new ArgumentException($"Cannot convert value '{text}' to a ClickHouse decimal");
        }

        return new ClickHouseTcpDecimal(mantissa, scale);
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
        // Unspecified is a wall clock; kinded values and offsets are instants converted to the declared timezone.
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
    /// The wire carries only wall-clock text. Without a declared timezone, the server may silently change the
    /// instant by interpreting it in <c>session_timezone</c>. Epoch text is not a safe fallback because the
    /// server parses values of four digits or fewer as years.
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
        // Formatting a wall clock is exactly the calendar use the zone is needed for, so an unrepresentable one
        // is reported here.
        TimeZoneInfo timeZone = DateTimeZones.Resolve(declaredTimezone, serverTimezone: null).Value;
        return TimeZoneInfo.ConvertTime(value, timeZone).DateTime;
    }

    private static string FormatMultidimensional(TypeNode type, Array value)
    {
        var builder = new StringBuilder();
        AppendAxis(builder, value, new int[value.Rank], dimension: 0, ResolveLeafType(type, value.Rank));
        return builder.ToString();
    }

    /// <summary>Returns the element type after validating the declared array depth against the CLR rank.</summary>
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
        // Nested formats as Array(Tuple(...)); a bare tuple represents one row.
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

    /// <summary>Formats the Map read shape while preserving pair order and duplicate keys.</summary>
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

        TypeNode jsonAlternative = null;
        foreach (TypeNode alternative in type.Arguments)
        {
            if (string.Equals(alternative.Name, "JSON", StringComparison.OrdinalIgnoreCase))
            {
                jsonAlternative ??= alternative;
                continue;
            }

            if (ParameterTypeInference.Accepts(alternative, value))
            {
                return Format(alternative, value, quote);
            }
        }

        if (jsonAlternative is not null)
        {
            return Format(jsonAlternative, value, quote);
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

        // Pre-round to even because decimal formatting rounds midpoint digits away from zero.
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
