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
/// The one deliberate difference is the outer <see cref="ParameterText.Escape"/> and
/// <see cref="ParameterText.QuoteSingle"/> in <see cref="Format"/>. HTTP puts the value in the query string,
/// where the server reads it directly. The native protocol carries it in the settings list as a custom entry,
/// which the server first restores as a Field, so the whole value must arrive as a quoted SQL literal — for
/// every type, not only for strings. An unquoted <c>42</c> for an <c>Int32</c> parameter is rejected.
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

        if (Array.IndexOf(IntegerTypeNames, name) >= 0 || Array.IndexOf(FloatTypeNames, name) >= 0)
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

            case "FixedString" when value is byte[] bytes:
                return QuoteIfNeeded(Encoding.UTF8.GetString(bytes).Escape(), quote);

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

            case "Variant":
                return FormatVariant(type, value, quote);

            case "JSON":
                return value is string json ? json : JsonSerializer.Serialize(value);

            default:
                throw new ArgumentException(
                    $"Cannot convert value of type '{value.GetType().FullName}' ({value}) to ClickHouse type {type}");
        }
    }

    private static string FormatDecimal(object value) => value switch
    {
        ClickHouseDecimal chd => chd.ToString(null, CultureInfo.InvariantCulture),
        string s => ParseDecimalText(s).ToString(null, CultureInfo.InvariantCulture),
        _ => Convert.ToDecimal(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
    };

    /// <summary>
    /// Reads a decimal from its text, keeping every digit. A <see cref="decimal"/> would cap the value at 29
    /// digits, which the wider ClickHouse decimals exceed.
    /// </summary>
    /// <param name="text">The decimal in invariant form.</param>
    /// <returns>The parsed value.</returns>
    /// <exception cref="ArgumentException"><paramref name="text"/> is not a decimal.</exception>
    private static ClickHouseDecimal ParseDecimalText(string text)
    {
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

        DateTime local = InTargetTimezone(type, ToOffset(value));
        return local.ToString("s", CultureInfo.InvariantCulture);
    }

    private static string FormatDateTime64(TypeNode type, object value)
    {
        if (value is DateTime { Kind: DateTimeKind.Unspecified } unspecified)
        {
            return unspecified.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        }

        DateTime local = InTargetTimezone(type, ToOffset(value));
        return local.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
    }

    private static DateTimeOffset ToOffset(object value) => value switch
    {
        DateTimeOffset dto => dto,
        DateTime dt => new DateTimeOffset(dt),
        _ => throw new ArgumentException(
            $"Cannot convert value of type '{value.GetType().FullName}' ({value}) to a date and time"),
    };

    /// <summary>
    /// Moves an instant into the type's timezone. The timezone defaults to UTC, not to the session timezone,
    /// which is what the HTTP formatter does.
    /// </summary>
    /// <param name="type">The timezone-bearing type.</param>
    /// <param name="value">The instant.</param>
    /// <returns>The wall-clock time in the type's timezone.</returns>
    private static DateTime InTargetTimezone(TypeNode type, DateTimeOffset value)
    {
        // The timezone is the last argument: DateTime('UTC') or DateTime64(3, 'UTC').
        string explicitTimezone = type.Arguments.Count > 0
            ? DateTimeZones.UnquoteTimezone(type.Arguments[^1])
            : null;

        // A DateTime64 precision digit is not a timezone.
        if (explicitTimezone is not null && int.TryParse(explicitTimezone, NumberStyles.None, CultureInfo.InvariantCulture, out _))
        {
            explicitTimezone = null;
        }

        TimeZoneInfo timeZone = DateTimeZones.Resolve(explicitTimezone, serverTimezone: null);
        return TimeZoneInfo.ConvertTime(value, timeZone).DateTime;
    }

    private static string FormatMultidimensional(TypeNode type, Array value)
    {
        TypeNode leaf = type;
        for (int rank = 0; rank < value.Rank && leaf.Name == "Array" && leaf.Arguments.Count == 1; rank++)
        {
            leaf = leaf.Arguments[0];
        }

        var builder = new StringBuilder();
        AppendAxis(builder, value, new int[value.Rank], dimension: 0, leaf);
        return builder.ToString();
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
            Format(type.Arguments[0], key, quote: true) + " : " + Format(type.Arguments[1], value[key], quote: true));

        return "{" + string.Join(",", pairs) + "}";
    }

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

        string secondsText = seconds.ToString("00." + new string('0', scale), CultureInfo.InvariantCulture);
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
