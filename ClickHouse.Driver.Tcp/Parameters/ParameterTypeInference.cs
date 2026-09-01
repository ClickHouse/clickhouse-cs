using System;
using System.Collections;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// Retained for future client-side placeholder support and used to match Variant alternatives.
/// </summary>
internal static class ParameterTypeInference
{
    /// <summary>Infers the ClickHouse type name to format a value as.</summary>
    /// <param name="value">The parameter value.</param>
    /// <param name="parameterName">The parameter name, for the error message.</param>
    /// <returns>The inferred ClickHouse type name.</returns>
    /// <exception cref="ArgumentException">The value's CLR type maps to no ClickHouse type.</exception>
    public static string Infer(object value, string parameterName)
    {
        switch (value)
        {
            case null or DBNull: return "Nullable(Nothing)";
            case bool: return "Bool";
            case byte: return "UInt8";
            case sbyte: return "Int8";
            case ushort: return "UInt16";
            case short: return "Int16";
            case uint: return "UInt32";
            case int: return "Int32";
            case ulong: return "UInt64";
            case long: return "Int64";
            case UInt128: return "UInt128";
            case Int128: return "Int128";
            case UInt256: return "UInt256";
            case Int256: return "Int256";
            case float: return "Float32";
            case double: return "Float64";

            // The scale is the value's own, so a round trip keeps every digit the caller supplied.
            case decimal d: return $"Decimal128({(decimal.GetBits(d)[3] >> 16) & 0x7F})";
            case ClickHouseTcpDecimal chd: return $"Decimal128({chd.Scale})";

            case string or char or byte[] or ReadOnlyMemory<byte>: return "String";
            case Guid: return "UUID";
            case DateOnly: return "Date";

            // Scale 9 holds every tick either one can carry, so neither loses a digit.
            case TimeSpan or TimeOnly: return "Time64(9)";

            // Sub-second precision is kept, and the instant is anchored to UTC rather than to a session zone.
            case DateTime or DateTimeOffset: return "DateTime64(7, 'UTC')";

            case IPAddress ip:
                return ip.AddressFamily == AddressFamily.InterNetworkV6 ? "IPv6" : "IPv4";

            case ITuple tuple:
                return $"Tuple({string.Join(", ", Enumerable.Range(0, tuple.Length).Select(i => Infer(tuple[i], parameterName)))})";

            case IDictionary dictionary:
                return InferMap(dictionary, parameterName);

            // The shape this client reads a Map column back as, which Accepts already knows. It is also an
            // IEnumerable, so it has to be decided before the Array arm below, whose element inference has no
            // reading for a KeyValuePair. Without this arm a row read from a Map column could not be sent back
            // as a Dynamic or an untyped parameter.
            case not string and IEnumerable pairs when MapPairs.IsPairSequence(value):
                return InferPairSequence(pairs, parameterName);

            case IEnumerable enumerable:
                return $"Array({InferElement(enumerable, parameterName)})";

            default:
                throw new ArgumentException(
                    $"Parameter '{parameterName}' has value type '{value.GetType().FullName}', which maps to no ClickHouse type. " +
                    "Add a {" + parameterName + ":Type} hint to the SQL, or set ClickHouseType on the parameter.",
                    parameterName);
        }
    }

    /// <summary>Reports whether a value would be formatted as the given type, used to pick a Variant alternative.</summary>
    /// <param name="node">The candidate alternative type.</param>
    /// <param name="value">The value to place.</param>
    /// <returns>True when the alternative accepts the value.</returns>
    public static bool Accepts(TypeNode node, object value)
    {
        // The names below are canonical, so an alias or a case variant is mapped to one first, as the formatter
        // does before its own dispatch. Without this Variant(BIGINT, String) accepted no Int64 at all, while the
        // server resolves that declaration to Variant(Int64, String).
        string name = ColumnCodecRegistry.Default.TryCanonicalName(node.Name, out string registered)
            ? registered
            : node.Name;

        if (value is null or DBNull)
        {
            return name is "Nothing";
        }

        // An alternative may itself be a wrapper; match against what it ultimately holds.
        if (name is "Nullable" or "LowCardinality" && node.Arguments.Count == 1)
        {
            return Accepts(node.Arguments[0], value);
        }

        // A geo name stands for a Tuple/Array shape, which is what the formatter writes it as. Matching the
        // name alone would reject every value, because no CLR type infers to "Point".
        if (TcpParameterFormatter.TryGeoShapeOf(name, out TypeNode geoShape))
        {
            return Accepts(geoShape, value);
        }

        // A QBit is a fixed-width vector of its element type, so it takes what an Array of that type takes.
        if (name is "QBit" && node.Arguments.Count == 2)
        {
            return value is not string and IEnumerable components && AcceptsElements(node.Arguments[0], components);
        }

        // Match composite alternatives recursively; their outer names are insufficient.
        switch (value)
        {
            case IDictionary dictionary when name is "Map":
                return node.Arguments.Count == 2 && AcceptsPairs(node, dictionary);

            // Handle the Map read shape before the general Array case.
            case not string and IEnumerable pairs when MapPairs.IsPairSequence(value):
                return name is "Map"
                    && node.Arguments.Count == 2
                    && AcceptsPairSequence(node, pairs);

            case ITuple tuple when name is "Tuple":
                return AcceptsTupleElements(node, tuple);

            // A byte[] is checked element by element here too, so it takes Array(UInt8) but not Array(String).
            case not string and IEnumerable elements when name is "Array":
                return node.Arguments.Count == 1 && AcceptsElements(node.Arguments[0], elements);

            case IDictionary or ITuple:
                return false;
        }

        string inferred = value switch
        {
            // The Array case above already took a byte[] an element type accepts, so only the text arms remain.
            // A ReadOnlyMemory is not IEnumerable, so it never reaches that case and only the text arms format it.
            byte[] or ReadOnlyMemory<byte> => name is "String" or "FixedString" ? name : "String",

            // These share one CLR type with several ClickHouse types, so the base name alone decides.
            string or char => name is "String" or "FixedString" or "Enum" or "Enum8" or "Enum16" ? name : "String",
            DateTime or DateTimeOffset => name is "DateTime" or "DateTime64" or "Date" or "Date32" ? name : "DateTime64",
            TimeSpan or TimeOnly => name is "Time" or "Time64" ? name : "Time64",
            decimal or ClickHouseTcpDecimal => name.StartsWith("Decimal", StringComparison.Ordinal) ? name : "Decimal128",
            not string and IEnumerable => "Array",
            _ => InferOrNothing(value),
        };

        return string.Equals(inferred, name, StringComparison.Ordinal);
    }

    /// <summary>Reports whether every element fits; an empty sequence fits any element type.</summary>
    /// <param name="elementType">The candidate element type.</param>
    /// <param name="elements">The sequence.</param>
    /// <returns>True when every element fits.</returns>
    private static bool AcceptsElements(TypeNode elementType, IEnumerable elements)
    {
        foreach (object element in elements)
        {
            if (!Accepts(elementType, element))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Reports whether every pair of a key/value sequence fits a Map's two argument types.</summary>
    /// <param name="node">The Map type.</param>
    /// <param name="pairs">The pair sequence.</param>
    /// <returns>True when every pair fits.</returns>
    private static bool AcceptsPairSequence(TypeNode node, IEnumerable pairs)
    {
        foreach (object pair in pairs)
        {
            (object key, object value) = MapPairs.Read(pair);
            if (!Accepts(node.Arguments[0], key) || !Accepts(node.Arguments[1], value))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Reports whether every key and value of a dictionary fits a Map's two argument types.</summary>
    /// <param name="node">The Map type.</param>
    /// <param name="value">The dictionary.</param>
    /// <returns>True when every pair fits.</returns>
    private static bool AcceptsPairs(TypeNode node, IDictionary value)
    {
        foreach (DictionaryEntry entry in value)
        {
            if (!Accepts(node.Arguments[0], entry.Key) || !Accepts(node.Arguments[1], entry.Value))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Reports whether a tuple's arity and every element fit a Tuple type.</summary>
    /// <param name="node">The Tuple type.</param>
    /// <param name="value">The tuple.</param>
    /// <returns>True when the arity and every element fit.</returns>
    private static bool AcceptsTupleElements(TypeNode node, ITuple value)
    {
        // Split does not validate arity, so compare it explicitly.
        TypeNode[] elementTypes = NamedElementParser.Split(node).Select(element => element.Type).ToArray();

        if (elementTypes.Length != value.Length)
        {
            return false;
        }

        for (int i = 0; i < elementTypes.Length; i++)
        {
            if (!Accepts(elementTypes[i], value[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Infers a Variant candidate, returning null instead of a parameter-level error.</summary>
    /// <param name="value">The value to place.</param>
    /// <returns>The base type name, or null.</returns>
    private static string InferOrNothing(object value)
    {
        try
        {
            return TypeParser.Parse(Infer(value, parameterName: null)).Name;
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    private static string InferMap(IDictionary dictionary, string parameterName)
    {
        // An empty map carries no sample value, so fall back to the widest pair the server will accept.
        foreach (DictionaryEntry entry in dictionary)
        {
            return $"Map({Infer(entry.Key, parameterName)}, {Infer(entry.Value, parameterName)})";
        }

        return "Map(String, String)";
    }

    private static string InferPairSequence(IEnumerable pairs, string parameterName)
    {
        foreach (object pair in pairs)
        {
            (object key, object value) = MapPairs.Read(pair);
            return $"Map({Infer(key, parameterName)}, {Infer(value, parameterName)})";
        }

        // Empty, so no sample pair: the same fallback an empty dictionary takes.
        return "Map(String, String)";
    }

    private static string InferElement(IEnumerable enumerable, string parameterName)
    {
        foreach (object element in enumerable)
        {
            if (element is not null and not DBNull)
            {
                return Infer(element, parameterName);
            }
        }

        // Every element is null, or there are none; String is the type most values print into.
        return "Nullable(String)";
    }
}
