using System;
using System.Collections;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// Infers a ClickHouse type name from a CLR value, for the last rung of the parameter type-resolution chain.
/// </summary>
/// <remarks>
/// This is a fallback, not the normal path. A native-protocol query carries its parameter types in the SQL
/// (<c>{name:Type}</c>), so the extracted hint almost always answers first and the server parses the value
/// against the declared type either way. Inference covers the two cases the hint cannot: a parameter that the
/// SQL does not reference, and SQL whose hint the scanner did not see.
/// </remarks>
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
            case ClickHouseDecimal chd: return $"Decimal128({chd.Scale})";

            case string or char or byte[]: return "String";
            case Guid: return "UUID";
            case DateOnly: return "Date";
            case TimeSpan: return "Time64(9)";

            // Sub-second precision is kept, and the instant is anchored to UTC rather than to a session zone.
            case DateTime or DateTimeOffset: return "DateTime64(7, 'UTC')";

            case IPAddress ip:
                return ip.AddressFamily == AddressFamily.InterNetworkV6 ? "IPv6" : "IPv4";

            case ITuple tuple:
                return $"Tuple({string.Join(", ", Enumerable.Range(0, tuple.Length).Select(i => Infer(tuple[i], parameterName)))})";

            case IDictionary dictionary:
                return InferMap(dictionary, parameterName);

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
        if (value is null or DBNull)
        {
            return node.Name is "Nothing";
        }

        // An alternative may itself be a wrapper; match against what it ultimately holds.
        if (node.Name is "Nullable" or "LowCardinality" && node.Arguments.Count == 1)
        {
            return Accepts(node.Arguments[0], value);
        }

        string inferred = value switch
        {
            // A byte array reads as text or as an array of bytes, so an Array alternative takes it first. The
            // string arms would otherwise win and print the CLR type name instead of the contents.
            byte[] => node.Name is "Array" or "String" or "FixedString" ? node.Name : "String",

            // These share one CLR type with several ClickHouse types, so the base name alone decides.
            string or char => node.Name is "String" or "FixedString" or "Enum8" or "Enum16" ? node.Name : "String",
            DateTime or DateTimeOffset => node.Name is "DateTime" or "DateTime64" or "Date" or "Date32" ? node.Name : "DateTime64",
            decimal or ClickHouseDecimal => node.Name.StartsWith("Decimal", StringComparison.Ordinal) ? node.Name : "Decimal128",
            IDictionary => "Map",
            ITuple => "Tuple",
            not string and IEnumerable => "Array",
            _ => InferOrNothing(value),
        };

        return string.Equals(inferred, node.Name, StringComparison.Ordinal);
    }

    /// <summary>
    /// The type a value maps to, or null when it maps to none. Used only when matching a Variant alternative,
    /// where an unmappable value must simply fail to match and let the caller report the Variant as a whole. An
    /// exception here would name a parameter the caller never wrote.
    /// </summary>
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
