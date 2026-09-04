using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// Recognises the key/value sequence returned for a Map column, preserving pair order and duplicate keys.
/// </summary>
internal static class MapPairs
{
    /// <summary>Reports whether a value is a sequence of <see cref="KeyValuePair{TKey, TValue}"/>.</summary>
    /// <param name="value">The value to test.</param>
    /// <returns>True when the value is a pair sequence.</returns>
    public static bool IsPairSequence(object value)
    {
        if (value is not IEnumerable || value is string)
        {
            return false;
        }

        Type type = value.GetType();
        return type.GetInterfaces()
            .Append(type)
            .Any(candidate => candidate.IsGenericType
                && candidate.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                && candidate.GetGenericArguments()[0] is { IsGenericType: true } element
                && element.GetGenericTypeDefinition() == typeof(KeyValuePair<,>));
    }

    /// <summary>Reads a key/value pair whose generic types are known only at runtime.</summary>
    /// <param name="pair">The pair.</param>
    /// <returns>The key and the value.</returns>
    public static (object Key, object Value) Read(object pair)
    {
        Type type = pair.GetType();
        return (type.GetProperty("Key").GetValue(pair), type.GetProperty("Value").GetValue(pair));
    }
}
