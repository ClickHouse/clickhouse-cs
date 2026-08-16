using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// Recognises a Map given as a sequence of key/value pairs, which is the shape this client reads a
/// <c>Map(K, V)</c> column back as.
/// </summary>
/// <remarks>
/// <c>MapColumnCodec</c> surfaces a row as <c>KeyValuePair&lt;K, V&gt;[]</c> rather than a dictionary so that
/// duplicate keys and pair order survive the read. A value taken from a Map column therefore has to be
/// bindable as a parameter in that same shape, or reading a row and sending it back would fail.
/// </remarks>
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

    /// <summary>Reads the key and the value out of a pair of any two types.</summary>
    /// <param name="pair">The pair.</param>
    /// <returns>The key and the value.</returns>
    /// <remarks>
    /// Reflection rather than a generic overload, because the caller reaches the pair as <c>object</c> through
    /// a non-generic <see cref="IEnumerable"/> and the two type arguments are not known at this point.
    /// </remarks>
    public static (object Key, object Value) Read(object pair)
    {
        Type type = pair.GetType();
        return (type.GetProperty("Key").GetValue(pair), type.GetProperty("Value").GetValue(pair));
    }
}
