using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.ADO.Parameters;

/// <summary>
/// Formats parameter values using a simple CLR Type → format function mapping.
/// Types not in the dictionary fall through to default formatting.
/// </summary>
public sealed class DictionaryParameterFormatter : IParameterFormatter
{
    private readonly IReadOnlyDictionary<Type, Func<object, string>> mappings;

    /// <summary>
    /// Initializes a new instance of the <see cref="DictionaryParameterFormatter"/> class.
    /// </summary>
    /// <param name="mappings">
    /// A dictionary mapping .NET types to functions that format values of that type.
    /// The dictionary is copied; subsequent changes to the original have no effect.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="mappings"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown if any format function is null.</exception>
    public DictionaryParameterFormatter(IDictionary<Type, Func<object, string>> mappings)
    {
        if (mappings == null)
            throw new ArgumentNullException(nameof(mappings));

        foreach (var (clrType, fn) in mappings)
        {
            if (fn == null)
            {
                throw new ArgumentException(
                    $"Format function for {clrType} cannot be null.", nameof(mappings));
            }
        }

        this.mappings = new Dictionary<Type, Func<object, string>>(mappings);
    }

    /// <inheritdoc/>
    public string Format(object value, string typeName, string parameterName)
        => mappings.TryGetValue(value.GetType(), out var fn) ? fn(value) : null;
}
