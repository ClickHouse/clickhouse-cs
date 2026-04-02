using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.ADO.Parameters;

/// <summary>
/// Resolves parameter types using a simple CLR Type → ClickHouse type name mapping.
/// Types not in the dictionary fall through to default inference.
/// </summary>
public sealed class DictionaryParameterTypeResolver : IParameterTypeResolver
{
    private readonly IReadOnlyDictionary<Type, string> mappings;

    /// <summary>
    /// Initializes a new instance of the <see cref="DictionaryParameterTypeResolver"/> class.
    /// </summary>
    /// <param name="mappings">
    /// A dictionary mapping .NET types to ClickHouse type strings.
    /// The dictionary is copied; subsequent changes to the original have no effect.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="mappings"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown if any ClickHouse type string is null, empty, or whitespace.</exception>
    public DictionaryParameterTypeResolver(IDictionary<Type, string> mappings)
    {
        if (mappings == null)
            throw new ArgumentNullException(nameof(mappings));

        foreach (var (clrType, chTypeName) in mappings)
        {
            if (string.IsNullOrWhiteSpace(chTypeName))
            {
                throw new ArgumentException(
                    $"ClickHouse type name for {clrType} cannot be null or whitespace.", nameof(mappings));
            }
        }

        this.mappings = new Dictionary<Type, string>(mappings);
    }

    /// <inheritdoc/>
    public string ResolveType(Type clrType, object value, string parameterName)
        => mappings.GetValueOrDefault(clrType);
}
