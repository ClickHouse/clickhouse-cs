using System;

namespace ClickHouse.Driver.ADO.Parameters;

/// <summary>
/// Resolves the ClickHouse type for a parameter value during @-style parameter substitution.
/// Return null to fall through to the default type inference.
/// </summary>
/// <remarks>
/// Implementations must be thread-safe, as they may be called concurrently from multiple queries.
/// </remarks>
public interface IParameterTypeResolver
{
    /// <summary>
    /// Resolves the ClickHouse type name for a parameter.
    /// </summary>
    /// <param name="clrType">The .NET type of the parameter value.</param>
    /// <param name="value">The parameter value (never null).</param>
    /// <param name="parameterName">The parameter name (without @ prefix).</param>
    /// <returns>A ClickHouse type string (e.g., "DateTime64(3)"), or null to use default inference.</returns>
    string ResolveType(Type clrType, object value, string parameterName);
}
