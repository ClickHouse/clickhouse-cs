namespace ClickHouse.Driver.ADO.Parameters;

/// <summary>
/// Formats a parameter value as a string for HTTP transport.
/// Return null to fall through to the default formatter.
/// </summary>
/// <remarks>
/// Implementations must be thread-safe, as they may be called concurrently from multiple queries.
/// The formatter is not consulted for null or DBNull values; those are always serialized as the
/// ClickHouse null sentinel. The formatter is invoked for the top-level parameter value and for
/// every element inside composite values (Array, Tuple, Map, Nested). Transparent wrappers
/// (Nullable, LowCardinality, Variant) are unwrapped before the formatter is consulted, so the
/// formatter sees the underlying concrete type exactly once.
/// </remarks>
public interface IParameterFormatter
{
    /// <summary>
    /// Formats a parameter value as a string for HTTP transport.
    /// </summary>
    /// <param name="value">The parameter value (never null).</param>
    /// <param name="typeName">The resolved ClickHouse type name (e.g., "DateTime64(3)").</param>
    /// <param name="parameterName">The parameter name (without @ prefix).</param>
    /// <returns>The formatted value string, or null to use default formatting.</returns>
    string Format(object value, string typeName, string parameterName);
}
