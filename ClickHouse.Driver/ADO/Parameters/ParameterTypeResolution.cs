using System;
using ClickHouse.Driver.Types;
namespace ClickHouse.Driver.ADO.Parameters;

/// <summary>
/// Central type resolution for parameters. Encapsulates the full precedence chain so that
/// SQL placeholder generation and HTTP value formatting use the same resolved type.
/// Called once per parameter per request by <see cref="ClickHouseClient"/>, which reuses the
/// result for both purposes.
/// </summary>
internal static class ParameterTypeResolution
{
    /// <summary>
    /// Resolves the effective ClickHouse type name for a parameter.
    /// </summary>
    /// <param name="parameter">The parameter to resolve.</param>
    /// <param name="sqlTypeHint">
    /// Type hint extracted from the SQL query (e.g., "UInt64" from <c>{name:UInt64}</c>), or null.
    /// </param>
    /// <param name="resolver">
    /// Custom resolver from <see cref="ClickHouseClientSettings.ParameterTypeResolver"/>, or null.
    /// </param>
    /// <returns>ClickHouse type name string (e.g., "DateTime64(3)", "Int32").</returns>
    internal static string ResolveTypeName(
        ClickHouseDbParameter parameter,
        string sqlTypeHint,
        IParameterTypeResolver resolver)
    {
        // 1. Explicit ClickHouseType on the parameter
        if (parameter.ClickHouseType != null)
            return parameter.ClickHouseType;

        // 2. SQL type hint from {name:Type} in the query
        if (!string.IsNullOrWhiteSpace(sqlTypeHint))
            return sqlTypeHint;

        // 3. Custom resolver from settings
        if (resolver != null && parameter.Value is not null and not DBNull)
        {
            var resolved = resolver.ResolveType(
                parameter.Value.GetType(), parameter.Value, parameter.ParameterName);
            if (resolved != null)
                return resolved;
        }

        // 4. Special decimal handling (preserve scale from the actual value)
        if (parameter.Value is decimal d)
        {
            var parts = decimal.GetBits(d);
            int scale = (parts[3] >> 16) & 0x7F;
            return $"Decimal128({scale})";
        }

        // 5. Default: value-based TypeConverter mapping (inspects the value for ambiguous types like IPAddress)
        if (parameter.Value is not null and not DBNull)
            return TypeConverter.ToClickHouseType(parameter.Value).ToString();

        return TypeConverter.ToClickHouseType(typeof(DBNull)).ToString();
    }
}
