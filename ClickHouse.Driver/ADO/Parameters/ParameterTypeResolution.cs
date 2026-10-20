using System;
using System.Globalization;
using System.Numerics;
using ClickHouse.Driver.Numerics;
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

        if (parameter.Value is ClickHouseDecimal chd)
            return ResolveClickHouseDecimalTypeName(chd, parameter.ParameterName);

        // 5. Default: value-based TypeConverter mapping (inspects the value for ambiguous types like IPAddress or instant-bearing DateTimes)
        if (parameter.Value is not null and not DBNull)
            return TypeConverter.ToClickHouseType(parameter.Value).ToString();

        return TypeConverter.ToClickHouseType(typeof(DBNull)).ToString();
    }

    /// <summary>
    /// Resolves the type for a <see cref="ClickHouseDecimal"/> value: <c>Decimal128(9)</c> if it holds the value
    /// exactly. Otherwise the scale is that of the value without its trailing fractional zeros (which the server
    /// parses exactly at a smaller scale), raised to 9 where the width has room, in <c>Decimal128</c> or, above
    /// 38 digits, <c>Decimal256</c>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The value needs more than 76 digits, which exceeds the capacity of ClickHouse's widest Decimal256.
    /// </exception>
    private static string ResolveClickHouseDecimalTypeName(ClickHouseDecimal value, string parameterName)
    {
        const int defaultScale = 9;
        const int decimal128Precision = 38;
        const int decimal256Precision = 76;

        var mantissa = BigInteger.Abs(value.Mantissa);
        var scale = mantissa.IsZero ? 0 : value.Scale;
        while (scale > 0)
        {
            var quotient = BigInteger.DivRem(mantissa, 10, out var remainder);
            if (!remainder.IsZero)
                break;
            mantissa = quotient;
            scale--;
        }

        var digits = mantissa.IsZero ? 0 : mantissa.ToString(CultureInfo.InvariantCulture).Length;
        var integerDigits = Math.Max(digits - scale, 0);
        if (integerDigits + scale > decimal256Precision)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                value,
                $"Decimal value of parameter '{parameterName}' requires a precision of {integerDigits + scale} digits, which exceeds the maximum of {decimal256Precision} supported by ClickHouse (Decimal256).");
        }

        scale = Math.Max(scale, Math.Min(defaultScale, decimal256Precision - integerDigits));
        return integerDigits + scale <= decimal128Precision ? $"Decimal128({scale})" : $"Decimal256({scale})";
    }
}
