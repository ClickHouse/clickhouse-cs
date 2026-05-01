using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Formats;

internal static class HttpParameterFormatter
{
    private const string NullValueString = "\\N";

    /// <summary>
    /// Formats a parameter value for HTTP transport
    /// </summary>
    /// <param name="parameter">The parameter to format.</param>
    /// <param name="typeName">The ClickHouse type name (e.g., "DateTime64(3)") for this parameter.</param>
    /// <param name="settings">Type settings for parsing.</param>
    /// <param name="customFormatter">Optional custom formatter consulted before the default switch; return null from it to fall through.</param>
    /// <returns>The formatted parameter value string.</returns>
    public static string Format(ClickHouseDbParameter parameter, string typeName, TypeSettings settings, IParameterFormatter customFormatter = null)
    {
        if (parameter.Value is null or DBNull)
        {
            return NullValueString;
        }

        var parsedType = TypeConverter.ParseClickHouseType(typeName, settings);
        return Format(parsedType, parameter.Value, false, customFormatter, parameter.ParameterName);
    }

    internal static string Format(ClickHouseType type, object value, bool quote, IParameterFormatter customFormatter = null, string parameterName = null)
    {
        // Transparent wrappers (Nullable/LowCardinality/Variant) unwrap to a concrete type
        // before consultation so the formatter sees the inner type name exactly once.
        if (customFormatter != null && value is not null and not DBNull && !IsTransparentWrapper(type))
        {
            var custom = customFormatter.Format(value, type.ToString(), parameterName);
            if (custom != null)
                return QuoteIfNeeded(custom, quote && TypeRequiresQuoting(type));
        }

        switch (type)
        {
            case NothingType nt:
                return NullValueString;
            case BooleanType bt:
                return (bool)value ? "true" : "false";
            case IntegerType it:
            case FloatType ft:
                return Convert.ToString(value, CultureInfo.InvariantCulture);
            case DecimalType dt when value is ClickHouseDecimal chd:
                return chd.ToString(CultureInfo.InvariantCulture);
            case DecimalType dt when value is string s:
                return ClickHouseDecimal.Parse(s, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);
            case DecimalType dt:
                return Convert.ToDecimal(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

            case DateType dt when value is DateTimeOffset @dto:
                return QuoteIfNeeded(@dto.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), quote);
#if NET6_0_OR_GREATER
            case DateType dt when value is DateOnly @do:
                return QuoteIfNeeded(@do.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), quote);
#endif
            case DateType dt:
                return QuoteIfNeeded(
                    Convert.ToDateTime(value, CultureInfo.InvariantCulture).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                    quote);

            case FixedStringType tt when value is byte[] fsb:
                return quote ? Encoding.UTF8.GetString(fsb).Escape().QuoteSingle() : Encoding.UTF8.GetString(fsb).Escape();

            case StringType st:
            case FixedStringType tt:
            case Enum8Type e8t:
            case Enum16Type e16t:
            case IPv4Type ip4:
            case IPv6Type ip6:
            case UuidType uuidType:
                return quote ? value.ToString().Escape().QuoteSingle() : value.ToString().Escape();

            case LowCardinalityType lt:
                return Format(lt.UnderlyingType, value, quote, customFormatter, parameterName);

            case DateTimeType dtt when value is DateTime dt:
                // ClickHouse HTTP parameters expect DateTime as ISO-formatted strings.
                // Unspecified: send as-is so ClickHouse interprets in parameter timezone
                // UTC/Local: convert to parameter timezone (or UTC if not specified) to preserve instant
                if (dt.Kind == DateTimeKind.Unspecified)
                    return QuoteIfNeeded(dt.ToString("s", CultureInfo.InvariantCulture), quote);
                return QuoteIfNeeded(FormatDateTimeInTargetTimezone(new DateTimeOffset(dt), dtt.TimeZoneOrUtc), quote);

            case DateTimeType dtt when value is DateTimeOffset dto:
                // DateTimeOffset: convert to parameter timezone (or UTC if not specified) to preserve instant
                return QuoteIfNeeded(FormatDateTimeInTargetTimezone(dto, dtt.TimeZoneOrUtc), quote);

            case DateTime64Type d64t when value is DateTime dtv:
                // ClickHouse HTTP parameters expect DateTime64 as ISO-formatted strings.
                // Unspecified: send as-is so ClickHouse interprets in parameter timezone
                // UTC/Local: convert to parameter timezone (or UTC if not specified) to preserve instant
                if (dtv.Kind == DateTimeKind.Unspecified)
                    return QuoteIfNeeded($"{dtv:yyyy-MM-dd HH:mm:ss.fffffff}", quote);
                return QuoteIfNeeded(FormatDateTime64InTargetTimezone(new DateTimeOffset(dtv), d64t.TimeZoneOrUtc), quote);

            case DateTime64Type d64t when value is DateTimeOffset dto:
                // DateTimeOffset: convert to parameter timezone (or UTC if not specified) to preserve instant
                return QuoteIfNeeded(FormatDateTime64InTargetTimezone(dto, d64t.TimeZoneOrUtc), quote);

            case TimeType tt when value is TimeSpan ts:
                return TimeType.FormatTimeString(ts);

            case TimeType tt:
                return TimeType.FormatTimeString(Convert.ToInt32(value, CultureInfo.InvariantCulture));

            case Time64Type t64 when value is TimeSpan ts:
                return t64.FormatTime64String(ts);

            case NullableType nt:
                return value is null || value is DBNull ? quote ? "null" : NullValueString : Format(nt.UnderlyingType, value, quote, customFormatter, parameterName);

            case ArrayType arrayType when value is IEnumerable enumerable:
                return $"[{string.Join(",", enumerable.Cast<object>().Select(obj => Format(arrayType.UnderlyingType, obj, true, customFormatter, parameterName)))}]";

            case QBitType qbitType when value is IEnumerable enumerable:
                return $"[{string.Join(",", enumerable.Cast<object>().Select(obj => Format(qbitType.ElementType, obj, true, customFormatter, parameterName)))}]";

            case NestedType nestedType when value is IEnumerable enumerable:
                var values = enumerable.Cast<object>().Select(x => Format(nestedType, x, false, customFormatter, parameterName));
                return $"[{string.Join(",", values)}]";

            case TupleType tupleType when value is ITuple tuple:
                return $"({string.Join(",", tupleType.UnderlyingTypes.Select((x, i) => Format(x, tuple[i], true, customFormatter, parameterName)))})";

            case TupleType tupleType when value is IList list:
                return $"({string.Join(",", tupleType.UnderlyingTypes.Select((x, i) => Format(x, list[i], true, customFormatter, parameterName)))})";

            case MapType mapType when value is IDictionary dict:
                var strings = string.Join(",", dict.Keys.Cast<object>().Select(k => $"{Format(mapType.KeyType, k, true, customFormatter, parameterName)} : {Format(mapType.ValueType, dict[k], true, customFormatter, parameterName)}"));
                return $"{{{string.Join(",", strings)}}}";

            case VariantType variantType:
                if (value is null or DBNull)
                    return quote ? "null" : NullValueString;
                var (_, chType) = variantType.GetMatchingType(value);
                return Format(chType, value, quote, customFormatter, parameterName);

            case JsonType jsonType:
                if (value is string jsonString)
                    return jsonString;
                else
                    return JsonSerializer.Serialize(value);

            default:
                throw new ArgumentException($"Cannot convert {value} to {type}");
        }
    }

    private static bool TypeRequiresQuoting(ClickHouseType type) => type switch
    {
        StringType or FixedStringType or Enum8Type or Enum16Type
            or IPv4Type or IPv6Type or UuidType
            or DateType or DateTimeType or DateTime64Type => true,
        LowCardinalityType lct => TypeRequiresQuoting(lct.UnderlyingType),
        NullableType nt => TypeRequiresQuoting(nt.UnderlyingType),
        _ => false,
    };

    private static string QuoteIfNeeded(string value, bool quote)
        => quote ? value.QuoteSingle() : value;

    private static bool IsTransparentWrapper(ClickHouseType type)
        => type is NullableType or LowCardinalityType or VariantType;

    /// <summary>
    /// Formats a DateTimeOffset as an ISO string in the target timezone.
    /// This preserves the instant while formatting in the timezone ClickHouse will interpret the string as.
    /// </summary>
    private static string FormatDateTimeInTargetTimezone(DateTimeOffset dto, NodaTime.DateTimeZone targetTimezone)
    {
        var instant = NodaTime.Instant.FromDateTimeOffset(dto);
        var zonedDateTime = instant.InZone(targetTimezone);
        return zonedDateTime.ToDateTimeUnspecified().ToString("s", CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Formats a DateTimeOffset as an ISO string with sub-second precision in the target timezone.
    /// </summary>
    private static string FormatDateTime64InTargetTimezone(DateTimeOffset dto, NodaTime.DateTimeZone targetTimezone)
    {
        var instant = NodaTime.Instant.FromDateTimeOffset(dto);
        var zonedDateTime = instant.InZone(targetTimezone);
        return $"{zonedDateTime.ToDateTimeUnspecified():yyyy-MM-dd HH:mm:ss.fffffff}";
    }
}
