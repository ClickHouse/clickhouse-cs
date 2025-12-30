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

    public static string Format(ClickHouseDbParameter parameter, TypeSettings settings)
    {
        if (string.IsNullOrWhiteSpace(parameter.ClickHouseType))
        {
            if (parameter.Value is null or DBNull)
            {
                // Type unknown and value is null so we can't infer it
                return NullValueString;
            }

            // Infer type and format accordingly
            var type = TypeConverter.ToClickHouseType(parameter.Value.GetType());
            return Format(type, parameter.Value, false);
        }
        else
        {
            // Type has been provided
            var type = TypeConverter.ParseClickHouseType(parameter.ClickHouseType, settings);
            return Format(type, parameter.Value, false);
        }
    }

    internal static string Format(ClickHouseType type, object value, bool quote)
    {
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
            case DecimalType dt:
                return Convert.ToDecimal(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

            case DateType dt when value is DateTimeOffset @dto:
                return @dto.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
#if NET6_0_OR_GREATER
            case DateType dt when value is DateOnly @do:
                return @do.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
#endif
            case DateType dt:
                return Convert.ToDateTime(value, CultureInfo.InvariantCulture).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

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
                return Format(lt.UnderlyingType, value, quote);

            case DateTimeType dtt when value is DateTime dt:
                // UTC/Local: convert to Unix timestamp to preserve the instant
                // Unspecified: send as string so ClickHouse interprets in column timezone
                if (dt.Kind == DateTimeKind.Unspecified)
                    return dt.ToString("s", CultureInfo.InvariantCulture);
                return new DateTimeOffset(dt).ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);

            case DateTimeType dtt when value is DateTimeOffset dto:
                // DateTimeOffset always represents a specific instant - send as Unix timestamp
                return dto.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);

            case DateTime64Type d64t when value is DateTime dtv:
                // UTC/Local: convert to Unix timestamp to preserve the instant
                // Unspecified: send as string so ClickHouse interprets in column timezone
                if (dtv.Kind == DateTimeKind.Unspecified)
                    return $"{dtv:yyyy-MM-dd HH:mm:ss.fffffff}";
                return FormatDateTime64AsUnixTime(new DateTimeOffset(dtv), d64t.Scale);

            case DateTime64Type d64t when value is DateTimeOffset dto:
                // DateTimeOffset always represents a specific instant - send as Unix timestamp
                return FormatDateTime64AsUnixTime(dto, d64t.Scale);

            case TimeType tt when value is TimeSpan ts:
                return TimeType.FormatTimeString(ts);

            case TimeType tt:
                return TimeType.FormatTimeString(Convert.ToInt32(value, CultureInfo.InvariantCulture));

            case Time64Type t64 when value is TimeSpan ts:
                return t64.FormatTime64String(ts);

            case NullableType nt:
                return value is null || value is DBNull ? quote ? "null" : NullValueString : Format(nt.UnderlyingType, value, quote);

            case ArrayType arrayType when value is IEnumerable enumerable:
                return $"[{string.Join(",", enumerable.Cast<object>().Select(obj => Format(arrayType.UnderlyingType, obj, true)))}]";

            case QBitType qbitType when value is IEnumerable enumerable:
                return $"[{string.Join(",", enumerable.Cast<object>().Select(obj => Format(qbitType.ElementType, obj, true)))}]";

            case NestedType nestedType when value is IEnumerable enumerable:
                var values = enumerable.Cast<object>().Select(x => Format(nestedType, x, false));
                return $"[{string.Join(",", values)}]";

#if !NET462
            case TupleType tupleType when value is ITuple tuple:
                return $"({string.Join(",", tupleType.UnderlyingTypes.Select((x, i) => Format(x, tuple[i], true)))})";
#endif

            case TupleType tupleType when value is IList list:
                return $"({string.Join(",", tupleType.UnderlyingTypes.Select((x, i) => Format(x, list[i], true)))})";

            case MapType mapType when value is IDictionary dict:
                var strings = string.Join(",", dict.Keys.Cast<object>().Select(k => $"{Format(mapType.KeyType, k, true)} : {Format(mapType.ValueType, dict[k], true)}"));
                return $"{{{string.Join(",", strings)}}}";

            case VariantType variantType:
                var (_, chType) = variantType.GetMatchingType(value);
                return Format(chType, value, quote);

            case JsonType jsonType:
                if (value is string jsonString)
                    return jsonString;
                else
                    return JsonSerializer.Serialize(value);

            default:
                throw new ArgumentException($"Cannot convert {value} to {type}");
        }
    }

    /// <summary>
    /// Formats a DateTimeOffset as a Unix timestamp with the appropriate scale for DateTime64.
    /// </summary>
    private static string FormatDateTime64AsUnixTime(DateTimeOffset dto, int scale)
    {
        // DateTime64 stores time as a scaled integer based on precision
        // Scale 0 = seconds, 3 = milliseconds, 6 = microseconds, 9 = nanoseconds
        // Ticks are 100ns units (10^7 per second), adjust to 10^scale per second
        var unixTicks = dto.UtcDateTime.Ticks - DateTimeConversions.DateTimeEpochStart.Ticks;

        var scaledValue = scale switch
        {
            0 => unixTicks / 10_000_000L,                    // seconds
            1 => unixTicks / 1_000_000L,                     // 10ths of second
            2 => unixTicks / 100_000L,                       // 100ths of second
            3 => unixTicks / 10_000L,                        // milliseconds
            4 => unixTicks / 1_000L,                         // 10ths of millisecond
            5 => unixTicks / 100L,                           // 100ths of millisecond
            6 => unixTicks / 10L,                            // microseconds
            7 => unixTicks,                                  // 100ns (same as ticks)
            8 => unixTicks * 10L,                            // 10ns
            9 => unixTicks * 100L,                           // nanoseconds
            _ => throw new ArgumentOutOfRangeException(nameof(scale), $"Unsupported DateTime64 scale: {scale}")
        };

        return scaledValue.ToString(CultureInfo.InvariantCulture);
    }
}
