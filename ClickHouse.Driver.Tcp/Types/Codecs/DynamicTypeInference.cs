using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Numerics;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Infers the ClickHouse type a CLR value should be written as inside a <c>Dynamic</c> column. The set of types a
/// <c>Dynamic</c> holds is not declared, so the ergonomic write path derives each value's type from its runtime
/// shape. This is self-contained (the TCP client cannot reference the main driver's type system), covering the
/// scalar types the codecs support plus recursion into <c>Array</c>/<c>Map</c>/<c>Tuple</c> of them.
///
/// <para>
/// The result is a canonical ClickHouse type string, resolved to a codec through the registry, whose element type
/// matches the CLR value — so an inferred type both round-trips the value and buckets values of the same type
/// together. Where a CLR type maps to more than one ClickHouse type, a documented default is chosen: an
/// <see cref="IPAddress"/> is disambiguated by its address family, and a bare <see cref="DateOnly"/> maps to the
/// wider <c>Date32</c>.
/// </para>
/// </summary>
internal static class DynamicTypeInference
{
    // CLR types with a single unambiguous ClickHouse mapping whose codec surfaces that same CLR type on read.
    private static readonly Dictionary<Type, string> Scalars = new()
    {
        [typeof(byte)] = "UInt8",
        [typeof(sbyte)] = "Int8",
        [typeof(ushort)] = "UInt16",
        [typeof(short)] = "Int16",
        [typeof(uint)] = "UInt32",
        [typeof(int)] = "Int32",
        [typeof(ulong)] = "UInt64",
        [typeof(long)] = "Int64",
        [typeof(UInt128)] = "UInt128",
        [typeof(Int128)] = "Int128",
        [typeof(UInt256)] = "UInt256",
        [typeof(Int256)] = "Int256",
        [typeof(float)] = "Float32",
        [typeof(double)] = "Float64",
        [typeof(bool)] = "Bool",
        [typeof(string)] = "String",
        [typeof(Guid)] = "UUID",
        [typeof(DateOnly)] = "Date32",
    };

    /// <summary>Infers the ClickHouse type string a non-null <paramref name="value"/> should be written as.</summary>
    /// <param name="value">The value to infer a type for; must not be null (a NULL row rides the discriminator, not a type).</param>
    /// <returns>
    /// The canonical ClickHouse type string and the value coerced to that codec's CLR element type — so the write
    /// path can bucket it as the element type the codec reads back. For most types the value is returned as-is; a
    /// <see cref="DateTimeOffset"/>/<see cref="DateTime"/> becomes a raw <see cref="long"/> nanosecond count
    /// (<c>DateTime64(9)</c>'s element type) and a <see cref="decimal"/> a <see cref="ClickHouseDecimal"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    /// <exception cref="NotSupportedException">No ClickHouse type is inferred for the value's CLR type.</exception>
    public static (string TypeName, object Value) Infer(object value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value is IPAddress ip)
        {
            return (ip.AddressFamily == AddressFamily.InterNetwork ? "IPv4" : "IPv6", value);
        }

        // Types whose ClickHouse mapping depends on the value (scale) or that map to a canonical read-back type
        // wider than the input CLR type. The value is coerced to that codec's element type so it round-trips:
        //  - a decimal maps to Decimal128 (element type ClickHouseDecimal), a ClickHouseDecimal to Decimal256;
        //  - a DateTimeOffset/DateTime maps to DateTime64 at nanosecond scale, coerced to the codec's Int64 count
        //    element type (exact for either — both hold at most 100 ns ticks). A raw Int64 is Int64, not
        //    DateTime64: there is no distinct CLR carrier to key on, so date-time semantics require a
        //    DateTimeOffset/DateTime input.
        switch (value)
        {
            case DateTimeOffset dateTimeOffset:
                return ("DateTime64(9)", ToNanosecondCount(dateTimeOffset));
            case DateTime dateTime:
                return ("DateTime64(9)", ToNanosecondCount(new DateTimeOffset(dateTime.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dateTime, DateTimeKind.Utc) : dateTime)));
            case ClickHouseDecimal wideDecimal:
                return (FormattableString.Invariant($"Decimal(76, {wideDecimal.Scale})"), value);
            case decimal value128:
                return (FormattableString.Invariant($"Decimal(38, {ScaleOf(value128)})"), ClickHouseDecimal.FromDecimal(value128));
        }

        Type type = value.GetType();
        if (Scalars.TryGetValue(type, out string scalar))
        {
            return (scalar, value);
        }

        // Composites carry their value through unchanged (their element codecs accept the element CLR types
        // directly); a composite of a coercion-needing element type (e.g. DateTimeOffset[]) is not supported.
        if (value is ITuple tuple)
        {
            return (InferTuple(tuple), value);
        }

        if (value is Array array)
        {
            return (InferArrayOrMap(array), value);
        }

        throw new NotSupportedException(
            $"No ClickHouse type is inferred for a Dynamic value of CLR type '{type}'. Supported: the fixed-width scalars, String, UUID, Date, IP addresses, decimals, date-times, and arrays/maps/tuples of them.");
    }

    // An array is either a Map (an array of KeyValuePair<K, V>) or a plain Array(T). The element type is inferred
    // from the first present element when the array is non-empty, else from the CLR element type.
    private static string InferArrayOrMap(Array array)
    {
        Type elementType = array.GetType().GetElementType();
        if (elementType is not null && elementType.IsGenericType && elementType.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))
        {
            Type[] pair = elementType.GetGenericArguments();
            return $"Map({InferFromClrType(pair[0])}, {InferFromClrType(pair[1])})";
        }

        // Prefer a present element's runtime type (so IPAddress and other value-disambiguated types resolve), and
        // fall back to the declared element type for an empty (or all-null) array. Every present element must
        // infer the same type: the write path buckets the whole array as one element type, so a heterogeneous
        // array (e.g. IPv4 mixed with IPv6 in an IPAddress[]) would fail the element cast later — reject it here.
        string inferredElement = null;
        foreach (object element in array)
        {
            if (element is null)
            {
                continue;
            }

            string current = InferComposable(element);
            if (inferredElement is null)
            {
                inferredElement = current;
            }
            else if (!string.Equals(inferredElement, current, StringComparison.Ordinal))
            {
                throw new NotSupportedException(
                    $"A Dynamic array must have a single element type, but it mixes '{inferredElement}' and '{current}'. Provide an array whose values all map to one ClickHouse type.");
            }
        }

        return $"Array({inferredElement ?? InferFromClrType(elementType)})";
    }

    private static string InferTuple(ITuple tuple)
    {
        if (tuple.Length == 0)
        {
            throw new NotSupportedException("A Dynamic value cannot be an empty tuple.");
        }

        var parts = new string[tuple.Length];
        for (int i = 0; i < tuple.Length; i++)
        {
            object element = tuple[i];
            parts[i] = element is null
                ? throw new NotSupportedException("A tuple element inside a Dynamic value must not be null; wrap it in Nullable instead.")
                : InferComposable(element);
        }

        return $"Tuple({string.Join(", ", parts)})";
    }

    // Infers a composite element's type, rejecting an element whose value must be coerced to a different CLR type
    // (a DateTimeOffset/DateTime/decimal): a composite carries its elements through unchanged, so the element must
    // already be the CLR type its codec reads back. Caught here (before any bytes are written) as a clear error
    // rather than a cast failure deep in the element projection.
    private static string InferComposable(object element)
    {
        (string typeName, object canonical) = Infer(element);
        if (canonical.GetType() != element.GetType())
        {
            throw new NotSupportedException(
                $"A Dynamic composite element of CLR type '{element.GetType()}' would be coerced to '{canonical.GetType()}', which is not supported inside a composite. Use the canonical element type directly (e.g. a raw long count for a DateTime64, ClickHouseDecimal for a decimal).");
        }

        return typeName;
    }

    // The scale of a System.Decimal — the count of fractional digits, held in bits 16–23 of its flags word.
    private static int ScaleOf(decimal value) => (decimal.GetBits(value)[3] >> 16) & 0xFF;

    // DateTime64's element type is the raw Int64 count; at scale 9 (nanoseconds) that is the .NET tick count
    // since the epoch times 100 (1 tick = 100 ns), exact for any DateTimeOffset/DateTime (both hold 100 ns ticks).
    private static long ToNanosecondCount(DateTimeOffset value) => checked((value.UtcDateTime.Ticks - DateTime.UnixEpoch.Ticks) * 100);

    // Infers a type from a CLR type alone (no value) — used for the element type of an empty array, where no
    // element is available to disambiguate. Ambiguous types (e.g. IPAddress) cannot be resolved this way.
    private static string InferFromClrType(Type type)
    {
        if (type is not null && Scalars.TryGetValue(type, out string scalar))
        {
            return scalar;
        }

        // An empty nested composite still resolves structurally from its CLR element type, with no value to
        // disambiguate — mirroring how a present value of the same shape infers (a KeyValuePair<,>[] as a Map, any
        // other T[] as Array(T), including byte[] as Array(UInt8)). This is what makes an empty nested array (an
        // empty Array(Array(UInt64)), say) usable inside Dynamic.
        if (type is not null && type.IsArray)
        {
            Type elementType = type.GetElementType();
            if (elementType is not null && elementType.IsGenericType && elementType.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))
            {
                Type[] pair = elementType.GetGenericArguments();
                return $"Map({InferFromClrType(pair[0])}, {InferFromClrType(pair[1])})";
            }

            return $"Array({InferFromClrType(elementType)})";
        }

        throw new NotSupportedException(
            $"No ClickHouse type is inferred for a Dynamic array whose element CLR type is '{type?.ToString() ?? "unknown"}' with no element to disambiguate it.");
    }
}
