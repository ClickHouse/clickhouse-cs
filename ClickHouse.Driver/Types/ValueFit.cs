using System;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Helpers for <see cref="ClickHouseType.CanWrite"/>, which picks a Variant member. Several ClickHouse types
/// share one CLR type, so a composite type's runtime type alone does not always tell which member a value is
/// for; its elements then have to.
/// </summary>
internal static class ValueFit
{
    /// <summary>
    /// Whether a value's CLR type alone tells which ClickHouse type it is for. It does not for IPv4 and IPv6
    /// (both <see cref="System.Net.IPAddress"/>), a tuple that reads as <see cref="LargeTuple"/>, an
    /// <see cref="object"/>-typed Variant or Dynamic value, or a composite type holding any of these.
    /// </summary>
    public static bool IdentifiedByFrameworkType(ClickHouseType type)
    {
        type = TransparentWrapper.Unwrap(type);
        if (type is NullableType nullable)
            type = TransparentWrapper.Unwrap(nullable.UnderlyingType);

        return type switch
        {
            IPv4Type or IPv6Type => false,

            // Its FrameworkType throws, by design, and no value of it can be written.
            AggregateFunctionType => true,
            TupleType tuple => tuple.ElementsIdentifyTuple,
            MapType map => map.EntriesIdentifyMap,
            ArrayType array => IdentifiedByFrameworkType(array.UnderlyingType),
            _ => type.FrameworkType != typeof(object),
        };
    }

    /// <summary>
    /// Whether <paramref name="type"/> can write <paramref name="value"/>, an element of a composite value.
    /// Null (or <see cref="DBNull"/>) fits a type whose Write accepts it: Nullable, Variant and Dynamic, and Array
    /// and Nested, which write it as empty.
    /// </summary>
    public static bool Fits(ClickHouseType type, object value) =>
        value is null or DBNull
            ? TransparentWrapper.Unwrap(type) is NullableType or VariantType or DynamicType or ArrayType or NestedType
            : type.CanWrite(value);
}
