using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;

namespace ClickHouse.Driver.Types;

internal class DynamicType : ClickHouseType
{
    /// <summary>
    /// Cache for inferred ClickHouse types from .NET types, with whether the type contains a decimal.
    /// </summary>
    private static readonly ConcurrentDictionary<Type, (ClickHouseType Type, bool HasDecimal)> InferredTypeCache = new();

    public override Type FrameworkType => typeof(object);

    public TypeSettings TypeSettings { get; init; }

    public override string ToString() => "Dynamic";

    public override object Read(ExtendedBinaryReader reader) =>
        BinaryTypeDecoder.
            FromByteCode(reader, TypeSettings).
            Read(reader);

    /// <summary>
    /// Writes a value with its type header for dynamic type encoding.
    /// The type is inferred from the value's .NET type and cached, except for decimals, whose
    /// ClickHouse width and scale depend on the values themselves and so are inferred per value.
    /// </summary>
    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is null || value is DBNull)
        {
            writer.Write(BinaryTypeIndex.Nothing);
            return;
        }

        // Decimals must be inferred from the value, not just its .NET type: a single cached type has
        // one fixed scale, which truncates every value with more fractional digits than that scale.
        ClickHouseType inferredType;
        if (value is ClickHouseDecimal chd)
            inferredType = TypeConverter.InferDecimalType(chd);
        else if (value is decimal dec)
            inferredType = TypeConverter.InferDecimalType(dec); // implicit decimal -> ClickHouseDecimal
        else
            inferredType = InferType(value);

        // System.Object infers as Dynamic (so that object[] infers as Array(Dynamic)), but a value of exactly that
        // type has no concrete type to write: writing it as Dynamic again would recurse without end.
        if (inferredType is DynamicType)
            throw new ArgumentOutOfRangeException(nameof(value), "Unknown type: " + value.GetType());

        BinaryTypeDescriptionWriter.WriteTypeHeader(writer, inferredType);
        inferredType.Write(writer, value);
    }

    /// <summary>
    /// Infers the type of a non-decimal value from its .NET type, through the cache. A cached type has one fixed
    /// scale for each decimal it contains, so those are inferred again from the value, and the result is not cached.
    /// </summary>
    private static ClickHouseType InferType(object value)
    {
        var (type, hasDecimal) = GetCachedInferredType(value.GetType());
        return hasDecimal ? InferDecimalTypes(type, [value]) : type;
    }

    /// <summary>
    /// Use the type converter to infer the ClickHouse type from the .NET type, and cache the results.
    /// </summary>
    private static (ClickHouseType Type, bool HasDecimal) GetCachedInferredType(Type type)
        => InferredTypeCache.GetOrAdd(type, static t =>
        {
            var inferred = TypeConverter.ToClickHouseType(t);
            return (inferred, ContainsDecimal(inferred));
        });

    /// <summary>
    /// Returns <paramref name="type"/> with each decimal in it replaced by one decimal type that holds all the
    /// values at that position of <paramref name="values"/> (see <see cref="TypeConverter.InferCommonDecimalType"/>).
    /// The parts of <paramref name="type"/> that contain no decimal are returned unchanged.
    /// </summary>
    /// <param name="type">A type inferred from the .NET type of <paramref name="values"/>.</param>
    /// <param name="values">The values to be written as <paramref name="type"/>.</param>
    private static ClickHouseType InferDecimalTypes(ClickHouseType type, IEnumerable<object> values)
    {
        if (!ContainsDecimal(type))
            return type;

        // A null has no payload to preserve, so it does not constrain the type.
        values = values.Where(v => v is not null and not DBNull);
        return type switch
        {
            // With no value to preserve, the decimal keeps the type inferred from its .NET type.
            DecimalType => TypeConverter.InferCommonDecimalType(values.Select(ToClickHouseDecimal)) ?? type,
            NullableType nullable => new NullableType { UnderlyingType = InferDecimalTypes(nullable.UnderlyingType, values) },
            ArrayType { UnderlyingType: DecimalType decimalType } => new ArrayType
            {
                UnderlyingType = InferArrayDecimalType(values) ?? decimalType,
            },
            ArrayType array => new ArrayType { UnderlyingType = InferDecimalTypes(array.UnderlyingType, values.SelectMany(ArrayElements)) },
            MapType map => new MapType
            {
                UnderlyingTypes = Tuple.Create(
                    InferDecimalTypes(map.KeyType, values.SelectMany(MapType.EnumerateEntries).Select(e => e.Key)),
                    InferDecimalTypes(map.ValueType, values.SelectMany(MapType.EnumerateEntries).Select(e => e.Value))),
            },
            TupleType tuple => new TupleType
            {
                UnderlyingTypes = tuple.UnderlyingTypes
                    .Select((itemType, i) => InferDecimalTypes(itemType, values.Select(v => TupleItem(v, i))))
                    .ToArray(),
            },
            _ => type,
        };
    }

    // The composite types are the ones TypeConverter.ToClickHouseType(Type) builds from .NET types.
    private static bool ContainsDecimal(ClickHouseType type) => type switch
    {
        DecimalType => true,
        NullableType nullable => ContainsDecimal(nullable.UnderlyingType),
        ArrayType array => ContainsDecimal(array.UnderlyingType),
        MapType map => ContainsDecimal(map.KeyType) || ContainsDecimal(map.ValueType),
        TupleType tuple => tuple.UnderlyingTypes.Any(ContainsDecimal),
        _ => false,
    };

    // The same conversion as DecimalType.Write.
    private static ClickHouseDecimal ToClickHouseDecimal(object value) =>
        value is ClickHouseDecimal chd ? chd : Convert.ToDecimal(value, CultureInfo.InvariantCulture);

    // The values one Array level down. A rank-N .NET array is N nested Array levels, but it enumerates all its
    // elements at once, so they are wrapped to reach the innermost level.
    private static IEnumerable<object> ArrayElements(object array)
    {
        if (array is not Array { Rank: > 1 } multidimensional)
            return ((IEnumerable)array).Cast<object>();

        var elements = multidimensional.Cast<object>();
        for (var level = 1; level < multidimensional.Rank; level++)
            elements = [elements];
        return elements;
    }

    // The common decimal type of the elements of the Array(Decimal) values in `arrays`. ArrayType writes a
    // decimal[] or a ClickHouseDecimal[] without boxing its elements, so they are read here without boxing them
    // either. When all the arrays are decimal[], their elements are also not converted to ClickHouseDecimal.
    private static ClickHouseType InferArrayDecimalType(IEnumerable<object> arrays) =>
        arrays.All(static array => array is decimal[])
            ? TypeConverter.InferCommonDecimalType(DecimalArrayElements(arrays))
            : TypeConverter.InferCommonDecimalType(ClickHouseDecimalArrayElements(arrays));

    private static IEnumerable<decimal> DecimalArrayElements(IEnumerable<object> arrays)
    {
        foreach (decimal[] array in arrays)
        {
            foreach (var value in array)
                yield return value;
        }
    }

    private static IEnumerable<ClickHouseDecimal> ClickHouseDecimalArrayElements(IEnumerable<object> arrays)
    {
        foreach (var array in arrays)
        {
            if (array is ClickHouseDecimal[] clickHouseDecimals)
            {
                foreach (var value in clickHouseDecimals)
                    yield return value;
            }
            else if (array is decimal[] decimals)
            {
                foreach (var value in decimals)
                    yield return value;
            }
            else
            {
                foreach (var value in ArrayElements(array))
                {
                    if (value is not null and not DBNull)
                        yield return ToClickHouseDecimal(value);
                }
            }
        }
    }

    // A Tuple type inferred from a .NET type is only written with System.Tuple and ValueTuple values.
    private static object TupleItem(object tuple, int index) => ((ITuple)tuple)[index];
}
